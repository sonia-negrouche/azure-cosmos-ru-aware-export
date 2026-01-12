using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;

/// <summary>
/// RU-aware scalar export from Azure Cosmos DB to CSV.
///
/// - Executes a Cosmos SQL query that returns a scalar VALUE (e.g., VALUE c.vin)
/// - Writes results to CSV
/// - Splits output files by max rows
/// - Throttles when request charge exceeds a threshold
///
/// The query is loaded from a file to avoid hardcoding business logic in a public repo.
/// </summary>
internal static class ExportScalarListRuAware
{
    private sealed class Options
    {
        public string ConnectionString { get; set; } = "";
        public string DatabaseId { get; set; } = "";
        public string ContainerId { get; set; } = "";
        public string QueryFile { get; set; } = "";
        public string OutputDir { get; set; } = ".";
        public string OutputPrefix { get; set; } = "export_scalar";
        public int MaxRowsPerFile { get; set; } = 50_000; // includes header
        public double RuThreshold { get; set; } = 10_000.0;
        public int RuSleepMs { get; set; } = 1000;
        public int MaxItemCount { get; set; } = 2000;
        public int MaxConcurrency { get; set; } = -1;
        public string HeaderName { get; set; } = "value";
    }

    public static async Task<int> Main(string[] args)
    {
        try
        {
            var opt = ParseArgs(args);

            opt.ConnectionString = Require(opt.ConnectionString, "connection string", "COSMOS_CONNECTION_STRING");
            opt.DatabaseId = Require(opt.DatabaseId, "database id", "COSMOS_DATABASE_ID");
            opt.ContainerId = Require(opt.ContainerId, "container id", "COSMOS_CONTAINER_ID");
            opt.QueryFile = Require(opt.QueryFile, "query file", "COSMOS_QUERY_FILE");

            Directory.CreateDirectory(opt.OutputDir);

            var query = File.ReadAllText(opt.QueryFile);
            if (string.IsNullOrWhiteSpace(query))
                throw new ArgumentException("Query file is empty.");

            using var client = new CosmosClient(opt.ConnectionString);
            var container = client.GetContainer(opt.DatabaseId, opt.ContainerId);

            int fileIndex = 1;
            int rowsInCurrentFile = 0;
            var rows = new List<string>(capacity: Math.Min(opt.MaxRowsPerFile + 10, 60_000));
            rows.Add(opt.HeaderName);
            rowsInCurrentFile = 1;

            var iterator = container.GetItemQueryIterator<string>(
                new QueryDefinition(query),
                requestOptions: new QueryRequestOptions
                {
                    MaxItemCount = opt.MaxItemCount,
                    MaxConcurrency = opt.MaxConcurrency
                }
            );

            int page = 0;
            long total = 0;

            while (iterator.HasMoreResults)
            {
                var response = await iterator.ReadNextAsync().ConfigureAwait(false);
                page++;

                foreach (var value in response)
                {
                    rows.Add(Csv(value));
                    rowsInCurrentFile++;
                    total++;

                    if (rowsInCurrentFile >= opt.MaxRowsPerFile)
                    {
                        SaveCsv(opt.OutputDir, opt.OutputPrefix, fileIndex, rows);
                        fileIndex++;
                        rows.Clear();
                        rows.Add(opt.HeaderName);
                        rowsInCurrentFile = 1;
                    }
                }

                Console.WriteLine($"Page {page}: items {response.Count} | total {total} | RU {response.RequestCharge:F2}");

                if (response.RequestCharge > opt.RuThreshold)
                {
                    Console.WriteLine($"Throttling: sleeping {opt.RuSleepMs} ms to respect RU threshold…");
                    await Task.Delay(opt.RuSleepMs).ConfigureAwait(false);
                }
            }

            if (rows.Count > 1)
                SaveCsv(opt.OutputDir, opt.OutputPrefix, fileIndex, rows);

            Console.WriteLine("✅ Export completed.");
            return 0;
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine("ERROR: " + ex.Message);
            return 1;
        }
    }

    private static void SaveCsv(string outputDir, string prefix, int fileIndex, List<string> rows)
    {
        var fileName = Path.Combine(outputDir, $"{prefix}_{fileIndex}.csv");
        File.WriteAllLines(fileName, rows, Encoding.UTF8);
        Console.WriteLine($"💾 Saved: {fileName} ({rows.Count - 1} data rows)");
    }

    private static string Csv(string? v)
    {
        v ??= "";
        var s = v.Replace("\"", "\"\"");
        return $"\"{s}\"";
    }

    private static Options ParseArgs(string[] args)
    {
        var o = new Options
        {
            ConnectionString = Env("COSMOS_CONNECTION_STRING"),
            DatabaseId = Env("COSMOS_DATABASE_ID"),
            ContainerId = Env("COSMOS_CONTAINER_ID"),
            QueryFile = Env("COSMOS_QUERY_FILE"),
            OutputDir = Env("COSMOS_OUTPUT_DIR") ?? ".",
            OutputPrefix = Env("COSMOS_OUTPUT_PREFIX") ?? "export_scalar",
            HeaderName = Env("COSMOS_HEADER_NAME") ?? "value"
        };

        for (int i = 0; i < args.Length; i++)
        {
            string a = args[i];
            string Next() => (i + 1 < args.Length) ? args[++i] : throw new ArgumentException($"Missing value after {a}");

            switch (a)
            {
                case "--connection":
                    o.ConnectionString = Next();
                    break;
                case "--database":
                    o.DatabaseId = Next();
                    break;
                case "--container":
                    o.ContainerId = Next();
                    break;
                case "--query-file":
                    o.QueryFile = Next();
                    break;
                case "--out-dir":
                    o.OutputDir = Next();
                    break;
                case "--out-prefix":
                    o.OutputPrefix = Next();
                    break;
                case "--header":
                    o.HeaderName = Next();
                    break;
                case "--max-rows":
                    o.MaxRowsPerFile = int.Parse(Next(), CultureInfo.InvariantCulture);
                    break;
                case "--ru-threshold":
                    o.RuThreshold = double.Parse(Next(), CultureInfo.InvariantCulture);
                    break;
                case "--ru-sleep-ms":
                    o.RuSleepMs = int.Parse(Next(), CultureInfo.InvariantCulture);
                    break;
                case "--max-item-count":
                    o.MaxItemCount = int.Parse(Next(), CultureInfo.InvariantCulture);
                    break;
                case "--max-concurrency":
                    o.MaxConcurrency = int.Parse(Next(), CultureInfo.InvariantCulture);
                    break;
                case "--help":
                case "-h":
                    PrintHelp();
                    Environment.Exit(0);
                    break;
                default:
                    if (a.StartsWith("--"))
                        throw new ArgumentException($"Unknown argument: {a}");
                    break;
            }
        }

        return o;
    }

    private static void PrintHelp()
    {
        Console.WriteLine(@"
ExportScalarListRuAware

Required (env or args):
  COSMOS_CONNECTION_STRING / --connection
  COSMOS_DATABASE_ID       / --database
  COSMOS_CONTAINER_ID      / --container
  COSMOS_QUERY_FILE        / --query-file

Optional:
  --header <name>          Default: value
  --out-dir <dir>          Default: .
  --out-prefix <name>      Default: export_scalar
  --max-rows <n>           Default: 50000
  --ru-threshold <n>       Default: 10000
  --ru-sleep-ms <ms>       Default: 1000
  --max-item-count <n>     Default: 2000
  --max-concurrency <n>    Default: -1
");
    }

    private static string Env(string name) => (Environment.GetEnvironmentVariable(name) ?? "").Trim();

    private static string Require(string value, string label, string envName)
    {
        if (!string.IsNullOrWhiteSpace(value)) return value;
        throw new ArgumentException($"Missing {label}. Provide arg or set env var {envName}.");
    }
}
