using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;

/// <summary>
/// RU-aware CSV export from Azure Cosmos DB using a list of IDs (e.g., VINs).
///
/// - Reads IDs from a text file (one ID per line)
/// - Queries Cosmos in batches using ARRAY_CONTAINS(@ids, UPPER(c.idField))
/// - Writes CSV with controlled file size (max rows per file)
/// - Throttles when request charge exceeds a threshold
///
/// SECURITY / PUBLIC REPO NOTE
/// - No secrets in code. Provide connection config via env vars or CLI args.
/// - Never commit real connection strings or keys.
/// </summary>
internal static class ExportByIdListRuAware
{
    private sealed class Options
    {
        public string ConnectionString { get; set; } = "";
        public string DatabaseId { get; set; } = "";
        public string ContainerId { get; set; } = "";
        public string IdsFile { get; set; } = "";
        public string OutputDir { get; set; } = ".";
        public string OutputPrefix { get; set; } = "export";
        public int IdBatchSize { get; set; } = 500;
        public int MaxRowsPerFile { get; set; } = 50_000; // includes header
        public double RuThreshold { get; set; } = 10_000.0;
        public int RuSleepMs { get; set; } = 1000;
        public int MaxItemCount { get; set; } = 2000;
        public int MaxConcurrency { get; set; } = -1;

        // Query customization
        public string IdField { get; set; } = "vin"; // logical name, used in query template
        public string QueryFile { get; set; } = "";  // optional .sql file; if empty uses default template
    }

    private sealed class ExportRow
    {
        // Customize these properties to match your SELECT projection aliases
        public string vin { get; set; } = "";
        public string field1 { get; set; } = "";
        public string field2 { get; set; } = "";
        public string field3 { get; set; } = "";
    }

    public static async Task<int> Main(string[] args)
    {
        try
        {
            var opt = ParseArgs(args);

            opt.ConnectionString = Require(opt.ConnectionString, "connection string", "COSMOS_CONNECTION_STRING");
            opt.DatabaseId = Require(opt.DatabaseId, "database id", "COSMOS_DATABASE_ID");
            opt.ContainerId = Require(opt.ContainerId, "container id", "COSMOS_CONTAINER_ID");
            opt.IdsFile = Require(opt.IdsFile, "ids file", "COSMOS_IDS_FILE");

            Directory.CreateDirectory(opt.OutputDir);

            var ids = LoadIds(opt.IdsFile);
            if (ids.Count == 0)
            {
                Console.WriteLine("No IDs found in input file.");
                return 0;
            }

            Console.WriteLine($"IDs loaded: {ids.Count}");
            Console.WriteLine($"Batch size: {opt.IdBatchSize} | Max rows/file: {opt.MaxRowsPerFile} | RU threshold: {opt.RuThreshold}");

            using var client = new CosmosClient(opt.ConnectionString, new CosmosClientOptions
            {
                SerializerOptions = new CosmosSerializationOptions { PropertyNamingPolicy = CosmosPropertyNamingPolicy.Default }
            });

            var container = client.GetContainer(opt.DatabaseId, opt.ContainerId);

            // CSV writer state
            int fileIndex = 1;
            int rowsInCurrentFile = 0;
            var csvRows = new List<string>(capacity: Math.Min(opt.MaxRowsPerFile + 10, 60_000));

            // CSV header: adapt to your ExportRow columns
            string header = "vin,field1,field2,field3";
            csvRows.Add(header);
            rowsInCurrentFile = 1;

            // Optional de-dup safeguard: if ids contain duplicates, avoid duplicate output
            var alreadyOutput = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            for (int i = 0; i < ids.Count; i += opt.IdBatchSize)
            {
                var batch = ids.Skip(i).Take(opt.IdBatchSize).ToList();
                if (batch.Count == 0) break;

                // Use UPPER matching to reduce case issues
                var batchUpper = batch.Select(x => (x ?? "").Trim().ToUpperInvariant()).Where(x => x.Length > 0).ToArray();

                var query = string.IsNullOrWhiteSpace(opt.QueryFile)
                    ? DefaultQueryTemplate(opt.IdField)
                    : File.ReadAllText(opt.QueryFile);

                var qd = new QueryDefinition(query).WithParameter("@ids", batchUpper);

                var iterator = container.GetItemQueryIterator<ExportRow>(
                    qd,
                    requestOptions: new QueryRequestOptions
                    {
                        MaxItemCount = opt.MaxItemCount,
                        MaxConcurrency = opt.MaxConcurrency
                    }
                );

                // Collect results for this batch in a dictionary by vin (case-insensitive)
                var foundById = new Dictionary<string, ExportRow>(StringComparer.OrdinalIgnoreCase);

                int page = 0;
                while (iterator.HasMoreResults)
                {
                    var response = await iterator.ReadNextAsync().ConfigureAwait(false);
                    page++;

                    foreach (var row in response)
                    {
                        if (!string.IsNullOrWhiteSpace(row.vin))
                            foundById[row.vin.Trim()] = row;
                    }

                    Console.WriteLine($"Batch {(i / opt.IdBatchSize) + 1}: page {page} | items {response.Count} | RU {response.RequestCharge:F2}");

                    if (response.RequestCharge > opt.RuThreshold)
                    {
                        Console.WriteLine($"Throttling: sleeping {opt.RuSleepMs} ms to respect RU threshold…");
                        await Task.Delay(opt.RuSleepMs).ConfigureAwait(false);
                    }
                }

                // Ensure 1 output row per requested ID, even if missing in Cosmos
                foreach (var id in batchUpper)
                {
                    if (!alreadyOutput.Add(id)) continue;

                    foundById.TryGetValue(id, out var row);
                    row ??= new ExportRow { vin = id };

                    // Build CSV line (escape)
                    var line = string.Join(",",
                        Csv(row.vin),
                        Csv(row.field1),
                        Csv(row.field2),
                        Csv(row.field3)
                    );

                    csvRows.Add(line);
                    rowsInCurrentFile++;

                    if (rowsInCurrentFile >= opt.MaxRowsPerFile)
                    {
                        SaveCsv(opt.OutputDir, opt.OutputPrefix, fileIndex, csvRows);
                        fileIndex++;
                        csvRows.Clear();
                        csvRows.Add(header);
                        rowsInCurrentFile = 1;
                    }
                }
            }

            if (csvRows.Count > 1)
                SaveCsv(opt.OutputDir, opt.OutputPrefix, fileIndex, csvRows);

            Console.WriteLine("✅ Export completed.");
            return 0;
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine("ERROR: " + ex.Message);
            return 1;
        }
    }

    private static string DefaultQueryTemplate(string idField)
    {
        // You can adapt this projection to your needs. Keep it generic for public repo.
        // Must return JSON objects matching ExportRow properties.
        return $@"
SELECT
  c.{idField} AS vin,
  c.field1 AS field1,
  c.field2 AS field2,
  c.field3 AS field3
FROM c
WHERE ARRAY_CONTAINS(@ids, UPPER(c.{idField}))
";
    }

    private static void SaveCsv(string outputDir, string prefix, int fileIndex, List<string> rows)
    {
        var fileName = Path.Combine(outputDir, $"{prefix}_{fileIndex}.csv");
        File.WriteAllLines(fileName, rows, Encoding.UTF8);
        Console.WriteLine($"💾 Saved: {fileName} ({rows.Count - 1} data rows)");
    }

    private static List<string> LoadIds(string path)
    {
        return File.ReadAllLines(path)
            .Select(l => (l ?? "").Trim())
            .Where(l => !string.IsNullOrWhiteSpace(l) && !l.StartsWith("#"))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();
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
            IdsFile = Env("COSMOS_IDS_FILE"),
            OutputDir = Env("COSMOS_OUTPUT_DIR") ?? ".",
            OutputPrefix = Env("COSMOS_OUTPUT_PREFIX") ?? "export",
            QueryFile = Env("COSMOS_QUERY_FILE") ?? ""
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
                case "--ids-file":
                    o.IdsFile = Next();
                    break;
                case "--query-file":
                    o.QueryFile = Next();
                    break;
                case "--id-field":
                    o.IdField = Next();
                    break;
                case "--out-dir":
                    o.OutputDir = Next();
                    break;
                case "--out-prefix":
                    o.OutputPrefix = Next();
                    break;
                case "--id-batch":
                    o.IdBatchSize = int.Parse(Next(), CultureInfo.InvariantCulture);
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
ExportByIdListRuAware

Required (env or args):
  COSMOS_CONNECTION_STRING / --connection
  COSMOS_DATABASE_ID       / --database
  COSMOS_CONTAINER_ID      / --container
  COSMOS_IDS_FILE          / --ids-file

Optional:
  --query-file <path>      Cosmos SQL query in a file (must use @ids parameter)
  --id-field <name>        Default: vin
  --out-dir <dir>          Default: .
  --out-prefix <name>      Default: export
  --id-batch <n>           Default: 500
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
        throw new ArgumentException($"Missing {label}. Provide --{label.Replace(' ', '-') } or set env var {envName}.");
    }
}
