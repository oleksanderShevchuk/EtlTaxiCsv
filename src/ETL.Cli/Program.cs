using ETL.Core.Ports;
using ETL.Core.Services;
using ETL.Infrastructure.Csv;
using ETL.Infrastructure.Dedup;
using ETL.Infrastructure.IO;
using ETL.Infrastructure.Sql;
using ETL.Infrastructure.Time;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Serilog;

var builder = Host.CreateDefaultBuilder(args)
    .ConfigureAppConfiguration((ctx, config) =>
    {
        config.AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
              .AddEnvironmentVariables()
              .AddCommandLine(args);
    })
    .UseSerilog((ctx, cfg) =>
    {
        cfg.ReadFrom.Configuration(ctx.Configuration);
    })
    .ConfigureServices((ctx, services) =>
    {
        var cfg = ctx.Configuration;

        var conn = cfg.GetConnectionString("TaxiDb")
                   ?? "Server=(localdb)\\mssqllocaldb;Database=TaxiTrips;Trusted_Connection=True;TrustServerCertificate=True;";
        var batch = cfg.GetValue<int?>("Csv:BatchSize") ?? 50000;
        var duplicates = cfg.GetValue<string>("Csv:DuplicatesFile") ?? "duplicates.csv";

        services.AddSingleton<ICsvReader, CsvHelperCsvReader>();
        services.AddSingleton<IDuplicateDetector, InMemoryDuplicateDetector>();
        services.AddSingleton<IFileWriter, FileDuplicateWriter>();
        services.AddSingleton<IBulkInserter>(_ => new SqlBulkInserter(conn, "dbo.StagingTrips", batch));
        services.AddSingleton<IClock, SystemClock>();
        services.AddSingleton<ImportService>();
    });

var host = builder.Build();

var config = host.Services.GetRequiredService<IConfiguration>();

var baseDir = AppContext.BaseDirectory;
var csvPath = Path.Combine(baseDir, "samples", "taxi_sample_small.csv");

if (!File.Exists(csvPath))
{
    csvPath = Path.GetFullPath(Path.Combine(baseDir, @"..\..\..\samples\taxi_sample_small.csv"));
}

if (!File.Exists(csvPath))
{
    Console.WriteLine($"❌ CSV file not found at: {csvPath}");
    return;
}
var tz = config.GetValue<string>("Csv:TimeZoneId") ?? "America/New_York";
var duplicatesFile = config.GetValue<string>("Csv:DuplicatesFile") ?? "duplicates.csv";

var svc = host.Services.GetRequiredService<ImportService>();
await svc.RunAsync(csvPath, duplicatesFile, tz);
