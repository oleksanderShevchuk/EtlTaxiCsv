### **ETL Taxi CSV**

#### 📄 Overview

This project implements an **ETL pipeline** that imports large New York City Taxi trip datasets (CSV) into a SQL Server database.
It performs validation, deduplication, transformation, and bulk insertion optimized for analytical queries.

#### 🧱 Architecture

```
sql/                      # Sql queries
src/
 ├── ETL.Core/            # Interfaces, models, and core services
 ├── ETL.Infrastructure/  # Implementations (CSV, SQL, I/O, Time)
 └── ETL.Cli/             # Console app entry point
 └── ETL.Tests/           # XUnit tests
```

#### ⚙️ Technologies

* **.NET 8**
* **Microsoft SQL Server**
* **CsvHelper** for CSV parsing
* **SqlBulkCopy** for efficient bulk loading
* **Serilog** for structured logging
* **Dependency Injection & High-level Architecture** principles

#### 🚀 Features

* Reads and transforms trip data from CSV in a streaming fashion
* Detects duplicates based on `(pickup_datetime, dropoff_datetime, passenger_count)`
* Writes duplicates into `duplicates.csv`
* Bulk loads clean records into `dbo.StagingTrips`
* Transforms and filters data into the final `dbo.Trips` table
* Schema optimized for analytical queries

#### 📊 Database Schema Highlights

* **`Trips`** – main clean dataset
* **`StagingTrips`** – temporary table for raw imports
* **Indexes optimized for:**

  * Average tip per `PULocationId`
  * Top 100 longest trips by distance or duration
  * Search queries filtered by `PULocationId`

#### 🧮 Example Analytical Queries

-- You can find in sql folder (perform_queries.sql)

#### 💾 Running the Project

1. Configure `appsettings.json`:

```json
{
  "ConnectionStrings": {
    "TaxiDb": "Server=(localdb)\\mssqllocaldb;Database=TaxiTrips;Trusted_Connection=True;TrustServerCertificate=True;"
  },
  "Csv": {
    "BatchSize": 50000,
    "TimeZoneId": "America/New_York",
    "DuplicatesFile": "duplicates.csv"
  }
}
```

2. Run the console app:

```bash
dotnet run --project src/ETL.Cli
```

3. Processed data will be inserted into `TaxiTrips.dbo.Trips`.

## 🐳 Run with Docker

### Build and start the environment
```bash
docker-compose up --build

```

#### 🧩 Scaling to Large Files

For 10GB+ CSVs:

* Continue using streaming reads and channels.
* Tune `BatchSize` for optimal performance.
* Optionally parallelize bulk inserts.
* Disable and rebuild indexes after bulk load.
