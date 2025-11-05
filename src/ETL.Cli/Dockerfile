# Build stage
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

COPY ../.. .

RUN dotnet restore "src/ETL.Cli/ETL.Cli.csproj"

RUN dotnet publish "src/ETL.Cli/ETL.Cli.csproj" -c Release -o /app/publish

# Runtime stage
FROM mcr.microsoft.com/dotnet/runtime:8.0 AS runtime
WORKDIR /app

COPY --from=build /app/publish .

ENTRYPOINT ["dotnet", "ETL.Cli.dll"]
