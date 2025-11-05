using ETL.Infrastructure.Csv;

namespace ETL.Tests
{
    public class CsvHelperCsvReaderTests
    {
        [Fact]
        public async Task ReadRowsAsync_ShouldReturnRows()
        {
            // Arrange
            var csv = "col1,col2\nA,B\nC,D";
            var path = Path.GetTempFileName();
            await File.WriteAllTextAsync(path, csv);

            var reader = new CsvHelperCsvReader();

            // Act
            var rows = new List<IDictionary<string, string>>();
            await foreach (var row in reader.ReadRowsAsync(path))
            {
                rows.Add(row);
            }

            // Assert
            Assert.Equal(2, rows.Count);
            Assert.Equal("A", rows[0]["col1"]);
            Assert.Equal("D", rows[1]["col2"]);
        }
    }
}
