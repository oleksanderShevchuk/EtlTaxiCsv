namespace ETL.Core.Ports
{
    public interface IFileWriter
    {
        Task AppendLineAsync(string path, string line, CancellationToken ct = default);
    }
}
