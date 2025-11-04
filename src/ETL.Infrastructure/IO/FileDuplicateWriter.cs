using ETL.Core.Ports;

namespace ETL.Infrastructure.IO
{
    public class FileDuplicateWriter : IFileWriter
    {
        private readonly SemaphoreSlim _sem = new(1, 1);

        public async Task AppendLineAsync(string path, string line, CancellationToken ct = default)
        {
            await _sem.WaitAsync(ct);
            try
            {
                using var sw = new StreamWriter(path, append: true);
                await sw.WriteLineAsync(line);
            }
            finally
            {
                _sem.Release();
            }
        }
    }
}
