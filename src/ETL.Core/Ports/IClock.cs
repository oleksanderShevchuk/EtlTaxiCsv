namespace ETL.Core.Ports
{
    public interface IClock
    {
        DateTime ConvertToUtc(DateTime localDateTime, string sourceTimeZoneId);
    }
}
