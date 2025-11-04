using ETL.Core.Ports;

namespace ETL.Infrastructure.Time
{
    public class SystemClock : IClock
    {
        public DateTime ConvertToUtc(DateTime localDateTime, string sourceTimeZoneId)
        {
            try
            {
                TimeZoneInfo tz;
                try
                {
                    tz = TimeZoneInfo.FindSystemTimeZoneById(sourceTimeZoneId);
                }
                catch
                {
                    // map common tz names
                    var map = sourceTimeZoneId switch
                    {
                        "America/New_York" => "Eastern Standard Time",
                        _ => sourceTimeZoneId
                    };
                    tz = TimeZoneInfo.FindSystemTimeZoneById(map);
                }
                return TimeZoneInfo.ConvertTimeToUtc(localDateTime, tz);
            }
            catch
            {
                // fallback: assume provided DateTime is already UTC
                return DateTime.SpecifyKind(localDateTime, DateTimeKind.Utc);
            }
        }
    }
}
