using ETL.Core.Models;
using ETL.Infrastructure.Dedup;

namespace ETL.Tests;

public class DuplicateDetectorTests
{
    [Fact]
    public void IsDuplicate_ShouldReturnTrue_ForSameTripKey()
    {
        var detector = new InMemoryDuplicateDetector();
        var key = new TripKey(DateTime.UtcNow, DateTime.UtcNow.AddMinutes(5), 2);

        // First insert => not duplicate
        var first = detector.IsDuplicate(key);
        // Second insert => duplicate
        var second = detector.IsDuplicate(key);

        Assert.False(first);
        Assert.True(second);
    }

    [Fact]
    public void IsDuplicate_ShouldReturnFalse_ForDifferentKeys()
    {
        var detector = new InMemoryDuplicateDetector();
        var k1 = new TripKey(DateTime.UtcNow, DateTime.UtcNow.AddMinutes(5), 1);
        var k2 = new TripKey(DateTime.UtcNow.AddMinutes(1), DateTime.UtcNow.AddMinutes(6), 2);

        Assert.False(detector.IsDuplicate(k1));
        Assert.False(detector.IsDuplicate(k2));
    }
}