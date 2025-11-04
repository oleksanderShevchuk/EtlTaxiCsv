namespace ETL.Core.Models
{
    public readonly record struct TripKey(DateTime PickupUtc, DateTime DropoffUtc, short PassengerCount)
    {
        public override string ToString() =>
            $"{PickupUtc:O}|{DropoffUtc:O}|{PassengerCount}";
    }
}
