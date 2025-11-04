namespace ETL.Core.Models
{
    public record TripRecord(
       DateTime PickupUtc,
       DateTime DropoffUtc,
       short PassengerCount,
       decimal TripDistance,
       string StoreAndFwdFlag, // "Yes" or "No"
       int PULocationId,
       int DOLocationId,
       decimal FareAmount,
       decimal TipAmount
   );
}
