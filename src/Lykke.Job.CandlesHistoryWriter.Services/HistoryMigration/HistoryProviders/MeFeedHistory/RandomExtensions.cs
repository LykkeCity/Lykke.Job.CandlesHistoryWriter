using System;

namespace Lykke.Job.CandlesHistoryWriter.Services.HistoryMigration.HistoryProviders.MeFeedHistory
{
    public static class RandomExtensions
    {
        public static decimal NextDecimal(this Random random, decimal minValue, decimal maxValue)
        {
            return (decimal)random.NextDouble() * (maxValue - minValue) + minValue;
        }
    }
}
