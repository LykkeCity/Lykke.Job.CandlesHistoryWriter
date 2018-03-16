using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.HistoryMigration.HistoryProviders.TradesSQLHistory;
using Lykke.Job.CandlesProducer.Contract;

namespace Lykke.Job.CandlesHistoryWriter.Services.HistoryMigration
{
    public class TradesCandleStripe
    {
        public string AssetId { get; private set; }
        public CandleTimeInterval TimeInterval { get; private set; }

        public CandlePriceType PriceType =>
            CandlePriceType.Unspecified; // TODO: change to Trades after rebase on test branch.

        public List<ICandle> Candles { get; set; }

        public TradesCandleStripe(string assetId, CandleTimeInterval interval)
        {
            AssetId = assetId;
            TimeInterval = interval;

            Candles = new List<ICandle>();
        }

        public async Task MakeFromTrades(IEnumerable<TradeHistoryItem> trade)
        {
            await Task.Run(() =>
            {
                throw new NotImplementedException();
            });
        }

        public async Task DeriveFromSmallerIntervalAsync(TradesCandleStripe basis)
        {
            await Task.Run(() =>
            {
                if ((int)(basis.TimeInterval) >= (int)TimeInterval)
                    throw new InvalidOperationException($"Can't derive candles for time interval {TimeInterval.ToString()} from candles of time interval {basis.TimeInterval.ToString()}.");

                throw new NotImplementedException();
            });
        }
    }
}
