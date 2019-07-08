// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.Linq;
using Lykke.Job.CandlesProducer.Contract;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;

namespace Lykke.Job.CandlesHistoryWriter.Services.HistoryMigration
{
    /// <summary>
    /// Stores bid and ask sec candles for single asset pair.
    /// Use to synchronize bid and ask history flow while mid candles generating
    /// </summary>
    public class BidAskHCacheService
    {
        private readonly LinkedList<(DateTime timestamp, ICandle ask, ICandle bid)> _storage;
        private ICandle _lastPushedAskCandle;
        private ICandle _lastPushedBidCandle;

        public BidAskHCacheService()
        {
            _storage = new LinkedList<(DateTime timestamp, ICandle ask, ICandle bid)>();
        }

        public IReadOnlyList<(DateTime timestamp, ICandle ask, ICandle bid)> PopReadyHistory()
        {
            lock (_storage)
            {
                var result = new List<(DateTime timestamp, ICandle ask, ICandle bid)>();

                // If none of the ask or bid candles was pushed yet, middle candles can't be generated

                if (_lastPushedAskCandle == null || _lastPushedBidCandle == null)
                {
                    return result;
                }

                // Assuming, that _storage is sorted by timestamp

                for (var storageItem = _storage.First; storageItem != null;)
                {
                    var storedCandle = storageItem.Value;

                    // Returns only items with both ask and bid candles

                    if (storedCandle.ask != null && storedCandle.bid != null)
                    {
                        result.Add(storedCandle);
                    }

                    // Breaks, when items filled with both ask and bid candles are ended, according to the last pushed candles.

                    else if (storedCandle.timestamp >= _lastPushedAskCandle.Timestamp ||
                             storedCandle.timestamp >= _lastPushedBidCandle.Timestamp)
                    {
                        break;
                    }

                    // Removes items that was added to the result 
                    // as well as items that was skipped due to one of ask or bid candles are empty

                    var itemToRemove = storageItem;
                    storageItem = storageItem.Next;

                    _storage.Remove(itemToRemove);
                }

                return result;
            }
        }

        public void PushHistory(IReadOnlyList<ICandle> candles)
        {
            lock (_storage)
            {
                var storageItem = _storage.First;

                // Assuming that both candles and _storage are sorted by timestamp

                foreach (var pushedCandle in candles)
                {
                    var found = false;

                    for(; storageItem != null; storageItem = storageItem.Next)
                    {
                        var storedCandle = storageItem.Value;
                        if (storedCandle.timestamp == pushedCandle.Timestamp)
                        {
                            storageItem.Value = (
                                storedCandle.timestamp,
                                pushedCandle.PriceType == CandlePriceType.Ask ? pushedCandle : storedCandle.ask,
                                pushedCandle.PriceType == CandlePriceType.Bid ? pushedCandle : storedCandle.bid);
                            found = true;
                            break;
                        }

                        if (storedCandle.timestamp > pushedCandle.Timestamp)
                        {
                            break;
                        }
                    }

                    if (!found)
                    {
                        _storage.AddLast((
                            pushedCandle.Timestamp,
                            pushedCandle.PriceType == CandlePriceType.Ask ? pushedCandle : null,
                            pushedCandle.PriceType == CandlePriceType.Bid ? pushedCandle : null));
                    }
                }

                // Assuming all candles have the same PriceType

                var lastCandle = candles.Last();
                
                UpdateLastPushedCandle(lastCandle);
            }
        }

        private void UpdateLastPushedCandle(ICandle lastCandle)
        {
            switch (lastCandle.PriceType)
            {
                case CandlePriceType.Ask:
                    _lastPushedAskCandle = lastCandle;
                    break;

                case CandlePriceType.Bid:
                    _lastPushedBidCandle = lastCandle;
                    break;

                default:
                    throw new ArgumentOutOfRangeException(nameof(lastCandle), lastCandle.PriceType, "Invalid priceType of the last candle");
            }
        }
    }
}
