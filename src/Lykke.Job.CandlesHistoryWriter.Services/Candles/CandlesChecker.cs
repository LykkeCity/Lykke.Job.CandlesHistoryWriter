using Common.Log;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using Lykke.Job.CandlesHistoryWriter.Core.Services;
using Lykke.Job.CandlesHistoryWriter.Core.Services.Candles;
using Lykke.Job.CandlesHistoryWriter.Services.Settings;
using System;
using System.Collections.Generic;

namespace Lykke.Job.CandlesHistoryWriter.Services.Candles
{
    public class CandlesChecker : ICandlesChecker
    {
        private readonly ILog _log;
        private readonly IClock _clock;
        private readonly ICandlesHistoryRepository _candlesHistoryRepository;
        private readonly ErrorManagementSettings _errorSettings;

        private Dictionary<string, DateTime> _knownUnsupportedAssetPairs;

        public CandlesChecker(
            ILog log,
            IClock clock,
            ICandlesHistoryRepository _historyRep,
            ErrorManagementSettings settings)
        {
            _log = log;
            _clock = clock;
            _candlesHistoryRepository = _historyRep;
            _errorSettings = settings;

            _knownUnsupportedAssetPairs = new Dictionary<string, DateTime>();
        }

        /// <summary>
        /// Checks if we can handle/store the given asset pair. Also, writes an error to log according to timeout from settings.
        /// </summary>
        /// <param name="assetPairId">Asset pair ID.</param>
        /// <returns>True if repository is able to store such a pair, and false otherwise.</returns>
        public bool CanHandleAssetPair(string assetPairId)
        {
            if (_candlesHistoryRepository.CanStoreAssetPair(assetPairId)) return true; // It's Ok, we can store this asset pair
            if (!_errorSettings.NotifyOnCantStoreAssetPair) return false; // We just can not store, and no notification on that is required in settings

            // If we can't store and need to notify others...
            bool needToLog = false;
            if (!_knownUnsupportedAssetPairs.ContainsKey(assetPairId))
            {
                _knownUnsupportedAssetPairs.Add(assetPairId, _clock.UtcNow);
                needToLog = true;
            }
            else
            {
                var lastLogDT = _knownUnsupportedAssetPairs[assetPairId];
                if (_clock.UtcNow.Subtract(lastLogDT).TotalSeconds > _errorSettings.NotifyOnCantStoreAssetPairTimeout)
                {
                    needToLog = true;
                    _knownUnsupportedAssetPairs[assetPairId] = _clock.UtcNow;
                }
            }

            if (needToLog)
                _log?.WriteErrorAsync(nameof(CandlesChecker),
                    nameof(CanHandleAssetPair),
                    null,
                    new ArgumentOutOfRangeException($"Incomptible candle batch recieved. Connection string for asset pair {assetPairId} not configured. Skipping..."));

            return false; // Finally
        }
    }
}
