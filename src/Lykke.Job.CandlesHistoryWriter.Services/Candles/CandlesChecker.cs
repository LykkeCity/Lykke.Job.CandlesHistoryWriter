﻿using System;
using System.Collections.Generic;
using JetBrains.Annotations;
using Lykke.Common.Log;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using Lykke.Job.CandlesHistoryWriter.Core.Services;

namespace Lykke.Job.CandlesHistoryWriter.Services.Candles
{
    [UsedImplicitly]
    public class CandlesChecker : CandlesCheckerSilent
    {
        private readonly IClock _clock;
        private readonly TimeSpan _notificationTimeout;

        private readonly Dictionary<string, DateTime> _knownUnsupportedAssetPairs;

        /// <inheritdoc />
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="logFactory">The <see cref="T:Common.Log.ILogFactory" /> instance.</param>
        /// <param name="clock">The <see cref="T:Lykke.Job.CandlesHistoryWriter.Core.Services.IClock" /> instance.</param>
        /// <param name="historyRep">The <see cref="T:Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles.ICandlesHistoryRepository" /> instance.</param>
        /// <param name="notificationTimeout">The timeout in seconds between log notifications for the same asset pair.</param>
        public CandlesChecker(
            ILogFactory logFactory,
            IClock clock,
            ICandlesHistoryRepository historyRep,
            TimeSpan notificationTimeout) : base(
                logFactory,
                historyRep, nameof(CandlesChecker))
        {

            _clock = clock;
            _notificationTimeout = notificationTimeout;

            _knownUnsupportedAssetPairs = new Dictionary<string, DateTime>();
        }

        /// <inheritdoc />
        /// <summary>
        /// Checks if we can handle/store the given asset pair. Also, writes an error to log according to timeout from settings.
        /// </summary>
        /// <param name="assetPairId">Asset pair ID.</param>
        /// <returns>True if repository is able to store such a pair, and false otherwise.</returns>
        public override bool CanHandleAssetPair(string assetPairId)
        {
            if (base.CanHandleAssetPair(assetPairId))
                return true; // It's Ok, we can store this asset pair

            // If we can't store and need to notify others...
            var needToLog = false;

            if (!_knownUnsupportedAssetPairs.TryGetValue(assetPairId, out var lastLogMoment))
            {
                _knownUnsupportedAssetPairs.Add(assetPairId, _clock.UtcNow);
                needToLog = true;
            }
            else
            {
                if (_clock.UtcNow.Subtract(lastLogMoment) > _notificationTimeout)
                {
                    needToLog = true;
                    _knownUnsupportedAssetPairs[assetPairId] = _clock.UtcNow;
                }
            }

            if (needToLog)
                Log?.Error(nameof(CanHandleAssetPair),
                    new ArgumentOutOfRangeException("Incomptible candle batch recieved: connection string for asset pair not configured. Skipping..."),
                    context: assetPairId);

            return false; // Finally
        }
    }
}
