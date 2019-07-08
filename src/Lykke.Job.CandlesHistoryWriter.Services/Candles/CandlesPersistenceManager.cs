// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

using System;
using System.Threading.Tasks;
using Common;
using Common.Log;
using Lykke.Job.CandlesHistoryWriter.Core.Services;
using Lykke.Job.CandlesHistoryWriter.Core.Services.Candles;
using Lykke.Job.CandlesHistoryWriter.Services.Settings;

namespace Lykke.Job.CandlesHistoryWriter.Services.Candles
{
    public class CandlesPersistenceManager : 
        TimerPeriod,
        ICandlesPersistenceManager
    {
        private readonly ICandlesPersistenceQueue _persistenceQueue;
        private readonly IHealthService _healthService;
        private readonly PersistenceSettings _settings;
        private DateTime _lastDispatchMoment;

        public CandlesPersistenceManager(
            ICandlesPersistenceQueue persistenceQueue,
            IHealthService healthService,
            ILog log,
            PersistenceSettings settings) : 

            base(nameof(CandlesPersistenceManager), (int)TimeSpan.FromSeconds(5).TotalMilliseconds, log)
        {
            _persistenceQueue = persistenceQueue;
            _healthService = healthService;
            _settings = settings;

            _lastDispatchMoment = DateTime.MinValue;
        }

        public override Task Execute()
        {
            var now = DateTime.UtcNow;

            if (_healthService.CandlesToDispatchQueueLength > _settings.CandlesToDispatchLengthPersistThreshold ||
                now - _lastDispatchMoment > _settings.PersistPeriod)
            {
                if (_healthService.BatchesToPersistQueueLength < _settings.MaxBatchesToPersistQueueLength)
                {
                    _persistenceQueue.DispatchCandlesToPersist(_settings.MaxBatchSize);
                    _lastDispatchMoment = now;
                }
            }


            return Task.FromResult(0);
        }
    }
}
