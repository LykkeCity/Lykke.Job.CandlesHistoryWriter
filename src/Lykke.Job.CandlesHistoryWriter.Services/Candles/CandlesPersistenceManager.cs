using System;
using System.Threading.Tasks;
using Common;
using Lykke.Common.Log;
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
            ILogFactory logFactory,
            PersistenceSettings settings) : 

            base(TimeSpan.FromSeconds(5), logFactory, nameof(CandlesPersistenceManager))
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
