﻿using System;
using System.Threading.Tasks;
using Common;
using Common.Log;
using JetBrains.Annotations;
using Lykke.Common.Log;
using Lykke.Job.CandlesHistoryWriter.Core.Services;

namespace Lykke.Job.CandlesHistoryWriter.Services
{
    [UsedImplicitly]
    public class HealthLogger : TimerPeriod
    {
        private readonly IHealthService _healthService;
        private readonly ILog _log;

        public HealthLogger(IHealthService healthService, ILogFactory logFactory) : 
            base(TimeSpan.FromMinutes(10), logFactory, nameof(HealthLogger))
        {
            _healthService = healthService;
            _log = logFactory.CreateLog(this);
        }

        public override Task Execute()
        {
            var health = new
            {
                Persistence = new
                {
                    Duration = new
                    {
                        Average = _healthService.AveragePersistDuration,
                        Last = _healthService.LastPersistDuration,
                        Total = _healthService.TotalPersistDuration,
                    },
                    PersistThroughput = new
                    {
                        AverageCandlesPersistedPerSecond = _healthService.AverageCandlesPersistedPerSecond,
                        AverageCandleRowsPersistedPerSecond = _healthService.AverageCandleRowsPersistedPerSecond
                    },
                    BatchesToPersistQueueLength = _healthService.BatchesToPersistQueueLength,
                    CandlesToDispatchQueueLength = _healthService.CandlesToDispatchQueueLength,
                    TotalCandlesPersistedCount = _healthService.TotalCandlesPersistedCount,
                    TotalCandleRowsPersistedCount = _healthService.TotalCandleRowsPersistedCount,
                },
                Cache = new
                {
                    Duration = new
                    {
                        Average = _healthService.AverageCacheDuration,
                        Last = _healthService.LastCacheDuration,
                        Total = _healthService.TotalCacheDuration,
                    },
                    Throughput = new
                    {
                        AverageCandlesCachedPerSecond = _healthService.AverageCandlesCachedPerSecond,
                        AverageCandlesCachedPerBatch = _healthService.AverageCandlesCachedPerBatch,
                        AverageCandleBatchesPerSecond = _healthService.AverageCandleBatchesPerSecond
                    },
                    TotalCandlesCachedCount = _healthService.TotalCandlesCachedCount,
                    TotalCandleBatchesCachedCount = _healthService.TotalCandleBatchesCachedCount
                }
            };

            _log.Info("Health", context: health);

            return Task.CompletedTask;
        }
    }
}
