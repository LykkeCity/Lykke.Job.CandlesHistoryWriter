﻿using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Common.Log;
using JetBrains.Annotations;
using Lykke.Common.Log;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using Lykke.Job.CandlesHistoryWriter.Core.Services.Candles;
using Lykke.Job.CandlesHistoryWriter.Services.Candles;
using Lykke.Sdk;

namespace Lykke.Job.CandlesHistoryWriter.Services
{
    [UsedImplicitly]
    public class StartupManager : IStartupManager
    {
        private readonly ILog _log;
        private readonly ICandlesCacheInitalizationService _cacheInitalizationService;
        private readonly RedisCacheCaretaker _cacheCaretaker;
        private readonly ICandlesSubscriber _candlesSubscriber;
        private readonly ISnapshotSerializer _snapshotSerializer;
        private readonly ICandlesPersistenceQueueSnapshotRepository _persistenceQueueSnapshotRepository;
        private readonly ICandlesPersistenceQueue _persistenceQueue;
        private readonly ICandlesPersistenceManager _persistenceManager;
        private readonly bool _migrationEnabled;

        public StartupManager(
            ILogFactory logFactory,
            ICandlesCacheInitalizationService cacheInitalizationService,
            RedisCacheCaretaker cacheCaretaker,
            ICandlesSubscriber candlesSubscriber,
            ISnapshotSerializer snapshotSerializer,
            ICandlesPersistenceQueueSnapshotRepository persistenceQueueSnapshotRepository,
            ICandlesPersistenceQueue persistenceQueue,
            ICandlesPersistenceManager persistenceManager,
            bool migrationEnabled)
        {
            if (logFactory == null)
                throw new ArgumentNullException(nameof(logFactory));

            _log = logFactory.CreateLog(this);

            _cacheInitalizationService = cacheInitalizationService ?? throw new ArgumentNullException(nameof(cacheInitalizationService));
            _cacheCaretaker = cacheCaretaker ?? throw new ArgumentNullException(nameof(cacheCaretaker));
            _candlesSubscriber = candlesSubscriber ?? throw new ArgumentNullException(nameof(candlesSubscriber));
            _snapshotSerializer = snapshotSerializer ?? throw new ArgumentNullException(nameof(snapshotSerializer));
            _persistenceQueueSnapshotRepository = persistenceQueueSnapshotRepository ?? throw new ArgumentNullException(nameof(persistenceQueueSnapshotRepository));
            _persistenceQueue = persistenceQueue ?? throw new ArgumentNullException(nameof(persistenceQueue));
            _persistenceManager = persistenceManager ?? throw new ArgumentNullException(nameof(persistenceManager));
            _migrationEnabled = migrationEnabled;
        }

        public async Task StartAsync()
        {
            _log.Info(nameof(StartAsync), "Deserializing persistence queue async...");

            var tasks = new List<Task>
            {
                _snapshotSerializer.DeserializeAsync(_persistenceQueue, _persistenceQueueSnapshotRepository)
            };

            if (!_migrationEnabled)
            {
                _log.Info(nameof(StartAsync), "Initializing cache from the history async...");

                tasks.Add(_cacheInitalizationService.InitializeCacheAsync());
            }

            _log.Info(nameof(StartAsync), "Waiting for async tasks...");

            await Task.WhenAll(tasks);

            _log.Info(nameof(StartAsync), "Starting persistence queue...");

            _persistenceQueue.Start();

            _log.Info(nameof(StartAsync), "Starting persistence manager...");

            _persistenceManager.Start();

            // We can not combine it with the previous if(!_migration...) due to launch order importance.
            if (!_migrationEnabled)
            {
                _log.Info(nameof(StartAsync), "Starting candles subscriber...");

                _candlesSubscriber.Start();

                _cacheCaretaker.Start();  // Should go after cache initialization has finished working && if no migration
            }

            _log.Info(nameof(StartAsync), "Started up");
        }
    }
}
