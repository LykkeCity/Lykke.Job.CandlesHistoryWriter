using System.Collections.Generic;
using System.Threading.Tasks;
using Common.Log;
using JetBrains.Annotations;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using Lykke.Job.CandlesHistoryWriter.Core.Services;
using Lykke.Job.CandlesHistoryWriter.Core.Services.Candles;

namespace Lykke.Job.CandlesHistoryWriter.Services
{
    [UsedImplicitly]
    public class StartupManager : IStartupManager
    {
        private readonly ILog _log;
        private readonly ICandlesCacheInitalizationService _cacheInitalizationService;
        private readonly ICandlesSubscriber _candlesSubscriber;
        private readonly ISnapshotSerializer _snapshotSerializer;
        private readonly ICandlesPersistenceQueueSnapshotRepository _persistenceQueueSnapshotRepository;
        private readonly ICandlesPersistenceQueue _persistenceQueue;
        private readonly ICandlesPersistenceManager _persistenceManager;
        private readonly bool _migrationEnabled;

        public StartupManager(
            ILog log,
            ICandlesCacheInitalizationService cacheInitalizationService,
            ICandlesSubscriber candlesSubscriber,
            ISnapshotSerializer snapshotSerializer,
            ICandlesPersistenceQueueSnapshotRepository persistenceQueueSnapshotRepository,
            ICandlesPersistenceQueue persistenceQueue,
            ICandlesPersistenceManager persistenceManager,
            bool migrationEnabled)
        {
            _log = log.CreateComponentScope(nameof(StartupManager));
            _cacheInitalizationService = cacheInitalizationService;
            _candlesSubscriber = candlesSubscriber;
            _snapshotSerializer = snapshotSerializer;
            _persistenceQueueSnapshotRepository = persistenceQueueSnapshotRepository;
            _persistenceQueue = persistenceQueue;
            _persistenceManager = persistenceManager;
            _migrationEnabled = migrationEnabled;
        }

        public async Task StartAsync()
        {
            await _log.WriteInfoAsync(nameof(StartAsync), "", "Deserializing persistence queue async...");

            var tasks = new List<Task>
            {
                //_snapshotSerializer.DeserializeAsync(_persistenceQueue, _persistenceQueueSnapshotRepository)
            };

            if (!_migrationEnabled)
            {
                await _log.WriteInfoAsync(nameof(StartAsync), "", "Initializing cache from the history async...");

                //tasks.Add(_cacheInitalizationService.InitializeCacheAsync());
            }

            await _log.WriteInfoAsync(nameof(StartAsync), "", "Waiting for async tasks...");

            //await Task.WhenAll(tasks);

            await _log.WriteInfoAsync(nameof(StartAsync), "", "Starting persistence queue...");

            //_persistenceQueue.Start();

            await _log.WriteInfoAsync(nameof(StartAsync), "", "Starting persistence manager...");

            //_persistenceManager.Start();

            // We can not combine it with the previous if(!_migration...) due to launch order importance.
            if (!_migrationEnabled)
            {
                await _log.WriteInfoAsync(nameof(StartAsync), "", "Starting candles subscriber...");

                //_candlesSubscriber.Start();
            }

            await _log.WriteInfoAsync(nameof(StartAsync), "", "Started up");
        }
    }
}
