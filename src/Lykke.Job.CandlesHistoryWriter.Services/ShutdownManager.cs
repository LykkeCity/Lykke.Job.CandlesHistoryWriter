using System;
using System.Threading.Tasks;
using Common.Log;
using Lykke.Common.Log;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using Lykke.Job.CandlesHistoryWriter.Core.Services.Candles;
using Lykke.Job.CandlesHistoryWriter.Services.HistoryMigration;
using Lykke.Sdk;

namespace Lykke.Job.CandlesHistoryWriter.Services
{
    public class ShutdownManager : IShutdownManager
    {
        public bool IsShuttedDown { get; private set; }
        public bool IsShuttingDown { get; private set; }

        private readonly ILog _log;
        private readonly ICandlesSubscriber _candlesSubcriber;
        private readonly ISnapshotSerializer _snapshotSerializer;
        private readonly ICandlesPersistenceQueueSnapshotRepository _persistenceQueueSnapshotRepository;
        private readonly ICandlesPersistenceQueue _persistenceQueue;
        private readonly ICandlesPersistenceManager _persistenceManager;
        private readonly CandlesMigrationManager _migrationManager;
        private readonly bool _migrationEnabled;

        public ShutdownManager(
            ILogFactory logFactory,
            ICandlesSubscriber candlesSubscriber,
            ISnapshotSerializer snapshotSerializer,
            ICandlesPersistenceQueueSnapshotRepository persistenceQueueSnapshotRepository,
            ICandlesPersistenceQueue persistenceQueue,
            ICandlesPersistenceManager persistenceManager,
            CandlesMigrationManager migrationManager,
            bool migrationEnabled)
        {
            if (logFactory == null)
                throw new ArgumentNullException(nameof(logFactory));

            _log = logFactory.CreateLog(this);

            _candlesSubcriber = candlesSubscriber ?? throw new ArgumentNullException(nameof(candlesSubscriber));
            _snapshotSerializer = snapshotSerializer ?? throw new ArgumentNullException(nameof(snapshotSerializer));
            _persistenceQueueSnapshotRepository = persistenceQueueSnapshotRepository ?? throw new ArgumentNullException(nameof(persistenceQueueSnapshotRepository));
            _persistenceQueue = persistenceQueue ?? throw new ArgumentNullException(nameof(persistenceQueue));
            _persistenceManager = persistenceManager ?? throw new ArgumentNullException(nameof(persistenceManager));
            _migrationManager = migrationManager ?? throw new ArgumentNullException(nameof(migrationManager));
            _migrationEnabled = migrationEnabled;
        }

        public async Task StopAsync()
        {
            IsShuttingDown = true;

            if (!_migrationEnabled)
            {
                _log.Info(nameof(StopAsync), "Stopping candles subscriber...");

                _candlesSubcriber.Stop();
            }

            _log.Info(nameof(StopAsync), "Stopping persistence manager...");

            _persistenceManager.Stop();

            _log.Info(nameof(StopAsync), "Stopping persistence queue...");

            _persistenceQueue.Stop();

            _log.Info(nameof(StopAsync), "Serializing state...");

            await _snapshotSerializer.SerializeAsync(_persistenceQueue, _persistenceQueueSnapshotRepository);

            // We can not combine it with the previous if(!_migration...) due to launch order importance.
            if (_migrationEnabled)
            {
                _log.Info(nameof(StopAsync), "Stopping candles migration manager...");

                _migrationManager.Stop();
            }

            _log.Info(nameof(StopAsync), "Shutted down");

            IsShuttedDown = true;
            IsShuttingDown = false;
        }
    }
}
