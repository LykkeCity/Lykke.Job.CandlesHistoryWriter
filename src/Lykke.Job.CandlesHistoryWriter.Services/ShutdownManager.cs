using System;
using System.Threading.Tasks;
using Common.Log;
using Lykke.Job.CandleHistoryWriter.Repositories.HistoryMigration.HistoryProviders.TradesSQLHistory;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using Lykke.Job.CandlesHistoryWriter.Core.Services;
using Lykke.Job.CandlesHistoryWriter.Core.Services.Candles;
using Lykke.Job.CandlesHistoryWriter.Services.HistoryMigration;

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
            ILog log,
            ICandlesSubscriber candlesSubscriber, 
            ISnapshotSerializer snapshotSerializer,
            ICandlesPersistenceQueueSnapshotRepository persistenceQueueSnapshotRepository,
            ICandlesPersistenceQueue persistenceQueue,
            ICandlesPersistenceManager persistenceManager,
            CandlesMigrationManager migrationManager,
            bool migrationEnabled)
        {
            if (log == null)
                throw new ArgumentNullException(nameof(log));
            _log = log.CreateComponentScope(nameof(ShutdownManager)) ?? throw new InvalidOperationException("Couldn't create a component scope for logging.");

            _candlesSubcriber = candlesSubscriber ?? throw new ArgumentNullException(nameof(candlesSubscriber));
            _snapshotSerializer = snapshotSerializer ?? throw new ArgumentNullException(nameof(snapshotSerializer));
            _persistenceQueueSnapshotRepository = persistenceQueueSnapshotRepository ?? throw new ArgumentNullException(nameof(persistenceQueueSnapshotRepository));
            _persistenceQueue = persistenceQueue ?? throw new ArgumentNullException(nameof(persistenceQueue));
            _persistenceManager = persistenceManager ?? throw new ArgumentNullException(nameof(persistenceManager));
            _migrationManager = migrationManager ?? throw new ArgumentNullException(nameof(migrationManager));
            _migrationEnabled = migrationEnabled;
        }

        public async Task ShutdownAsync()
        {
            IsShuttingDown = true;

            if (!_migrationEnabled)
            {
                await _log.WriteInfoAsync(nameof(ShutdownAsync), "", "Stopping candles subscriber...");

                _candlesSubcriber.Stop();
            }

            await _log.WriteInfoAsync(nameof(ShutdownAsync), "", "Stopping persistence manager...");
                
            _persistenceManager.Stop();

            await _log.WriteInfoAsync(nameof(ShutdownAsync), "", "Stopping persistence queue...");
                
            _persistenceQueue.Stop();
            
            await _log.WriteInfoAsync(nameof(ShutdownAsync), "", "Serializing state...");

            await _snapshotSerializer.SerializeAsync(_persistenceQueue, _persistenceQueueSnapshotRepository);

            // We can not combine it with the previous if(!_migration...) due to launch order importance.
            if (_migrationEnabled)
            {
                await _log.WriteInfoAsync(nameof(ShutdownAsync), "", "Stopping candles migration manager...");

                _migrationManager.Stop();
            }

            await _log.WriteInfoAsync(nameof(ShutdownAsync), "", "Shutted down");

            IsShuttedDown = true;
            IsShuttingDown = false;
        }
    }
}
