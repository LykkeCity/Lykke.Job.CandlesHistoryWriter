using System.Threading.Tasks;
using Common.Log;
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

        public ShutdownManager(
            ILog log,
            ICandlesSubscriber candlesSubscriber, 
            ISnapshotSerializer snapshotSerializer,
            ICandlesPersistenceQueueSnapshotRepository persistenceQueueSnapshotRepository,
            ICandlesPersistenceQueue persistenceQueue,
            ICandlesPersistenceManager persistenceManager,
            CandlesMigrationManager migrationManager)
        {
            _log = log.CreateComponentScope(nameof(ShutdownManager));
            _candlesSubcriber = candlesSubscriber;
            _snapshotSerializer = snapshotSerializer;
            _persistenceQueueSnapshotRepository = persistenceQueueSnapshotRepository;
            _persistenceQueue = persistenceQueue;
            _persistenceManager = persistenceManager;
            _migrationManager = migrationManager;
        }

        public async Task ShutdownAsync()
        {
            IsShuttingDown = true;

            await _log.WriteInfoAsync(nameof(ShutdownAsync), "", "Stopping candles subscriber...");

            //_candlesSubcriber.Stop();

            await _log.WriteInfoAsync(nameof(ShutdownAsync), "", "Stopping persistence manager...");
                
            //_persistenceManager.Stop();

            await _log.WriteInfoAsync(nameof(ShutdownAsync), "", "Stopping persistence queue...");
                
            //_persistenceQueue.Stop();
            
            await _log.WriteInfoAsync(nameof(ShutdownAsync), "", "Serializing state...");

           // await _snapshotSerializer.SerializeAsync(_persistenceQueue, _persistenceQueueSnapshotRepository);

            await _log.WriteInfoAsync(nameof(ShutdownAsync), "", "Stopping candles migration manager...");

            //_migrationManager.Stop();

            await _log.WriteInfoAsync(nameof(ShutdownAsync), "", "Shutted down");

            IsShuttedDown = true;
            IsShuttingDown = false;
        }
    }
}
