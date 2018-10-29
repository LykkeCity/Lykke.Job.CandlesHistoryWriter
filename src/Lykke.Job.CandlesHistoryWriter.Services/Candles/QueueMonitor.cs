using System.Threading.Tasks;
using Common;
using Common.Log;
using Lykke.Common.Log;
using Lykke.Job.CandlesHistoryWriter.Core.Services;
using Lykke.Job.CandlesHistoryWriter.Services.Settings;

namespace Lykke.Job.CandlesHistoryWriter.Services.Candles
{
    /// <summary>
    /// Monitors length of tasks queue.
    /// </summary>
    public class QueueMonitor : TimerPeriod
    {
        private readonly QueueMonitorSettings _setting;
        private readonly ILog _log;
        private readonly IHealthService _healthService;

        public QueueMonitor(
            ILogFactory logFactory, 
            IHealthService healthService,
            QueueMonitorSettings setting)
            : base(setting.ScanPeriod, logFactory, nameof(QueueMonitor))
        {
            _log = logFactory.CreateLog(this);
            _healthService = healthService;
            _setting = setting;
        }

        public override Task Execute()
        {
            var currentBatchesQueueLength = _healthService.BatchesToPersistQueueLength;
            var currentCandlesQueueLength = _healthService.CandlesToDispatchQueueLength;

            if (currentBatchesQueueLength > _setting.BatchesToPersistQueueLengthWarning ||
                currentCandlesQueueLength > _setting.CandlesToDispatchQueueLengthWarning)
            {
                _log.Warning(nameof(Execute),
                    $@"One of processing queue's size exceeded warning level. 
Candles batches to persist queue length={currentBatchesQueueLength} (warning={_setting.BatchesToPersistQueueLengthWarning}).
Candles to dispatch queue length={currentCandlesQueueLength} (warning={_setting.CandlesToDispatchQueueLengthWarning})");
            }
            
            return Task.CompletedTask;
        }
    }
}
