// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

using System.Threading.Tasks;
using Common;
using Common.Log;
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
            ILog log, 
            IHealthService healthService,
            QueueMonitorSettings setting)
            : base(nameof(QueueMonitor), (int)setting.ScanPeriod.TotalMilliseconds, log)
        {
            _log = log;
            _healthService = healthService;
            _setting = setting;
        }

        public override async Task Execute()
        {
            var currentBatchesQueueLength = _healthService.BatchesToPersistQueueLength;
            var currentCandlesQueueLength = _healthService.CandlesToDispatchQueueLength;

            if (currentBatchesQueueLength > _setting.BatchesToPersistQueueLengthWarning ||
                currentCandlesQueueLength > _setting.CandlesToDispatchQueueLengthWarning)
            {
                await _log.WriteWarningAsync(
                    nameof(QueueMonitor),
                    nameof(Execute),
                    "",
                    $@"One of processing queue's size exceeded warning level. 
Candles batches to persist queue length={currentBatchesQueueLength} (warning={_setting.BatchesToPersistQueueLengthWarning}).
Candles to dispatch queue length={currentCandlesQueueLength} (warning={_setting.CandlesToDispatchQueueLengthWarning})");
            }
        }
    }
}
