using System;
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
            return Task.CompletedTask;
        }
    }
}
