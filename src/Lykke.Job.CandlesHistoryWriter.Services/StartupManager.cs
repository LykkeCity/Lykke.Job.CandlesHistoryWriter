using System;
using System.Diagnostics;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Lykke.Job.CandlesHistoryWriter.Core.Services.Candles;
using Lykke.Sdk;

namespace Lykke.Job.CandlesHistoryWriter.Services
{
    [UsedImplicitly]
    public class StartupManager : IStartupManager
    {
        private readonly ICandlesCacheInitalizationService _cacheInitalizationService;

        public StartupManager(
            ICandlesCacheInitalizationService cacheInitalizationService)
        {
            _cacheInitalizationService = cacheInitalizationService ?? throw new ArgumentNullException(nameof(cacheInitalizationService));
        }

        public async Task StartAsync()
        {
            Console.WriteLine("Initializing cache from the history async...");

            var sw = new Stopwatch();
            sw.Start();
            await _cacheInitalizationService.InitializeCacheAsync();
            _cacheInitalizationService.ShowStat();
            sw.Stop();
            Console.WriteLine("Started up");
            Console.WriteLine(sw.Elapsed);
        }
    }
}
