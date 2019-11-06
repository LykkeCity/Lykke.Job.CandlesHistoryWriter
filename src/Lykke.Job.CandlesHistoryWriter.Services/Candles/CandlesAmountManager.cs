using Lykke.Job.CandlesHistoryWriter.Core.Services.Candles;
using Lykke.Job.CandlesHistoryWriter.Core.Settings;
using Lykke.Job.CandlesProducer.Contract;
using System;
using System.Collections.Generic;
using System.Text;

namespace Lykke.Job.CandlesHistoryWriter.Services.Candles
{
    public class CandlesAmountManager : ICandlesAmountManager
    {
        private readonly CleanupSettings _cleanupSettings;
        private readonly int _amountOfCandlesToStore;

        public CandlesAmountManager(
            CleanupSettings cleanupSettings,
            int amountOfCandlesToStore)
        {
            _cleanupSettings = cleanupSettings;
            _amountOfCandlesToStore = amountOfCandlesToStore;
        }

        public int GetCandlesAmountToStore(CandleTimeInterval timeInterval)
        {
            return _cleanupSettings.Enabled
                ? timeInterval.GetCleaupSettings(_cleanupSettings)
                : _amountOfCandlesToStore;
        }
    }
}
