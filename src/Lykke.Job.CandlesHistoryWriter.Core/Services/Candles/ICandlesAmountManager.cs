using Lykke.Job.CandlesProducer.Contract;
using System;
using System.Collections.Generic;
using System.Text;

namespace Lykke.Job.CandlesHistoryWriter.Core.Services.Candles
{
    public interface ICandlesAmountManager
    {
        int GetCandlesAmountToStore(CandleTimeInterval timeInterval);
    }
}
