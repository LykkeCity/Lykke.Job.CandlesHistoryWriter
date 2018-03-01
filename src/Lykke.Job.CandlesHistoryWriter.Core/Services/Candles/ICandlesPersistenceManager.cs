using Autofac;
using Common;

namespace Lykke.Job.CandlesHistoryWriter.Core.Services.Candles
{
    public interface ICandlesPersistenceManager : IStartable, IStopable
    {
    }
}
