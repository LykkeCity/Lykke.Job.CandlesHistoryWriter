using Autofac;
using Common;

namespace Lykke.Job.CandlesHistoryWriter.Core.Services.Candles
{
    public interface ICandlesSubscriber : IStartable, IStopable
    {
    }
}
