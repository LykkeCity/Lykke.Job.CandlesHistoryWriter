using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Lykke.Job.CandlesHistoryWriter.Core.Services
{
    public interface IRabbitPoisonHandingService<T> where T : class
    {
        Task<string> PutMessagesBack();
    }
}
