using System.Threading.Tasks;
using Lykke.Job.CandlesHistoryWriter.Core.Services;
using Lykke.Job.CandlesProducer.Contract;
using Microsoft.AspNetCore.Mvc;

namespace Lykke.Job.CandlesHistoryWriter.Controllers
{
    [Route("api/[controller]")]
    public class PoisonController : Controller
    {
        private readonly IRabbitPoisonHandingService<CandlesUpdatedEvent> _rabbitPoisonHandingService;

        public PoisonController(IRabbitPoisonHandingService<CandlesUpdatedEvent> rabbitPoisonHandingService)
        {
            _rabbitPoisonHandingService = rabbitPoisonHandingService;
        }

        [HttpPost("put-messages-back")]
        public async Task<string> PutMessagesBack()
        {
            return await _rabbitPoisonHandingService.PutMessagesBack();
        }
    }
}
