// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

using System.Threading.Tasks;
using Lykke.Job.CandlesHistoryWriter.Core.Domain;
using Microsoft.AspNetCore.Mvc;

namespace Lykke.Job.CandlesHistoryWriter.Controllers
{
    [Route("api/[controller]")]
    public class ServiceController : Controller
    {
        private readonly ICandlesCleanup _candlesCleanup;

        public ServiceController(ICandlesCleanup candlesCleanup)
        {
            _candlesCleanup = candlesCleanup;
        }

        /// <summary>
        /// For testing purposes only!
        /// </summary>
        [HttpPost]
        public async Task InvokeCleanup()
        {
            await _candlesCleanup.Invoke();
        }
    }
}
