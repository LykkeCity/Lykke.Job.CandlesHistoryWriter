// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

using System.Threading.Tasks;
using BookKeeper.Client.Workflow.Events;
using JetBrains.Annotations;

namespace Lykke.Job.CandlesHistoryWriter.Services.Workflow
{
    /// <summary>
    /// Listens to <see cref="EodProcessStartedEvent"/>s and builds a projection
    /// </summary>
    [UsedImplicitly]
    public class EodStartedProjection
    {
        public EodStartedProjection()
        {
        }

        [UsedImplicitly]
        public async Task Handle(EodProcessStartedEvent @event)
        {
            
        }
    }
}
