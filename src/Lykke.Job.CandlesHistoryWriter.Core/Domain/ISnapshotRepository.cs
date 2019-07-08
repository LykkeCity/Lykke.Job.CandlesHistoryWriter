// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

using System.Threading.Tasks;

namespace Lykke.Job.CandlesHistoryWriter.Core.Domain
{
    public interface ISnapshotRepository<TState>
    {
        Task SaveAsync(TState state);
        Task<TState> TryGetAsync();
    }
}
