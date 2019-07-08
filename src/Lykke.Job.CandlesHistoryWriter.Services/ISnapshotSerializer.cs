// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

using System.Threading.Tasks;
using Lykke.Job.CandlesHistoryWriter.Core.Domain;
using Lykke.Job.CandlesHistoryWriter.Core.Services;

namespace Lykke.Job.CandlesHistoryWriter.Services
{
    public interface ISnapshotSerializer
    {
        Task SerializeAsync<TState>(IHaveState<TState> stateHolder, ISnapshotRepository<TState> repository);
        Task<bool> DeserializeAsync<TState>(IHaveState<TState> stateHolder, ISnapshotRepository<TState> repository);
    }
}
