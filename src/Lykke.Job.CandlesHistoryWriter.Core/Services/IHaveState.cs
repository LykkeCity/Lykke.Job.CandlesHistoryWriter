// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

namespace Lykke.Job.CandlesHistoryWriter.Core.Services
{
    public interface IHaveState<TState>
    {
        TState GetState();
        void SetState(TState state);
        string DescribeState(TState state);
    }
}
