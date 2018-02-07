namespace Lykke.Job.CandlesHistoryWriter.Core.Services
{
    public interface IHaveState<TState>
    {
        TState GetState();
        void SetState(TState state);
        string DescribeState(TState state);
    }
}
