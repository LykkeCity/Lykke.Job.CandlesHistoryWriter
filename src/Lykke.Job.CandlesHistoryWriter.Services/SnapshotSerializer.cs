using System;
using System.Threading.Tasks;
using Common.Log;
using Lykke.Job.CandlesHistoryWriter.Core.Domain;
using Lykke.Job.CandlesHistoryWriter.Core.Services;

namespace Lykke.Job.CandlesHistoryWriter.Services
{
    public class SnapshotSerializer : ISnapshotSerializer
    {
        private readonly ILog _log;

        public SnapshotSerializer(ILog log)
        {
            _log = log ?? throw new ArgumentNullException(nameof(log));
        }

        public async Task SerializeAsync<TState>(IHaveState<TState> stateHolder, ISnapshotRepository<TState> repository)
        {
            await SerializeAsync(_log.CreateComponentScope($"{nameof(SnapshotSerializer)}[{stateHolder.GetType().Name}]"), stateHolder, repository);
        }

        public async Task<bool> DeserializeAsync<TState>(IHaveState<TState> stateHolder, ISnapshotRepository<TState> repository)
        {
            return await DeserializeAsync(_log.CreateComponentScope($"{nameof(SnapshotSerializer)}[{stateHolder.GetType().Name}]"), stateHolder, repository);
        }

        private static async Task SerializeAsync<TState>(ILog log, IHaveState<TState> stateHolder, ISnapshotRepository<TState> repository)
        {
            await log.WriteInfoAsync(nameof(SerializeAsync), "", "Gettings state...");

            TState state;

            try
            {
                state = stateHolder.GetState();
            }
            catch (NotSupportedException)
            {
                await log.WriteWarningAsync(nameof(SerializeAsync), "", "Not supported, skipping");
                return;
            }

            await log.WriteInfoAsync(nameof(SerializeAsync), stateHolder.DescribeState(state), "Saving state...");

            await repository.SaveAsync(state);

            await log.WriteInfoAsync(nameof(SerializeAsync), "", "State saved");
        }

        private async Task<bool> DeserializeAsync<TState>(ILog log, IHaveState<TState> stateHolder, ISnapshotRepository<TState> repository)
        {
            await log.WriteInfoAsync(nameof(DeserializeAsync), "", "Loading state...");

            var state = await repository.TryGetAsync();

            if (state == null)
            {
                await log.WriteWarningAsync("SnapshotSerializer", nameof(DeserializeAsync),
                    stateHolder.GetType().Name, "No snapshot found to deserialize");

                return false;
            }

            string stateDescription;

            try
            {
                stateDescription = stateHolder.DescribeState(state);
            }
            catch (NotSupportedException)
            {
                await log.WriteWarningAsync(nameof(DeserializeAsync), "", "Not supported, skipping");
                return false;
            }

            await log.WriteInfoAsync(nameof(DeserializeAsync), stateDescription, "Settings state...");

            try
            {
                stateHolder.SetState(state);
            }
            catch (NotSupportedException)
            {
                await log.WriteWarningAsync(nameof(DeserializeAsync), "", "Not supported, skipping");
                return false;
            }

            await log.WriteInfoAsync(nameof(DeserializeAsync), "", "State was set");

            return true;
        }
    }
}
