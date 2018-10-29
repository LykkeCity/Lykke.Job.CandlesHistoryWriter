using System;
using System.Threading.Tasks;
using Common.Log;
using Lykke.Common.Log;
using Lykke.Job.CandlesHistoryWriter.Core.Domain;
using Lykke.Job.CandlesHistoryWriter.Core.Services;

namespace Lykke.Job.CandlesHistoryWriter.Services
{
    public class SnapshotSerializer : ISnapshotSerializer
    {
        private readonly ILogFactory _logFactory;

        public SnapshotSerializer(ILogFactory logFactory)
        {
            _logFactory = logFactory ?? throw new ArgumentNullException(nameof(logFactory));
        }

        public async Task SerializeAsync<TState>(IHaveState<TState> stateHolder, ISnapshotRepository<TState> repository)
        {
            await SerializeAsync(_logFactory.CreateLog($"{nameof(SnapshotSerializer)}[{stateHolder.GetType().Name}]"), stateHolder, repository);
        }

        public async Task<bool> DeserializeAsync<TState>(IHaveState<TState> stateHolder, ISnapshotRepository<TState> repository)
        {
            return await DeserializeAsync(_logFactory.CreateLog($"{nameof(SnapshotSerializer)}[{stateHolder.GetType().Name}]"), stateHolder, repository);
        }

        private static async Task SerializeAsync<TState>(ILog log, IHaveState<TState> stateHolder, ISnapshotRepository<TState> repository)
        {
            log.Info(nameof(SerializeAsync), "Gettings state...");

            TState state;

            try
            {
                state = stateHolder.GetState();
            }
            catch (NotSupportedException)
            {
                log.Warning(nameof(SerializeAsync), "Not supported, skipping");
                return;
            }

            log.Info(nameof(SerializeAsync), "Saving state...", stateHolder.DescribeState(state));

            await repository.SaveAsync(state);

            log.Info(nameof(SerializeAsync), "State saved");
        }

        private async Task<bool> DeserializeAsync<TState>(ILog log, IHaveState<TState> stateHolder, ISnapshotRepository<TState> repository)
        {
            log.Info(nameof(DeserializeAsync), "Loading state...");

            var state = await repository.TryGetAsync();

            if (state == null)
            {
                log.Warning(nameof(DeserializeAsync), "No snapshot found to deserialize", context: stateHolder.GetType().Name);
                return false;
            }

            string stateDescription;

            try
            {
                stateDescription = stateHolder.DescribeState(state);
            }
            catch (NotSupportedException)
            {
                log.Warning(nameof(DeserializeAsync), "Not supported, skipping");
                return false;
            }

            log.Info(nameof(DeserializeAsync), "Settings state...", stateDescription);

            try
            {
                stateHolder.SetState(state);
            }
            catch (NotSupportedException)
            {
                log.Warning(nameof(DeserializeAsync), "Not supported, skipping");
                return false;
            }

            log.Info(nameof(DeserializeAsync), "State was set");

            return true;
        }
    }
}
