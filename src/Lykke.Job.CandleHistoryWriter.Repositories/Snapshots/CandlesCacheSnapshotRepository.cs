using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using AzureStorage;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using MessagePack;

namespace Lykke.Service.CandleHistory.Repositories.Snapshots
{
    public class CandlesCacheSnapshotRepository : ICandlesCacheSnapshotRepository
    {
        private const string Key = "CandlesCache";

        private readonly IBlobStorage _storage;

        public CandlesCacheSnapshotRepository(IBlobStorage storage)
        {
            _storage = storage;
        }

        public async Task SaveAsync(IImmutableDictionary<string, IImmutableList<ICandle>> state)
        {
            using (var stream = new MemoryStream())
            {
                var model = state.ToDictionary(i => i.Key, i => i.Value.Select(SnapshotCandleEntity.Copy));

                MessagePackSerializer.Serialize(stream, model);

                await stream.FlushAsync();
                stream.Seek(0, SeekOrigin.Begin);

                await _storage.SaveBlobAsync(Constants.SnapshotsContainer, Key, stream);
            }
        }

        public async Task<IImmutableDictionary<string, IImmutableList<ICandle>>> TryGetAsync()
        {
            if (!await _storage.HasBlobAsync(Constants.SnapshotsContainer, Key))
            {
                return null;
            }

            using (var stream = await _storage.GetAsync(Constants.SnapshotsContainer, Key))
            {
                var model = MessagePackSerializer.Deserialize<Dictionary<string, IEnumerable<SnapshotCandleEntity>>>(stream);

                return model.ToImmutableDictionary(i => i.Key, i => (IImmutableList<ICandle>) i.Value.ToImmutableList<ICandle>());
            }
        }
    }
}
