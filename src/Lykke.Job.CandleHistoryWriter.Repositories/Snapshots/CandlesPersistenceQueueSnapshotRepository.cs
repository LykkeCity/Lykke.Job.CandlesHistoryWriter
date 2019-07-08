// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using AzureStorage;
using JetBrains.Annotations;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using MessagePack;

namespace Lykke.Job.CandleHistoryWriter.Repositories.Snapshots
{
    [UsedImplicitly(ImplicitUseKindFlags.InstantiatedNoFixedConstructorSignature)]
    public class CandlesPersistenceQueueSnapshotRepository : ICandlesPersistenceQueueSnapshotRepository
    {
        private const string Key = "CandlesPersistenceQueue";

        private readonly IBlobStorage _storage;

        public CandlesPersistenceQueueSnapshotRepository(IBlobStorage storage)
        {
            _storage = storage;
        }

        public async Task SaveAsync(IImmutableList<ICandle> state)
        {
            using (var stream = new MemoryStream())
            {
                var model = state.Select(SnapshotCandleEntity.Copy);

                MessagePackSerializer.Serialize(stream, model);

                await stream.FlushAsync();
                stream.Seek(0, SeekOrigin.Begin);

                await _storage.SaveBlobAsync(Constants.SnapshotsContainer, Key, stream);
            }
        }

        public async Task<IImmutableList<ICandle>> TryGetAsync()
        {
            if (!await _storage.HasBlobAsync(Constants.SnapshotsContainer, Key))
            {
                return null;
            }

            using (var stream = await _storage.GetAsync(Constants.SnapshotsContainer, Key))
            {
                var model = MessagePackSerializer.Deserialize<IEnumerable<SnapshotCandleEntity>>(stream);

                return model.ToImmutableList<ICandle>();
            }
        }
    }
}
