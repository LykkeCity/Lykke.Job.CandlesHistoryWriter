// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;
using Microsoft.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using JetBrains.Annotations;
using Newtonsoft.Json;
using Dapper;
using Lykke.Logs.MsSql.Extensions;

namespace Lykke.Job.CandleHistoryWriter.Repositories.Snapshots
{
    [UsedImplicitly(ImplicitUseKindFlags.InstantiatedNoFixedConstructorSignature)]
    public class SqlCandlesPersistenceQueueSnapshotRepository : ICandlesPersistenceQueueSnapshotRepository
    {
        private const string TableName = "CandlesPersistenceQueue";
        private const string BlobKey = "CandlesPersistenceQueu";
        private const string CreateTableScript = "CREATE TABLE [{0}](" +
                                                 "[BlobKey] [nvarchar] (64) NOT NULL PRIMARY KEY, " +
                                                 "[Data] [nvarchar] (MAX) NULL, " +
                                                 "[Timestamp] [DateTime] NULL " +
                                                 ");";

        private readonly string _connectionString;

        public SqlCandlesPersistenceQueueSnapshotRepository(string connectionString)
        {
            _connectionString = connectionString;

            using (var conn = new SqlConnection(_connectionString))
            {
                conn.CreateTableIfDoesntExists(CreateTableScript, TableName);
            }
        }

        public async Task<IImmutableList<ICandle>> TryGetAsync()
        {
            using (var conn = new SqlConnection(_connectionString))
            {
                var data = (await conn.QueryAsync<string>(
                    $"SELECT Data FROM {TableName} WHERE BlobKey=@blobKey",
                    new { blobKey = BlobKey })).SingleOrDefault();

                if (string.IsNullOrEmpty(data))
                    return null;

                var model = JsonConvert.DeserializeObject <IEnumerable < SnapshotCandleEntity >> (data);

                return model.ToImmutableList<ICandle>();
            }
        }

        public async Task SaveAsync(IImmutableList<ICandle> state)
        {

            var model = state.Select(SnapshotCandleEntity.Copy);

            var request = new
            {
                data = JsonConvert.SerializeObject(model),
                blobKey = BlobKey,
                timestamp = DateTime.Now
            };

            using (var conn = new SqlConnection(_connectionString))
            {
                try
                {
                    await conn.ExecuteAsync(
                        $"insert into {TableName} (BlobKey, Data, Timestamp) values (@blobKey, @data, @timestamp)",
                        request);
                }
                catch
                {
                    await conn.ExecuteAsync(
                        $"update {TableName} set Data=@data, Timestamp = @timestamp where BlobKey=@blobKey",
                        request);
                }
            }

        }
    }
}
