using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Common.Log;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.HistoryMigration.HistoryProviders.TradesSQLHistory;
using Lykke.Job.CandlesProducer.Contract;

namespace Lykke.Job.CandleHistoryWriter.Repositories.HistoryMigration.HistoryProviders.TradesSQLHistory
{
    public class TradesSqlHistoryRepository : ITradesSqlHistoryRepository, IDisposable
    {
        private readonly int _sqlQueryBatchSize;
        private readonly SqlConnection _sqlConnection;
        private readonly TimeSpan _sqlTimeout;

        private readonly ILog _log;

        private int StartingRowOffset { get; set; }
        private bool _gotTheLastBatch;

        private string AssetPairId { get; }
        private string SearchToken { get; }
        private DateTime? MigrateByDate { get; }

        public TradesSqlHistoryRepository(
            string sqlConnString,
            int sqlQueryBatchSize,
            TimeSpan sqlTimeout,
            ILog log,
            DateTime? migrateByDate,
            string assetPairId,
            string searchToken
            )
        {
            _sqlQueryBatchSize = sqlQueryBatchSize;
            _sqlTimeout = sqlTimeout;
            _log = log;

            StartingRowOffset = 0; // Will read everything.

            AssetPairId = assetPairId;
            SearchToken = searchToken;
            MigrateByDate = migrateByDate;

            _sqlConnection = new SqlConnection(sqlConnString);
            _sqlConnection.Open();
        }
        
        public async Task<IReadOnlyCollection<TradeHistoryItem>> GetNextBatchAsync()
        {
            var result = new List<TradeHistoryItem>();

            // If we got the last batch in the previous iteration, there is no reason to execute one more query
            // with empty result. Just return.
            if (_gotTheLastBatch)
                return result;

            try
            {
                if (_sqlConnection == null || _sqlConnection.State != ConnectionState.Open)
                    throw new InvalidOperationException("Can't fetch from DB while connection is not opened.");

                await _log.WriteInfoAsync(nameof(TradesSqlHistoryRepository), nameof(GetNextBatchAsync),
                    $"Starting offset = {StartingRowOffset}, asset pair ID = {AssetPairId}",
                    $"Trying to fetch next {_sqlQueryBatchSize} rows...");
                
                using (var sqlCommand = new SqlCommand(BuildCurrentQueryCommand(), _sqlConnection))
                {
                    sqlCommand.CommandTimeout = (int)_sqlTimeout.TotalSeconds;
                    using (var reader = await sqlCommand.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            result.Add(new TradeHistoryItem
                            {
                                Id = reader.GetInt64(0),
                                AssetToken = reader.GetString(1),
                                Direction = (TradeDirection)Enum.Parse(typeof(TradeDirection), reader.GetString(2)),
                                Volume = reader.GetDecimal(3),
                                Price = reader.GetDecimal(4),
                                DateTime = reader.GetDateTime(5),
                                OppositeVolume = reader.GetDecimal(6),
                                OrderId = Guid.Parse(reader.GetString(7)),
                                OppositeOrderId = Guid.Parse(reader.GetString(8))
                            });
                        }
                    }
                }

                if (result.Count > 0)
                {
                    // Now we need to remove the last several trades which have the same date and time (accuracy - to seconds).
                    // This will guarantee that we did not peek up some orders of the same trade on this iteration, and others
                    // on the next. On the next iteration we will read them again for the next batch. No temporary buffer, for
                    // it can't save any observable value of time. NOTE: if we have got less records than _sqlQueryBatchSize,
                    // this means that we obtained the last )or the single) data pack, and there is no reason to delete "tail"
                    // trades.

                    if (result.Count == _sqlQueryBatchSize)
                    {
                        // I do not want to use Linq.Remove(func) for it might case full sequence iterating which is slower than
                        // the following messy code. Actually, we need to iterate only a bit of elements from the tail-side.
                        var lastDateTime = result.Last().DateTime.TruncateTo(CandleTimeInterval.Sec);
                        var equalTimestampCandlesAtTailCount = 1;
                        while (equalTimestampCandlesAtTailCount++ < result.Count) // Looking through the list starting from the last element. No additional work.
                        {
                            if (result[result.Count - equalTimestampCandlesAtTailCount].DateTime.TruncateTo(CandleTimeInterval.Sec) != lastDateTime)
                            {
                                equalTimestampCandlesAtTailCount--; // When we got the first inconsistent element, we need to step back.
                                break;
                            }
                        }

                        // Now remove the candles with the same (to seconds) timestamp ONLY in case if we do have any other candles in the list.
                        if (equalTimestampCandlesAtTailCount < result.Count)
                            result.RemoveRange(result.Count - equalTimestampCandlesAtTailCount, equalTimestampCandlesAtTailCount);
                    }
                    else _gotTheLastBatch = true; // If we have got smaller amount of records than _sqlQueryBatchSize, this only means we have the last batch now.

                    // Reporting.
                    await _log.WriteInfoAsync(nameof(TradesSqlHistoryRepository), nameof(GetNextBatchAsync),
                        $"Starting offset = {StartingRowOffset}, asset pair ID = {AssetPairId}",
                        $"Fetched {result.Count} rows successfully.");

                    StartingRowOffset += result.Count;
                }
                else
                    await _log.WriteInfoAsync(nameof(TradesSqlHistoryRepository), nameof(GetNextBatchAsync),
                        $"Starting offset = {StartingRowOffset}, asset pair ID = {AssetPairId}",
                        $"No data to fetch.");
            }
            catch (Exception ex)
            {
                await _log.WriteErrorAsync(nameof(TradesSqlHistoryRepository), nameof(GetNextBatchAsync), ex);
                result?.Clear();
            }

            return result;
        }

        public void Dispose()
        {
            _sqlConnection?.Close();
        }

        private string BuildCurrentQueryCommand()
        {
            var commandBld = new StringBuilder();

            // TODO: implement a query with pagination.
            commandBld.Append($@"SELECT Id, (Asset + OppositeAsset) AS AssetToken, Direction, Volume, Price, ""DateTime"", OppositeVolume, OrderId, OppositeOrderId ");
            commandBld.Append("FROM Trades ");
            commandBld.Append("WHERE ");
            commandBld.Append($@"OrderType = 'Limit' AND (Asset + OppositeAsset = '{SearchToken}' OR OppositeAsset + Asset = '{SearchToken}') AND Direction IN ('Buy', 'Sell') ");
            if (MigrateByDate.HasValue)
                commandBld.Append($@"AND ""DateTime"" < TRY_PARSE('{MigrateByDate.Value.ToString("o")}' AS DateTime) ");
            commandBld.Append(@"ORDER BY ""DateTime"", Id ASC ");
            commandBld.Append($"OFFSET {StartingRowOffset} ROWS FETCH NEXT {_sqlQueryBatchSize} ROWS ONLY;");

            return commandBld.ToString();
        }
    }
}
