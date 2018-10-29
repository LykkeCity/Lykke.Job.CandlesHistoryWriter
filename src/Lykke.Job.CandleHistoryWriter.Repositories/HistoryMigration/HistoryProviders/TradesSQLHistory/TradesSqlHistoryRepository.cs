﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Common.Log;
using Lykke.Common.Log;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.HistoryMigration.HistoryProviders.TradesSQLHistory;
using Lykke.Job.CandlesProducer.Contract;

namespace Lykke.Job.CandleHistoryWriter.Repositories.HistoryMigration.HistoryProviders.TradesSQLHistory
{
    public class TradesSqlHistoryRepository : ITradesSqlHistoryRepository
    {
        private readonly int _sqlQueryBatchSize;
        private readonly string _sqlConnString;
        private readonly TimeSpan _sqlTimeout;
        private readonly IHealthNotifier _healthNotifier;

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
            ILogFactory logFactory,
            IHealthNotifier healthNotifier,
            DateTime? migrateByDate,
            string assetPairId,
            string searchToken
            )
        {
            _sqlQueryBatchSize = sqlQueryBatchSize;
            _sqlConnString = !string.IsNullOrWhiteSpace(sqlConnString) ? sqlConnString : throw new ArgumentNullException(nameof(sqlConnString));
            _sqlTimeout = sqlTimeout;
            _healthNotifier = healthNotifier;

            if (logFactory == null)
                throw new ArgumentNullException(nameof(logFactory));

            _log = logFactory.CreateLog(this);

            StartingRowOffset = 0; // Will read everything.

            AssetPairId = assetPairId;
            SearchToken = searchToken;
            MigrateByDate = migrateByDate;
        }
        
        public async Task<IReadOnlyCollection<TradeHistoryItem>> GetNextBatchAsync()
        {
            // If we got the last batch in the previous iteration, there is no reason to execute one more query
            // with empty result. Just return.
            if (_gotTheLastBatch)
                return Array.Empty<TradeHistoryItem>();

            try
            {
                var result = new List<TradeHistoryItem>();

                // Renew the connection on every call.
                using (var sqlConnection = new SqlConnection(_sqlConnString))
                {
                    sqlConnection.Open();

                    if (sqlConnection.State != ConnectionState.Open)
                        throw new InvalidOperationException("Can't fetch from DB while connection is not opened.");

                    _log.Info(nameof(GetNextBatchAsync),
                        $"Trying to fetch next {_sqlQueryBatchSize} rows...",
                        $"Starting offset = {StartingRowOffset}, asset pair ID = {AssetPairId}");

                    using (var sqlCommand = BuildCurrentQueryCommand(sqlConnection))
                    {
                        sqlCommand.CommandTimeout = (int) _sqlTimeout.TotalSeconds;
                        using (var reader = await sqlCommand.ExecuteReaderAsync())
                        {
                            while (await reader.ReadAsync())
                            {
                                var trade = new TradeHistoryItem
                                {
                                    Id = reader.GetInt64(0),
                                    AssetToken = reader.GetString(1),
                                    Direction =
                                        (TradeDirection) Enum.Parse(typeof(TradeDirection), reader.GetString(2)),
                                    Volume = reader.GetDecimal(3),
                                    Price = reader.GetDecimal(4),
                                    DateTime = reader.GetDateTime(5),
                                    OppositeVolume = reader.GetDecimal(6),
                                    OrderId = Guid.Parse(reader.GetString(7)),
                                    OppositeOrderId = Guid.Parse(reader.GetString(8)),
                                    TradeId = reader.GetString(9),
                                    IsStraight =
                                        reader.GetString(1) == SearchToken // If the trade is straight or reverse.
                                };

                                // We must ignore trades with negative prices and\or volumes (if any).
                                if (trade.Price > 0 &&
                                    trade.Volume > 0 &&
                                    trade.OppositeVolume > 0)
                                {
                                    result.Add(trade);
                                }
                                else
                                {
                                    _healthNotifier.Notify("Got a trade with non-posotive price or volume(s) values.", trade);
                                }
                            }
                        }
                    }

                    sqlConnection.Close();
                }

                if (result.Count > 0)
                {
                    // Now we need to remove the last several trades which have the same date and time (accuracy - to seconds).
                    // This will guarantee that we did not peek up some orders of the same trade on this iteration, and others
                    // on the next. On the next iteration we will read them again for the next batch. No temporary buffer, for
                    // it can't save any observable value of time. NOTE: if we have got less records than _sqlQueryBatchSize,
                    // this means that we obtained the last (or the single) data pack, and there is no reason to delete "tail"
                    // trades.

                    if (result.Count == _sqlQueryBatchSize)
                    {
                        var lastDateTime = result.Last().DateTime.TruncateTo(CandleTimeInterval.Sec);
                        var resultWithoutTail = result.TakeWhile(t => t.DateTime < lastDateTime).ToList();

                        if (!resultWithoutTail.Any())
                            throw new InvalidOperationException($"Got an SQL data batch of {result.Count} trade records with the same timestamp {lastDateTime:O}. " +
                                                                $"Migration for asset pair {AssetPairId} will be terminated. Row offset was {StartingRowOffset} before the incident.");

                        result = resultWithoutTail;
                    }
                    else _gotTheLastBatch = true; // If we have got smaller amount of records than _sqlQueryBatchSize, this only means we have the last batch now.

                    _log.Info(nameof(GetNextBatchAsync),
                        $"Fetched {result.Count} rows successfully. First date is {result.First().DateTime:O}, last date is {result.Last().DateTime:O}",
                        $"Starting offset = {StartingRowOffset}, asset pair ID = {AssetPairId}");

                    StartingRowOffset += result.Count;
                }
                else
                    _log.Info(nameof(GetNextBatchAsync),
                        "No data to fetch.",
                        $"Starting offset = {StartingRowOffset}, asset pair ID = {AssetPairId}");

                return result;
            }
            catch (Exception ex)
            {
                _log.Error(nameof(GetNextBatchAsync), ex);
                // We can just report about the error and return an empty list - this will be interpreted as "no data".
                return Array.Empty<TradeHistoryItem>();
            }
        }

        private SqlCommand BuildCurrentQueryCommand(SqlConnection conn)
        {
            var sqlParameters = new List<SqlParameter>
            {
                new SqlParameter("@SearchToken", SearchToken),
                new SqlParameter("@StartingRowOffset", StartingRowOffset),
                new SqlParameter("@QueryBatchSize", _sqlQueryBatchSize)
            };

            var commandBld = new StringBuilder();

            commandBld.Append(@"SELECT Id, (Asset + OppositeAsset) AS AssetToken, Direction, Volume, Price, ""DateTime"", OppositeVolume, OrderId, OppositeOrderId, TradeId ");
            commandBld.Append("FROM Trades ");
            commandBld.Append("WHERE ");
            commandBld.Append(@"OrderType = 'Limit' AND (Asset + OppositeAsset = @SearchToken OR OppositeAsset + Asset = @SearchToken) AND Direction IN ('Buy', 'Sell') ");
            if (MigrateByDate.HasValue)
            {
                sqlParameters.Add(new SqlParameter("@MigrateByDate", MigrateByDate));
                commandBld.Append(@"AND ""DateTime"" < @MigrateByDate ");
            }

            commandBld.Append(@"ORDER BY ""DateTime"", Id ASC ");
            commandBld.Append("OFFSET @StartingRowOffset ROWS FETCH NEXT @QueryBatchSize ROWS ONLY;");

            var sqlCommand = new SqlCommand(commandBld.ToString(), conn);
            sqlCommand.Parameters.AddRange(sqlParameters.ToArray());

            return sqlCommand;
        }
    }
}
