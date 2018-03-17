using System;
using System.Text;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;
using Common.Log;
using JetBrains.Annotations;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.HistoryMigration.HistoryProviders.TradesSQLHistory;

namespace Lykke.Job.CandleHistoryWriter.Repositories.HistoryMigration.HistoryProviders.TradesSQLHistory
{
    [UsedImplicitly]
    public class TradesSqlHistoryRepository : ITradesSqlHistoryRepository, IDisposable
    {
        private readonly string _sqlConnString;
        private readonly int _sqlQueryBatchSize;
        private readonly ILog _log;

        private SqlConnection _sqlConnection;

        public int StartingRowOffset { get; private set; }
        public string AssetPairId { get; private set; }

        public TradesSqlHistoryRepository(
            string sqlConnString,
            int sqlQueryBatchSize,
            ILog log
            )
        {
            _sqlConnString = sqlConnString;
            _sqlQueryBatchSize = sqlQueryBatchSize;
            _log = log;
        }

        public async Task<bool> InitAsync(int startingRowOffset, string assetPairId)
        {
            if (_sqlConnection == null)
                _sqlConnection = new SqlConnection(_sqlConnString);

            // Cant init when the connection is not idle.
            if (_sqlConnection.State == ConnectionState.Connecting ||
                _sqlConnection.State == ConnectionState.Executing ||
                _sqlConnection.State == ConnectionState.Fetching)
                return false;

            StartingRowOffset = startingRowOffset;
            AssetPairId = assetPairId;
            
            // If already opened, there is no any work to do.
            if (_sqlConnection.State == ConnectionState.Open)
                return true;

            // The last case: the connection is Broken or just Closed (or not opened yet).
            if (_sqlConnection.State == ConnectionState.Broken)
                _sqlConnection.Close();

            await _sqlConnection.OpenAsync();
            return true;
        }

        public async Task<IEnumerable<TradeHistoryItem>> GetNextBatchAsync()
        {
            var result = new List<TradeHistoryItem>();

            try
            {
                if (_sqlConnection == null || _sqlConnection.State != ConnectionState.Open)
                    throw new InvalidOperationException("Can't fetch from DB while connection is not opened. You should call InitAsync first.");

                await _log.WriteMonitorAsync(nameof(TradesSqlHistoryRepository), nameof(GetNextBatchAsync),
                    $"Starting offset = {StartingRowOffset}, asset pair ID = {AssetPairId}",
                    $"Trying to fetch next {_sqlQueryBatchSize} rows...");
                
                using (var sqlCommand = new SqlCommand(_buildCurrentQueryCommand(), _sqlConnection))
                {
                    using (var reader = await sqlCommand.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            result.Add(new TradeHistoryItem
                            {
                                Id = reader.GetInt64(0),
                                Volume = reader.GetDecimal(1),
                                Price = reader.GetDecimal(2),
                                DateTime = reader.GetDateTime(3),
                                OppositeVolume = reader.GetDecimal(4)
                            });
                        }
                    }
                }

                await _log.WriteMonitorAsync(nameof(TradesSqlHistoryRepository), nameof(GetNextBatchAsync),
                    $"Starting offset = {StartingRowOffset}, asset pair ID = {AssetPairId}",
                    $"Fetched {_sqlQueryBatchSize} rows successfully.");

                StartingRowOffset += result.Count;
            }
            catch (Exception ex)
            {
                await _log.WriteErrorAsync(nameof(TradesSqlHistoryRepository), nameof(GetNextBatchAsync), ex);
            }

            return result;
        }

        public void Dispose()
        {
            _sqlConnection?.Close();
        }

        private string _buildCurrentQueryCommand()
        {
            var commandBld = new StringBuilder();

            // TODO: implement a query with pagination.
            //commandBld.Append($@"SELECT TOP {_sqlQueryBatchSize} Volume, Price, ""DateTime"", OppositeVolume ");
            commandBld.Append($@"SELECT Id, Volume, Price, ""DateTime"", OppositeVolume ");
            commandBld.Append("FROM Trades ");
            commandBld.Append("WHERE ");
            commandBld.Append($@"OrderType = 'Limit' AND Asset + OppositeAsset = '{AssetPairId}' AND Direction IN ('Buy', 'Sell') ");
            commandBld.Append("ORDER BY Id ASC ");
            commandBld.Append($"OFFSET {StartingRowOffset} ROWS FETCH NEXT {_sqlQueryBatchSize} ROWS ONLY;");

            return commandBld.ToString();
        }
    }
}
