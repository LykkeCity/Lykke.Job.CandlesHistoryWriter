using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Common.Log;
using Dapper;
using Lykke.Job.CandleHistoryWriter.Repositories.Extensions;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using Lykke.Job.CandlesProducer.Contract;
using Lykke.Service.Assets.Client.Models;
using Microsoft.Data.OData.Query.SemanticAst;
using MoreLinq;
using Newtonsoft.Json;

namespace Lykke.Job.CandleHistoryWriter.Repositories.Candles
{
    public class SqlAssetPairCandlesHistoryRepository
    {
      
        private const string CreateTableScript = "CREATE TABLE [{0}](" +
                                                 "[Id] [bigint] NOT NULL IDENTITY(1,1) PRIMARY KEY," +
                                                 "[AssetPairId] [nvarchar] (64) NOT NULL, " +
                                                 "[PriceType] [int] NOT NULL ," +
                                                 "[Open] [float] NOT NULL, " +
                                                 "[Close] [float] NOT NULL, " +
                                                 "[High] [float] NOT NULL, " +
                                                 "[Low] [float] NOT NULL, " +
                                                 "[TimeInterval] [int] NOT NULL, " +
                                                 "[TradingVolume] [float] NOT NULL, " +
                                                 "[TradingOppositeVolume] [float] NOT NULL, " +
                                                 "[LastTradePrice] [float] NOT NULL, " +
                                                 "[Timestamp] [datetime] NULL, " +
                                                 "[LastUpdateTimestamp] [datetime] NULL" +
                                                 ");";

        private static Type DataType => typeof(ICandle);
        private static readonly string GetColumns = "[" + string.Join("],[", DataType.GetProperties().Select(x => x.Name)) + "]";
        private static readonly string GetFields = string.Join(",", DataType.GetProperties().Select(x => "@" + x.Name));
        private static readonly string GetUpdateClause = string.Join(",",
            DataType.GetProperties().Select(x => "[" + x.Name + "]=@" + x.Name));

        private readonly string assetName;
        private readonly string TableName;
        private readonly string _connectionString;
        private readonly ILog _log;

        public SqlAssetPairCandlesHistoryRepository(string assetName, string connectionString, ILog log)
        {
            _log = log;
            _connectionString = connectionString;
            TableName = assetName + "_candleshistory";

            using (var conn = new SqlConnection(_connectionString))
            {
                try { conn.CreateTableIfDoesntExists(CreateTableScript, assetName + "_candleshistory"); }
                catch (Exception ex)
                {
                    _log?.WriteErrorAsync(nameof(SqlAssetPairCandlesHistoryRepository), "CreateTableIfDoesntExists", null, ex);
                    throw;
                }
            }
        }
 

        public async Task InsertOrMergeAsync(IEnumerable<ICandle> candles)
        {
                using (var conn = new SqlConnection(_connectionString))
                {
                            if (conn.State == ConnectionState.Closed)
                                await conn.OpenAsync();

                            var transaction = conn.BeginTransaction();
                            try
                            {
                                
                                await conn.ExecuteAsync(
                                    $"insert into {TableName} ({GetColumns}) values ({GetFields})",
                                    candles, transaction, commandTimeout: 150);
                                transaction.Commit();
                            }
                            catch (Exception ex)
                            {
                                transaction.Rollback();
                                _log?.WriteErrorAsync(nameof(SqlCandlesHistoryRepository), nameof(InsertOrMergeAsync),
                                    $"Failed to insert an candle list", ex);
                             }
                }
        }

        public async Task<bool> TryInsertAsync(ICandle candle)
        {
            using (var conn = new SqlConnection(_connectionString))
            {

                try
                {
                    await conn.ExecuteAsync(
                        $"insert into {TableName} ({GetColumns}) values ({GetFields})",
                        candle);
                }
                catch (Exception ex)
                {
                    _log?.WriteErrorAsync(nameof(SqlCandlesHistoryRepository), nameof(InsertOrMergeAsync),
                        $"Failed to insert an candle list", ex);
                    return false;
                }
            }

            return true;
        }

        public async Task<IEnumerable<ICandle>> GetCandlesAsync(CandlePriceType priceType, CandleTimeInterval interval, DateTime from, DateTime to)
        {

            var whereClause =
                "WHERE PriceType=@priceTypeVar AND TimeInterval=@intervalVar AND CAST(Timestamp as time) > CAST(@fromVar as time) AND CAST(Timestamp as time) < CAST(@toVar as time)";

            using (var conn = new SqlConnection(_connectionString))
            {
                var objects = await conn.QueryAsync<Candle>($"SELECT * FROM {TableName} {whereClause}",
                    new { priceTypeVar = priceType, intervalVar = interval, fromVar = from, toVar = to});

                return objects;
            }

        }

        public async Task<ICandle> TryGetFirstCandleAsync(CandlePriceType priceType, CandleTimeInterval timeInterval)
        {
            using (var conn = new SqlConnection(_connectionString))
            {
                var candle = await conn.QueryAsync<ICandle>(
                    $"SELECT TOP(1) * FROM {TableName} WHERE PriceType=@priceTypeVar ANDTimeInterval=@intervalVar ", 
                                                                    new { priceTypeVar = priceType, intervalVar = timeInterval });
                return candle.FirstOrDefault();
            }
        }


        public async Task<int> DeleteCandlesAsync(IReadOnlyList<ICandle> candlesToDelete, CandlePriceType priceType)
        {
            int count = 0;

            using (var conn = new SqlConnection(_connectionString))
            {
                if (conn.State == ConnectionState.Closed)
                    await conn.OpenAsync();
                var transaction = conn.BeginTransaction();
                try
                {
                    foreach (var candle in candlesToDelete)
                    {

                        count += await conn.ExecuteAsync(
                            $"DELETE {TableName} WHERE AssetPairId=@AssetPairIdVar AND Close=@CloseVar AND" +
                            $" TimeInterval = @TimeIntervalVar AND Timestamp = @TimestampVar AND", new
                            {
                                AssetPairIdVar = candle.AssetPairId,
                                TimeIntervalVar = candle.TimeInterval,
                                TimestampVar = candle.Timestamp
                            }, transaction);
                    }
                    transaction.Commit();
                }
                catch (Exception ex)
                {
                    transaction.Rollback();
                    _log?.WriteWarningAsync(nameof(SqlAssetPairCandlesHistoryRepository), nameof(DeleteCandlesAsync),
                        $"Failed to delete an asset pairs", ex);

                }

               
            }

            return count;
        }

        public async Task<int> ReplaceCandlesAsync(IEnumerable<ICandle> candlesToReplace, CandlePriceType priceType)
        {
            int count = 0;

            using (var conn = new SqlConnection(_connectionString))
            {
                if (conn.State == ConnectionState.Closed)
                    await conn.OpenAsync();
                var transaction = conn.BeginTransaction();
                try
                {
                    foreach (var candle in candlesToReplace)
                {
                  
                        count += await conn.ExecuteAsync(
                            $"UPDATE {TableName} SET AssetPairId=@AssetPairIdVar AND Close=@CloseVar AND" +
                            $" High=@HighVar AND LastTradePrice=@LTVar AND TradingVolume = @TVVar AND Low = @LowVar" +
                            $"Open = @OpenVar AND TimeInterval = @TimeIntervalVar AND LastUpdateTimestamp = @TimestampVar AND" +
                            $"  WHERE PriceType=@PTVar AND Timestamp = @TimestampVar", new
                            {
                                AssetPairIdVar = candle.AssetPairId,
                                CloseVar = candle.Close,
                                HighVar = candle.High,
                                LTVar = candle.LastTradePrice,
                                TVVar = candle.TradingVolume,
                                LowVar = candle.Low,
                                OpenVar = candle.Open,
                                TimeIntervalVar = candle.TimeInterval,
                                TimestampVar = candle.Timestamp,
                                PTVar = candle.PriceType
                            });
                  

                }
                    transaction.Commit();
                }
                catch (Exception ex)
                {
                    transaction.Rollback();
                    _log?.WriteErrorAsync(nameof(SqlAssetPairCandlesHistoryRepository), nameof(ReplaceCandlesAsync),
                        $"Failed to repalce an asset pair", ex);

                }

            }

            return count;
        }


    }
}
