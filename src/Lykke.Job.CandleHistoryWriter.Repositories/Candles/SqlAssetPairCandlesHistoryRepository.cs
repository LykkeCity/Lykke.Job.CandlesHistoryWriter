using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Common.Log;
using Dapper;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using Lykke.Job.CandlesProducer.Contract;
using Lykke.Service.Assets.Client.Models;
using Microsoft.Data.OData.Query.SemanticAst;
using Microsoft.Extensions.Internal;
using MoreLinq;
using Newtonsoft.Json;
using Lykke.Logs.MsSql.Extensions;

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
                                                 ",INDEX IX_{0} UNIQUE NONCLUSTERED (Timestamp, PriceType, TimeInterval));"

        private static Type DataType => typeof(ICandle);
        private static readonly string GetColumns = "[" + string.Join("],[", DataType.GetProperties().Select(x => x.Name)) + "]";
        private static readonly string GetFields = string.Join(",", DataType.GetProperties().Select(x => "@" + x.Name));
        private static readonly string GetUpdateClause = string.Join(",",
            DataType.GetProperties().Select(x => "[" + x.Name + "]=@" + x.Name));

        private readonly string assetName;
        private readonly string TableName;
        private readonly string _connectionString;
        private readonly ILog _log;
        private readonly ISystemClock _systemClock;

        public SqlAssetPairCandlesHistoryRepository(string assetName, string connectionString, ILog log)
        {
            _systemClock = new SystemClock();
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
                                var timestamp = _systemClock.UtcNow.UtcDateTime;
                                string sql = $"IF EXISTS (SELECT * FROM {TableName}" +
                                             $" WHERE PriceTpe=@PriceType AND TimeStamp=@TimeStamp AND TimeInterval=@TimeInterval)" +
                                             $" UPDATE {TableName}  SET Open=@Open, Close=@Close, High=@High, Low=@Low, TradingVolume=@TradingVolume, TradingOppositeVolume=@TradingOppositeVolume, LastTradePrice=@LastTradePrice, LastUpdateTimestamp={timestamp}" +
                                             $" WHERE  PriceTpe=@PriceType AND TimeStamp=@TimeStamp AND TimeInterval=@TimeInterval" +
                                             " ELSE " +
                                             $" INSERT INTO {TableName} ({GetColumns}) values ({GetFields})";
                                
                                await conn.ExecuteAsync(
                                    sql,
                                    candles, transaction, commandTimeout: 150);
                                transaction.Commit();
                            }
                            catch (Exception ex)
                            {
                                transaction.Rollback();
                            }
                }
        }

        public async Task<IEnumerable<ICandle>> GetCandlesAsync(CandlePriceType priceType, CandleTimeInterval interval, DateTime from, DateTime to)
        {

            var whereClause =
                "WHERE PriceType=@priceTypeVar AND TimeInterval=@intervalVar AND CAST(Timestamp as time) >= CAST(@fromVar as time) AND CAST(Timestamp as time) <= CAST(@toVar as time)";

            using (var conn = new SqlConnection(_connectionString))
            {
                try
                {
                    var objects = await conn.QueryAsync<Candle>($"SELECT * FROM {TableName} {whereClause}",
                        new { priceTypeVar = priceType, intervalVar = interval, fromVar = from, toVar = to }, null, commandTimeout: 600);
                    return objects;
                }

                catch 
                {
                    return Enumerable.Empty<ICandle>();
                }
            }

        }

        public async Task<ICandle> TryGetFirstCandleAsync(CandlePriceType priceType, CandleTimeInterval timeInterval)
        {
            using (var conn = new SqlConnection(_connectionString))
            {
                var candle = await conn.QueryAsync<ICandle>(
                    $"SELECT TOP(1) * FROM {TableName} WHERE PriceType=@priceTypeVar AND TimeInterval=@intervalVar ", 
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
                        count += await conn.ExecuteAsync(
                            $"DELETE {TableName} WHERE TimeInterval=@TimeInterval AND" +
                            $" Timestamp=@Timestamp AND PriceType=@PriceType", candlesToDelete, transaction);
                   
                    transaction.Commit();
                }
                catch 
                {
                    transaction.Rollback();
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
                    var timestamp = _systemClock.UtcNow.UtcDateTime;
                    count += await conn.ExecuteAsync(
                            $"UPDATE {TableName} SET  Close=@Close, High=@High, LastTradePrice=@LastTradePrice," +
                            $" TradingVolume = @TradingVolume, Low = @Low, Open = @Open, LastUpdateTimestamp = {timestamp}" +
                            $" WHERE TimeInterval = @TimeInterval AND PriceType=@PriceType AND Timestamp = @Timestamp", candlesToReplace, transaction);
                  
                    transaction.Commit();
                }
                catch
                {
                    transaction.Rollback();
                }

            }

            return count;
        }


    }
}
