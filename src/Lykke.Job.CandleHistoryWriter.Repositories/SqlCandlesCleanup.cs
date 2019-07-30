// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

using System;
using System.Data.SqlClient;
using System.Threading.Tasks;
using Common.Log;
using Dapper;
using Lykke.Job.CandlesHistoryWriter.Core.Domain;
using Lykke.Job.CandlesHistoryWriter.Core.Settings;

namespace Lykke.Job.CandleHistoryWriter.Repositories
{
    public class SqlCandlesCleanup : ICandlesCleanup
    {
        private readonly CleanupSettings _cleanupSettings;
        private readonly string _connectionString;
        private readonly ILog _log;

        private const string CleanupJobName = "Candles.CleanupJob";

        public SqlCandlesCleanup(CleanupSettings cleanupSettings, string connectionString, ILog log)
        {
            _cleanupSettings = cleanupSettings;
            _connectionString = connectionString;
            _log = log;
        }

        public async Task Invoke()
        {
            if (!_cleanupSettings.Enabled)
            {
                await _log.WriteInfoAsync(nameof(ICandlesCleanup), nameof(Invoke),
                    "Cleanup is disabled in settings, skipping.");
                return;
            }
            
            using (var conn = new SqlConnection(_connectionString))
            {
                try
                {
                    //todo ensure that the process is over

                    var procedureBody = "01_Candles.SP_Cleanup.sql".GetFileContent();
                    await conn.ExecuteAsync(string.Format(procedureBody, _cleanupSettings.GetFormatParams()));
                    await conn.ExecuteAsync("02_Candles.CleanupJob.sql".GetFileContent());

                    await conn.ExecuteAsync($"EXEC {CleanupJobName}");

                    await _log.WriteInfoAsync(nameof(ICandlesCleanup), nameof(Invoke), "Candles cleanup started.");
                }
                catch (Exception ex)
                {
                    await _log.WriteErrorAsync(nameof(SqlCandlesCleanup), "Initialization", null, ex);
                    throw;
                }
            }
        }
    }
}
