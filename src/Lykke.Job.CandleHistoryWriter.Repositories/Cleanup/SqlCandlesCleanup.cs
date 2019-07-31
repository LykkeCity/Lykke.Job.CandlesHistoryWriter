// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

using System;
using System.Data.SqlClient;
using System.Threading.Tasks;
using Common;
using Common.Log;
using Dapper;
using Lykke.Job.CandlesHistoryWriter.Core.Domain;
using Lykke.Job.CandlesHistoryWriter.Core.Settings;

namespace Lykke.Job.CandleHistoryWriter.Repositories.Cleanup
{
    public class SqlCandlesCleanup : ICandlesCleanup
    {
        private readonly CleanupSettings _cleanupSettings;
        private readonly string _connectionString;
        private readonly ILog _log;

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
                    var status = await conn.QuerySingleOrDefaultAsync<JobStatus>(
                        "03_Candles.CleanupJobValidation.sql".GetFileContent());

                    if (status?.StepStatus == "Running")
                    {
                        throw new Exception($"Previous cleanup process is still running, it is prohibited to run new process until previous is finished. Statistics: {status.ToJson()}.");
                    }

                    await _log.WriteInfoAsync(nameof(ICandlesCleanup), nameof(Invoke),
                        $"Previous cleanup statistics: {status?.ToJson() ?? "no data"}.");

                    var procedureBody = "01_Candles.SP_Cleanup.sql".GetFileContent();
                    await conn.ExecuteAsync(string.Format(procedureBody, _cleanupSettings.GetFormatParams()));
                    await conn.ExecuteAsync("02_Candles.CleanupJob.sql".GetFileContent());

                    await conn.ExecuteAsync("EXEC msdb.dbo.sp_start_job 'Candles Cleanup Job'");

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
