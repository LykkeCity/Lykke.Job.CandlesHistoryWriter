
IF NOT EXISTS(SELECT job_id FROM msdb.dbo.sysjobs WHERE (name = N'Candles Cleanup Job'))
    BEGIN
        EXEC msdb.dbo.sp_add_job
             @job_name = N'Candles Cleanup Job' ;

        EXEC msdb.dbo.sp_add_jobstep
             @job_name = N'Candles Cleanup Job',
             @step_name = N'Candles Cleanup',
             @subsystem = N'TSQL',
             @command = N'EXEC Candles.SP_Cleanup',
             @retry_attempts = 0,
             @retry_interval = 0 ;

        EXEC msdb.dbo.sp_add_jobserver
             @job_name = N'Candles Cleanup Job';
    END