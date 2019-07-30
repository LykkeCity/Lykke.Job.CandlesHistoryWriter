
EXEC Candles.sp_add_job
     @job_name = N'Candles Cleanup Job' ;


EXEC sp_add_jobstep
     @job_name = N'Candles Cleanup Job',
     @step_name = N'Candles Cleanup',
     @subsystem = N'TSQL',
     @command = N'EXEC Candles.SP_Cleanup',
     @retry_attempts = 0,
     @retry_interval = 0 ;

EXEC Candles.sp_add_jobserver
     @job_name = N'Candles Cleanup Job'; 