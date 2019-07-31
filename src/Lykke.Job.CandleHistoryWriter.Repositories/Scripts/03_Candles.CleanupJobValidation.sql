SELECT
    H.*
FROM
    msdb.dbo.sysjobs AS J
        CROSS APPLY (
        SELECT TOP 1
            JobName = J.name,
            StepNumber = T.step_id,
            StepName = T.step_name,
            StepStatus = CASE T.run_status
                             WHEN 0 THEN 'Failed'
                             WHEN 1 THEN 'Succeeded'
                             WHEN 2 THEN 'Retry'
                             WHEN 3 THEN 'Canceled'
                             ELSE 'Running' END,
            ExecutedAt = msdb.dbo.agent_datetime(T.run_date, T.run_time),
            ExecutingHours = ((T.run_duration/10000 * 3600 + (T.run_duration/100) % 100 * 60 + T.run_duration % 100 + 31 ) / 60) / 60,
            ExecutingMinutes = ((T.run_duration/10000 * 3600 + (T.run_duration/100) % 100 * 60 + T.run_duration % 100 + 31 ) / 60) % 60,
            Message = T.message
        FROM
            msdb.dbo.sysjobhistory AS T
        WHERE
                T.job_id = J.job_id
        ORDER BY
            T.instance_id DESC) AS H
ORDER BY
    J.name