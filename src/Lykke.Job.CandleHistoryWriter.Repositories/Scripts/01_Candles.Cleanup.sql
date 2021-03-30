CREATE OR ALTER PROCEDURE Candles.Cleanup
AS
    BEGIN
        DECLARE
            @i INT = 0, @count INT;
        SELECT @count = Count(*)
        FROM INFORMATION_SCHEMA.TABLES c
        WHERE c.TABLE_TYPE = 'BASE TABLE'
          AND TABLE_SCHEMA = 'Candles';

        WHILE @i <= @count
        BEGIN

            BEGIN TRANSACTION;

            BEGIN TRY

                DECLARE
                    @table nvarchar(128);

                SELECT @table = c.TABLE_NAME
                FROM INFORMATION_SCHEMA.TABLES c
                WHERE c.TABLE_TYPE = 'BASE TABLE'
                  AND TABLE_SCHEMA = 'Candles'
                ORDER BY c.TABLE_NAME
                    OFFSET @i ROWS
                    FETCH NEXT 1 ROWS ONLY;

                DECLARE
                    @SQL NVARCHAR(MAX) = ''
                SELECT @SQL = 'DELETE FROM [Candles].[' + @table + '] WHERE Id NOT IN (
   SELECT Id FROM (SELECT TOP {0} Id FROM [Candles].[' + @table + '] WHERE TimeInterval=1 ORDER BY Timestamp DESC) AS TI1 UNION ALL
   SELECT Id FROM (SELECT TOP {1} Id FROM [Candles].[' + @table + '] WHERE TimeInterval=60 ORDER BY Timestamp DESC) AS TI60 UNION ALL
   SELECT Id FROM (SELECT TOP {2} Id FROM [Candles].[' + @table + '] WHERE TimeInterval=300 ORDER BY Timestamp DESC) AS TI300 UNION ALL
   SELECT Id FROM (SELECT TOP {3} Id FROM [Candles].[' + @table + '] WHERE TimeInterval=900 ORDER BY Timestamp DESC) AS TI900 UNION ALL
   SELECT Id FROM (SELECT TOP {4} Id FROM [Candles].[' + @table + '] WHERE TimeInterval=1800 ORDER BY Timestamp DESC) AS TI1800 UNION ALL
   SELECT Id FROM (SELECT TOP {5} Id FROM [Candles].[' + @table + '] WHERE TimeInterval=3600 ORDER BY Timestamp DESC) AS TI3600 UNION ALL
   SELECT Id FROM (SELECT TOP {6} Id FROM [Candles].[' + @table + '] WHERE TimeInterval=7200 ORDER BY Timestamp DESC) AS TI7200 UNION ALL
   SELECT Id FROM (SELECT TOP {7} Id FROM [Candles].[' + @table + '] WHERE TimeInterval=21600 ORDER BY Timestamp DESC) AS TI21600 UNION ALL
   SELECT Id FROM (SELECT TOP {8} Id FROM [Candles].[' + @table + '] WHERE TimeInterval=43200 ORDER BY Timestamp DESC) AS TI43200 UNION ALL
   SELECT Id FROM (SELECT TOP {9} Id FROM [Candles].[' + @table + '] WHERE TimeInterval=86400 ORDER BY Timestamp DESC) AS TI86400 UNION ALL
   SELECT Id FROM (SELECT TOP {10} Id FROM [Candles].[' + @table + '] WHERE TimeInterval=604800 ORDER BY Timestamp DESC) AS TI604800 UNION ALL
   SELECT Id FROM (SELECT TOP {11} Id FROM [Candles].[' + @table + '] WHERE TimeInterval=3000000 ORDER BY Timestamp DESC) AS TI3000000
    );';

                EXEC sp_executesql @SQL

                COMMIT TRANSACTION;
            END TRY
            BEGIN CATCH
                IF @@TRANCOUNT > 0
                    ROLLBACK TRANSACTION;
            END CATCH

            SET @i = @i + 1;
        END
    END