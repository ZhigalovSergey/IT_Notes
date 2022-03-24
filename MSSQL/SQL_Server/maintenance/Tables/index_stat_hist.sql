CREATE TABLE [maintenance_idx].[index_stat_hist] (
    [dbase_name]       [sysname]       NOT NULL,
    [sch_name]         [sysname]       NOT NULL,
    [table_name]       [sysname]       NOT NULL,
    [index_name]       [sysname]       NOT NULL,
    [partition_number] INT             NOT NULL,
    [index_type]       [sysname]       NOT NULL,
    [size_MB]          NUMERIC (19, 3) NOT NULL,
    [row_count]        BIGINT          NOT NULL,
    [fragmentation]    TINYINT         NULL,
    [mt_insert_dt]     DATETIME2 (0)   NOT NULL,
    [mt_update_dt]     DATETIME2 (0)   NOT NULL,
    [start_utc_dt]     DATETIME2 (7)   NOT NULL,
    [finish_utc_dt]    DATETIME2 (7)   NOT NULL
);


GO
CREATE CLUSTERED COLUMNSTORE INDEX [ccix]
    ON [maintenance_idx].[index_stat_hist];

