CREATE TABLE [maintenance_idx].[index_stat] (
    [dbase_name]       [sysname]                                          NOT NULL,
    [sch_name]         [sysname]                                          NOT NULL,
    [table_name]       [sysname]                                          NOT NULL,
    [index_name]       [sysname]                                          NOT NULL,
    [partition_number] INT                                                NOT NULL,
    [index_type]       [sysname]                                          NOT NULL,
    [size_MB]          NUMERIC (19, 3)                                    NOT NULL,
    [row_count]        BIGINT                                             NOT NULL,
    [fragmentation]    TINYINT                                            NULL,
    [mt_insert_dt]     DATETIME2 (0)                                      NOT NULL,
    [mt_update_dt]     DATETIME2 (0)                                      NOT NULL,
    [start_utc_dt]     DATETIME2 (7) GENERATED ALWAYS AS ROW START HIDDEN NOT NULL,
    [finish_utc_dt]    DATETIME2 (7) GENERATED ALWAYS AS ROW END HIDDEN   NOT NULL,
    CONSTRAINT [PK_maintenance_index_stat] PRIMARY KEY CLUSTERED ([dbase_name] ASC, [sch_name] ASC, [table_name] ASC, [index_name] ASC, [partition_number] ASC),
    PERIOD FOR SYSTEM_TIME ([start_utc_dt], [finish_utc_dt])
)
WITH (SYSTEM_VERSIONING = ON (HISTORY_TABLE=[maintenance_idx].[index_stat_hist], DATA_CONSISTENCY_CHECK=ON, HISTORY_RETENTION_PERIOD=3 MONTH));

