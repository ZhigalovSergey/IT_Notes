CREATE TABLE [maintenance_idx].[index_maintenance_setting] (
    [dbase_name]              [sysname] NOT NULL,
    [sch_name]                [sysname] NOT NULL,
    [table_name]              [sysname] NOT NULL,
    [priority_lvl]            TINYINT   NOT NULL,
    [fragmentation_threshold] TINYINT   NULL,
    [time_limit]              SMALLINT  NULL,
    [lock_timeout]            INT       NULL,
    CONSTRAINT [PK_maintenance_index_maintenance_setting] PRIMARY KEY CLUSTERED ([sch_name] ASC, [table_name] ASC) 
);

