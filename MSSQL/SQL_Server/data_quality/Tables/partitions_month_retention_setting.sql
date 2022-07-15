CREATE TABLE [maintenance].[partitions_month_retention_setting] (
    [table_full_name]           [sysname] NOT NULL,
    [retention_partition_count] INT       NOT NULL,
    CONSTRAINT [PK_maintenance_partitions_month_retention_setting] PRIMARY KEY CLUSTERED ([table_full_name] ASC) WITH (FILLFACTOR = 95),
    CONSTRAINT [CK_maintenance_partitions_month_retention_setting] CHECK ([retention_partition_count]>(1))
);


GO
EXECUTE sp_addextendedproperty @name = N'MS_description', @value = N'Колво партиций которые оставляются. Отсчет идет от текущей', @level0type = N'SCHEMA', @level0name = N'maintenance', @level1type = N'TABLE', @level1name = N'partitions_month_retention_setting', @level2type = N'COLUMN', @level2name = N'retention_partition_count';


GO
EXECUTE sp_addextendedproperty @name = N'MS_description', @value = N'Полное имя таблицы со схемой без квадратных скобок ', @level0type = N'SCHEMA', @level0name = N'maintenance', @level1type = N'TABLE', @level1name = N'partitions_month_retention_setting', @level2type = N'COLUMN', @level2name = N'table_full_name';


GO
EXECUTE sp_addextendedproperty @name = N'MS_description', @value = N'Настройки очистки партиций', @level0type = N'SCHEMA', @level0name = N'maintenance', @level1type = N'TABLE', @level1name = N'partitions_month_retention_setting';

