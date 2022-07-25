```sql
use [sandbox]
go

create procedure [dbo].[prcGetTableCreateScript] (
	@tableName sysname
	,@base sysname = ''
	,@bracket nvarchar(2) = ''
	,@flags nvarchar(6) = '000000'
	,@viewCode nvarchar(MAX) = null output
	,@file_group sysname = null
	,@tableName_suffix sysname = null --добвляем к имени суффикс
	)
as
begin
	set nocount on

	declare @objectName sysname
		,@tempName sysname
		,@objectId int
		,@tab nvarchar(10) = SPACE(4)
		,@bracketStart nvarchar(1)
		,@bracketEnd nvarchar(1)
		,@isDrop nvarchar(1)
		,@isIndex nvarchar(1)
		,@isInsert nvarchar(1)
		,@isUseDB nvarchar(1)
		,@isPrint nvarchar(1)
		,@isView nvarchar(1)

	select @bracketStart = SUBSTRING(@bracket, 1, 1)

	select @bracketEnd = SUBSTRING(@bracket, 2, 1)

	select @isDrop = IIF(SUBSTRING(@flags, 1, 1) = '', N'0', SUBSTRING(@flags, 1, 1))

	select @isIndex = IIF(SUBSTRING(@flags, 2, 1) = '', N'0', SUBSTRING(@flags, 2, 1))

	select @isInsert = IIF(SUBSTRING(@flags, 3, 1) = '', N'0', SUBSTRING(@flags, 3, 1))

	select @isUseDB = IIF(SUBSTRING(@flags, 4, 1) = '', N'0', SUBSTRING(@flags, 4, 1))

	select @isPrint = IIF(SUBSTRING(@flags, 5, 1) = '', N'0', SUBSTRING(@flags, 5, 1))

	select @isView = IIF(SUBSTRING(@flags, 6, 1) = '', N'0', SUBSTRING(@flags, 6, 1))

	set @base = IIF(@base = '', db_name(), @base)
	set @tempName = IIF(@base = 'tempdb', concat (
				@base
				,'.dbo.'
				,@tableName
				), concat (
				@base
				,'.'
				,@tableName
				))

	declare @SQLEXEC nvarchar(MAX) = ''
	declare @SQLEXEC2 nvarchar(MAX) = ''
	declare @SQLEXEC3 nvarchar(MAX) = ''
	declare @SQLEXEC4 nvarchar(MAX) = ''
	declare @SQLEXEC4_2 nvarchar(MAX) = ''
	declare @SQLEXEC5 nvarchar(MAX) = ''

	if exists (
			select *
			from tempdb.sys.objects
			where OBJECT_ID('tempdb..#object') = OBJECT_ID
			)
		drop table #object

	create table #object (
		objectName sysname
		,objectId int
		)

	set @SQLEXEC = 'insert into #object
    select 
          IIF(''' + @base + ''' != ''tempdb'', ''' + @bracketStart + ''' + s.name + ''' + @bracketEnd + '''+ ''.'','''') + ''' + @bracketStart + ''' + SUBSTRING(o.NAME, 1, IIF(CHARINDEX(''___'', o.NAME)>0, CHARINDEX(''___'', o.NAME) - 1, LEN(o.name))) + ''' + isnull(@tableName_suffix, '') + @bracketEnd + ''' AS name
        , o.[object_id]
    FROM ' + @base + '.sys.objects o WITH (NOWAIT)
    JOIN ' + @base + '.sys.schemas s WITH (NOWAIT) ON o.[schema_id] = s.[schema_id]
    WHERE 1=1
        AND o.object_id = OBJECT_ID(''' + @tempName + ''')
        AND o.[type] IN(''U'',''V'')
        AND o.is_ms_shipped = 0
    '

	exec (@SQLEXEC)

	if (@isPrint = N'1')
	begin
		print @SQLEXEC
	end

	select @objectName = objectName
		,@objectId = objectId
	from #object

	if @objectId is null
	begin
		declare @err varchar(1000) = 'Объект ' + @base + '.' + @tableName + ' не найден.';

		THROW 777777
			,@err
			,1
	end

	/*IIF(' + @isDrop + ' = 1, ''IF	EXISTS (select * FROM ' + @base + '.sys.objects WHERE OBJECT_ID(''+''''''' + @tempName + '''''''+'') = OBJECT_ID)  '' + CHAR(13) + ''' + @tab + '''+''DROP TABLE '' + '''+ @objectName +''' + CHAR(13), '''') +*/
	set @SQLEXEC = '
    DECLARE @SQL NVARCHAR(MAX) = ''''
    ;WITH index_column AS 
    (
        select 
              ic.[object_id]
            , ic.index_id
            , ic.is_descending_key
            , ic.is_included_column
            , c.name
        FROM ' + @base + '.sys.index_columns ic WITH (NOWAIT)
        JOIN ' + @base + '.sys.columns c WITH (NOWAIT) ON ic.[object_id] = c.[object_id] AND ic.column_id = c.column_id
        WHERE ic.[object_id] = ' + CAST(@objectId as nvarchar(15)) + '
    ),
    fk_columns AS 
    (
         select 
              k.constraint_object_id
            , cname = c.name
            , rcname = rc.name
        FROM ' + @base + '.sys.foreign_key_columns k WITH (NOWAIT)
        JOIN ' + @base + '.sys.columns rc WITH (NOWAIT) ON rc.[object_id] = k.referenced_object_id AND rc.column_id = k.referenced_column_id 
        JOIN ' + @base + 
		'.sys.columns c WITH (NOWAIT) ON c.[object_id] = k.parent_object_id AND c.column_id = k.parent_column_id
        WHERE k.parent_object_id = ' + CAST(@objectId as nvarchar(15)) + 
		'
    ),maxLen AS 
    (
        select 
             column_id
            ,maxLenColumn
            ,MAX(LEN(columnType)) OVER() AS maxColumnType
            ,columnType
        FROM  (
            select 
                c.column_id
               ,MAX(LEN(c.name))  OVER() AS maxLenColumn
               ,UPPER(tp.name) + 
                        CASE WHEN tp.name IN (''varchar'', ''char'', ''varbinary'', ''binary'', ''text'')
                               THEN ''('' + CASE WHEN c.max_length = -1 THEN ''MAX'' ELSE CAST(c.max_length AS VARCHAR(5)) END + '')''
                             WHEN tp.name IN (''nvarchar'', ''nchar'', ''ntext'')
                               THEN ''('' + CASE WHEN c.max_length = -1 THEN ''MAX'' ELSE CAST(c.max_length / 2 AS VARCHAR(5)) END + '')''
                             WHEN tp.name IN (''datetime2'', ''time2'', ''datetimeoffset'') 
                               THEN ''('' + CAST(c.scale AS VARCHAR(5)) + '')''
                             WHEN tp.name in ( ''decimal'' , ''numeric'')
                               THEN ''('' + CAST(c.[precision] AS VARCHAR(5)) + '','' + CAST(c.scale AS VARCHAR(5)) + '')''
                            ELSE ''''
                        END AS columnType
            FROM ' 
		+ @base + '.sys.columns c
            JOIN ' + @base + '.sys.types tp WITH (NOWAIT) ON c.user_type_id = tp.user_type_id
            WHERE object_id = ' + CAST(@objectId as nvarchar(15)) + '
        ) c
    )'
	set @SQLEXEC2 = '
    select @SQL = 
        IIF(' + @isUseDB + ' = 1, ''USE '' + ''' + @base + ''' + CHAR(13) + ''GO'' + CHAR(13), '''') + 
        IIF(' + @isDrop + ' = 1, ''drop table if exists ' + @tableName + ''', '''') +  CHAR(13) +
        + ''create table '' + ''' + @objectName + ''' + ''('' + CHAR(13) + STUFF((
        select ''' + @tab + ''' + '','' + ''' + @bracketStart + '''+ c.name + ''' + @bracketEnd + 
		''' + SPACE(mlc.maxLenColumn - LEN(c.name)+1) +
            CASE WHEN c.is_computed = 1
                THEN ''AS '' + cc.[definition] 
                ELSE columnType +
                    /*CASE WHEN c.collation_name IS NOT NULL THEN '' COLLATE '' + c.collation_name ELSE '''' END +*/
                    SPACE(mlc.maxColumnType - LEN(mlc.columnType)+1) + CASE WHEN c.is_nullable = 1 THEN ''NULL'' ELSE ''NOT NULL'' END +
                    CASE WHEN dc.[definition] IS NOT NULL THEN '' DEFAULT'' + dc.[definition] ELSE '''' END + 
                    CASE WHEN ic.is_identity = 1 THEN '' IDENTITY('' + CAST(ISNULL(ic.seed_value, ''0'') AS CHAR(1)) + '','' + CAST(ISNULL(ic.increment_value, ''1'') AS CHAR(1)) + '')'' ELSE '''' END 
            END + CHAR(13)
        FROM ' + @base + '.sys.columns c WITH (NOWAIT)
        JOIN ' + @base + '.sys.types tp WITH (NOWAIT) 
            ON c.user_type_id = tp.user_type_id
        LEFT JOIN ' + @base + 
		'.sys.computed_columns cc WITH (NOWAIT) 
            ON c.[object_id] = cc.[object_id] AND c.column_id = cc.column_id
        LEFT JOIN ' + @base + '.sys.default_constraints dc WITH (NOWAIT) 
            ON c.default_object_id != 0 
            AND c.[object_id] = dc.parent_object_id 
            AND c.column_id = dc.parent_column_id
        LEFT JOIN ' + @base + '.sys.identity_columns ic WITH (NOWAIT) 
            ON c.is_identity = 1 
            AND c.[object_id] = ic.[object_id] 
            AND c.column_id = ic.column_id
        JOIN maxLen mlc ON c.column_id = mlc.column_id        
        WHERE c.[object_id] =  ' + CAST(@objectId as nvarchar(15)) + '
        ORDER BY c.column_id
        FOR XML PATH(''''), TYPE).value(''.'', ''NVARCHAR(MAX)''), 1, LEN(REPLACE(''' + @tab + ''','' '', ''*''))+1, ''' + @tab + ''' + '' '')        
        + ISNULL((select ''' + @tab + ''' + '',CONSTRAINT '' + ''' + @bracketStart + '''+ k.name +  ''' + isnull(@tableName_suffix, '') + @bracketEnd + 
		'''  + '' PRIMARY KEY ''  +
		                             CASE WHEN  i.type = 1 THEN +'' CLUSTERED ('' ELSE +'' NONCLUSTERED ('' END +
                        (select STUFF((
                             select '', '' + ''' + @bracketStart + '''+ c.name + ''' + @bracketEnd + ''' + '' '' + CASE WHEN ic.is_descending_key = 1 THEN ''DESC'' ELSE ''ASC'' END
                             FROM ' + @base + '.sys.index_columns ic WITH (NOWAIT)
                             JOIN ' + @base + '.sys.columns c WITH (NOWAIT) ON c.[object_id] = ic.[object_id] AND c.column_id = ic.column_id
                             WHERE ic.is_included_column = 0
                                 AND ic.[object_id] = k.parent_object_id 
                                 AND ic.index_id = k.unique_index_id     
                             FOR XML PATH(N''''), TYPE).value(''.'', ''NVARCHAR(MAX)''), 1, 2, ''''))
                + '')'' + isnull('' ON ''+ nullif(''' + isnull(@file_group, '') + 
		''',''''),'''')  + CHAR(13) 
                FROM ' + @base + '.sys.key_constraints k WITH (NOWAIT)
				left join ' + @base + '.sys.indexes i on i.object_id=k.parent_object_id and i.is_primary_key=1
                WHERE k.parent_object_id = ' + CAST(@objectId as nvarchar(15)) + ' 
                    AND k.[type] = ''PK''), '''') + '')'' + isnull('' ON ''+ nullif(''' + isnull(@file_group, '') + ''',''''),'''')  + CHAR(13) 
        '
	set @SQLEXEC3 = '+ IIF(' + @isIndex + ' = 1, 
        ISNULL((select (
            select CHAR(13) +
                 ''ALTER TABLE '' + ''' + @objectName + ''' + CHAR(13) + ''' + @tab + ''' + '' WITH'' 
                + CASE WHEN fk.is_not_trusted = 1 
                    THEN '' NOCHECK'' 
                    ELSE '' CHECK'' 
                  END + 
                  '' ADD CONSTRAINT '' + ''' + @bracketStart + ''' + fk.name  + ''' + @bracketEnd + ''' + CHAR(13) + ''' + @tab + ''' + '' FOREIGN KEY('' 
                  + STUFF((
                    select '','' + ''' + @bracketStart + ''' + k.cname + ''' + @bracketEnd + '''
                    FROM fk_columns k
                    WHERE k.constraint_object_id = fk.[object_id]
                    FOR XML PATH(''''), TYPE).value(''.'', ''NVARCHAR(MAX)''), 1, 2, '''')
                   + '')'' +
                  '' REFERENCES '' + ''' + @bracketStart + ''' + SCHEMA_NAME(ro.[schema_id]) + ''' + @bracketEnd + ''' + ''.'' + ''' + @bracketStart + 
		''' + ro.name + ''' + @bracketEnd + ''' + '' (''
                  + STUFF((
                    select '','' + ''' + @bracketStart + ''' + k.rcname + ''' + @bracketEnd + '''
                    FROM fk_columns k
                    WHERE k.constraint_object_id = fk.[object_id]
                    FOR XML PATH(''''), TYPE).value(''.'', ''NVARCHAR(MAX)''), 1, 2, '''')
                   + '')''
                + CASE 
                    WHEN fk.delete_referential_action = 1 THEN CHAR(13) + ''' + @tab + ''' + ''' + @tab + ''' +'' ON DELETE CASCADE'' 
                    WHEN fk.delete_referential_action = 2 THEN CHAR(13) + ''' + @tab + ''' + ''' + @tab + ''' +'' ON DELETE SET NULL''
                    WHEN fk.delete_referential_action = 3 THEN CHAR(13) + ''' + @tab + ''' + ''' + @tab + ''' +'' ON DELETE SET DEFAULT'' 
                    ELSE '''' 
                  END
                + CASE 
                    WHEN fk.update_referential_action = 1 THEN CHAR(13) + ''' + @tab + ''' + ''' + @tab + 
		''' +'' ON UPDATE CASCADE''
                    WHEN fk.update_referential_action = 2 THEN CHAR(13) + ''' + @tab + ''' + ''' + @tab + ''' +'' ON UPDATE SET NULL''
                    WHEN fk.update_referential_action = 3 THEN CHAR(13) + ''' + @tab + ''' + ''' + @tab + ''' +'' ON UPDATE SET DEFAULT''  
                    ELSE '''' 
                  END 
                + CHAR(13) +  CHAR(13) + ''ALTER TABLE '' + ''' + @objectName + ''' + CHAR(13) + ''' + @tab + ''' + '' CHECK CONSTRAINT '' + ''' + @bracketStart + ''' + fk.name  + ''' + @bracketEnd + ''' + CHAR(13)
            FROM ' + @base + '.sys.foreign_keys fk WITH (NOWAIT)
            JOIN ' + @base + '.sys.objects ro WITH (NOWAIT) ON ro.[object_id] = fk.referenced_object_id
            WHERE fk.parent_object_id = ' + CAST(@objectId as nvarchar(15)) + '
            FOR XML PATH(N''''), TYPE).value(''.'', ''NVARCHAR(MAX)'')), '''')
            ,'''')
        '
	set @SQLEXEC4 = '+ IIF(' + @isIndex + ' = 1, ISNULL((
            (
             select
                CHAR(13) + ''CREATE'' + CASE WHEN i.is_unique = 1 THEN '' UNIQUE'' ELSE '''' END 
                + '' NONCLUSTERED INDEX '' + ''' + @bracketStart + ''' + i.name + ''' + @bracketEnd + ''' +'' ON '' + ''' + @objectName + ''' + CHAR(13) + ''('' + CHAR(13) + ''' + @tab + ''' + '' '' +
                STUFF((
                select CHAR(13) + ''' + @tab + ''' + '','' + ''' + @bracketStart + ''' + c.name + ''' + @bracketEnd + ''' + SPACE(maxLenIdxColumn - lenIdxColumn+1) + CASE WHEN c.is_descending_key = 1 THEN ''DESC'' ELSE ''ASC'' END
                FROM (
                    select 
                         c.name
                        ,c.is_descending_key
                        ,MAX(LEN( ''' + @bracketStart + ''' + c.name + ''' + @bracketEnd + ''')) OVER() AS maxLenIdxColumn
                        ,LEN( ''' + @bracketStart + ''' + c.name + ''' + @bracketEnd + 
		''') AS lenIdxColumn
                    FROM index_column c
                    WHERE c.is_included_column = 0
                    AND c.index_id = i.index_id
                ) c
                FOR XML PATH(''''), TYPE).value(''.'', ''NVARCHAR(MAX)''), 1, LEN(REPLACE(''' + @tab + ''','' '', ''*''))+2, '''') + CHAR(13) + '')'' 
                + ISNULL(CHAR(13) + ''INCLUDE ''+ CHAR(13) + ''('' + CHAR(13) + ''' + @tab + ''' + '' '' + 
                    STUFF((
                    select CHAR(13) + ''' + @tab + ''' + '','' + ''' + @bracketStart + ''' + c.name + ''' + @bracketEnd + '''
                    FROM index_column c
                    WHERE c.is_included_column = 1
                        AND c.index_id = i.index_id
                    FOR XML PATH(''''), TYPE).value(''.'', ''NVARCHAR(MAX)''), 1, LEN(REPLACE(''' + @tab + ''','' '', ''*''))+2, '''') + CHAR(13) + '')'', '''')  + CHAR(13)
            FROM ' + @base + '.sys.indexes i WITH (NOWAIT)
            WHERE i.[object_id] = ' + 
		CAST(@objectId as nvarchar(15)) + '
                AND i.is_primary_key = 0
                AND i.[type] = 2
            FOR XML PATH(''''), TYPE).value(''.'', ''NVARCHAR(MAX)'')
        ), '''')
        ,'''')'
	set @SQLEXEC4_2 = '+ IIF(' + @isIndex + ' = 1, ISNULL((
            (
             select
                CHAR(13) + ''CREATE CLUSTERED COLUMNSTORE INDEX ''  + ''' + @bracketStart + ''' + i.name + ''' + @bracketEnd + ''' +'' ON '' + ''' + @objectName + '''  +
				isnull('' ON ''+ nullif(''' + isnull(@file_group, '') + ''',''''),'''')  + CHAR(13)
             FROM ' + @base + '.sys.indexes i WITH (NOWAIT)
            WHERE i.[object_id] = ' + CAST(@objectId as nvarchar(15)) + '
                AND i.is_primary_key = 0
                AND i.[type] = 5
            FOR XML PATH(''''), TYPE).value(''.'', ''NVARCHAR(MAX)'')
        ), '''')
        ,'''')'
	--print @SQLEXEC4_2
	set @SQLEXEC5 = '+IIF(' + @isInsert + ' = 1, ISNULL(CHAR(13) + ''insert into '' + ''' + @objectName + ''' + ''('' + CHAR(13) + STUFF((
        select ''' + @tab + ''' + '','' + ''' + @bracketStart + '''+ c.name + ''' + @bracketEnd + ''' + SPACE(mlc.maxLenColumn - LEN(c.name)+1) +
            + CHAR(13)
        FROM ' + @base + '.sys.columns c WITH (NOWAIT)
        JOIN ' + @base + '.sys.types tp WITH (NOWAIT) 
            ON c.user_type_id = tp.user_type_id
        LEFT JOIN ' + @base + '.sys.computed_columns cc WITH (NOWAIT) 
            ON c.[object_id] = cc.[object_id] AND c.column_id = cc.column_id
        LEFT JOIN ' + @base + '.sys.default_constraints dc WITH (NOWAIT) 
            ON c.default_object_id != 0 
            AND c.[object_id] = dc.parent_object_id 
            AND c.column_id = dc.parent_column_id
        LEFT JOIN ' + @base + 
		'.sys.identity_columns ic WITH (NOWAIT) 
            ON c.is_identity = 1 
            AND c.[object_id] = ic.[object_id] 
            AND c.column_id = ic.column_id
        JOIN maxLen mlc ON c.column_id = mlc.column_id        
        WHERE c.[object_id] =  ' + CAST(@objectId as nvarchar(15)) + '
        ORDER BY c.column_id
        FOR XML PATH(''''), TYPE).value(''.'', ''NVARCHAR(MAX)''), 1, LEN(REPLACE(''' + @tab + ''','' '', ''*''))+1, ''' + @tab + ''' + '' '')   
        + '')'' + CHAR(13) ,'''')
        ,'''')
        PRINT @SQL
        RAISERROR (@SQL, 12,1) WITH SETERROR
        '

	if (@isPrint = N'0')
	begin
		begin try
			exec (@SQLEXEC + @SQLEXEC2 + @SQLEXEC3 + @SQLEXEC4 + @SQLEXEC4_2 + @SQLEXEC5)
		end try

		begin catch
			if @isView = 1
			begin
				set @viewCode = ERROR_MESSAGE()
			end
			else
			begin
				print ''
					/*PRINT ERROR_MESSAGE()*/
			end
		end catch
	end
	else
	begin
		print @SQLEXEC
		print @SQLEXEC2
		print @SQLEXEC3
		print @SQLEXEC4
		print @SQLEXEC4_2
		print @SQLEXEC5
	end

	return
end
go



```

