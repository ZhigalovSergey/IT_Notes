use msdb;
go

-- Drop functions
if object_id(N'dbo.regex_match') is not null
	drop function dbo.regex_match;
go

if object_id(N'dbo.regex_is_match') is not null
	drop function dbo.regex_is_match;
go

if object_id(N'dbo.regex_matches') is not null
	drop function dbo.regex_matches;
go

if object_id(N'dbo.regex_group_match') is not null
	drop function dbo.regex_group_match;
go

if object_id(N'dbo.regex_split') is not null
	drop function dbo.regex_split;
go

if object_id(N'dbo.regex_replace') is not null
	drop function dbo.regex_replace;
go

-- Drop assembly
if (select 1 from sys.assemblies where [name] = N'RegexAssembly')
	is not null
	drop assembly RegexAssembly
go

-- Create the assembly
declare @AssemblyLocation nvarchar(4000);

set @AssemblyLocation = N'C:\Integration\lib\SqlRegex.dll'

create assembly RegexAssembly
from @AssemblyLocation
with permission_set = safe;
go

--  Create TVF
create function dbo.regex_match
(
	@input nvarchar(max),
	@pattern nvarchar(max)
)
returns nvarchar(max)
as external name [RegexAssembly].[UDF].[Match]
go

create function dbo.regex_is_match
(
	@input nvarchar(max),
	@pattern nvarchar(max)
)
returns bit
as external name [RegexAssembly].[UDF].[IsMatch]
go

create function dbo.regex_matches
(
	@input nvarchar(max),
	@pattern nvarchar(max)
)
returns table (
	Position int,
	[Match] nvarchar(max)
) as external name [RegexAssembly].[UDF].[Matches]
go

create function dbo.regex_group_match
(
	@input nvarchar(max),
	@pattern nvarchar(max),
	@group nvarchar(max)
)
returns nvarchar(max)
as external name [RegexAssembly].[UDF].[GroupMatch]
go

create function dbo.regex_split
(
	@input nvarchar(max),
	@pattern nvarchar(max)
)
returns table (
	Position int,
	[Match] nvarchar(max)
) as external name [RegexAssembly].[UDF].[Split]
go

create function dbo.regex_replace
(
	@input nvarchar(max),
	@pattern nvarchar(max),
	@replacement nvarchar(max)
)
returns nvarchar(max)
as external name [RegexAssembly].[UDF].[Replace]
go