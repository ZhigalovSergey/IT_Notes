
create table [core].[utm_extended_source_ref] (
	[utm_extended_source_key] [tinyint] not null,
	[utm_extended_source_name] [nvarchar](255) not null,
	constraint [pk_utm_extended_source_ref] primary key clustered
	(
	[utm_extended_source_key] asc
	) on [core]
) on [core]
go

--insert into [core].[utm_extended_source_ref]
--([utm_extended_source_key], [utm_extended_source_name]) values
--(1,'MDWH_RAW.raw_gbq168810_g161014_dwh_output.dimUtmExtended')