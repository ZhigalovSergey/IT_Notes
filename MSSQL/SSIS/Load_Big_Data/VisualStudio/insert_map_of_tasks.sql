use [MDWH_SNAPSHOT]
go

/*

declare @res nvarchar(128)
exec insert_map_of_tasks 'test', 6, @res output
select @res

*/

-- truncate table tempdb.dbo.map_of_tasks
-- select * from tempdb.dbo.map_of_tasks (nolock)

alter procedure [dbo].[insert_map_of_tasks]
	@ExecutionInstanceGUID nvarchar(128),
	@task_id int,
	@result nvarchar(128) output
as begin
	set nocount on;
	begin tran

	insert into tempdb.dbo.map_of_tasks (
		ExecutionInstanceGUID,
		task_id,
		insert_dt,
		status
	)
	values (
		@ExecutionInstanceGUID,
		@task_id,
		getdate(),
		'running'
	)

	if (
			select
				ExecutionInstanceGUID
			from
				(
					select
						ExecutionInstanceGUID,
						row_number() over (partition by task_id order by id) rn
					from
						tempdb.dbo.map_of_tasks(nolock)
					where
						task_id = @task_id
				) t
			where
				rn = 1
		)
		<> @ExecutionInstanceGUID
	begin
		rollback
		set @result = 'failed'
	end
	else
	begin
		commit
		set @result = 'success'
	end

end
	go


