with today_dag as (
	SELECT 
		 dag_id
		,next_dagrun_data_interval_start
		,next_dagrun_data_interval_end
	FROM dag 
	where is_paused = false
	and is_active = true
	and schedule_interval not in('null','"Dataset"')
	and (date(next_dagrun_data_interval_start) between current_date -1 and current_date		-- 어제나 오늘 수행된 대상
	or date(next_dagrun_data_interval_end) between current_date -1 and current_date)		-- 어제나 오늘이 배치일인 대상
)
, today_dagrun as (
	select 
		 dag_id
		,count(1) as run_cnt
		,count(case when state = 'success' then 'success' end) as success_cnt
		,count(case when state = 'failed' then 'failed' end) as failed_cnt
		,count(case when state = 'running' then 'running' end) as running_cnt
		,max(case when state = 'failed' then data_interval_end end) as last_failed_date
		,max(case when state = 'success' then data_interval_end end) as last_success_date
	from dag_run 
	where date(data_interval_end) between current_date -1 and current_date
	group by dag_id
)
select 
	 d.dag_id
	,coalesce(run_cnt, 0)
	,coalesce(r.success_cnt, 0)
	,coalesce(r.failed_cnt, 0)
	,coalesce(r.running_cnt, 0)
    ,r.last_failed_date
	,r.last_success_date
	,d.next_dagrun_data_interval_start
	,d.next_dagrun_data_interval_end
from today_dag d
left join today_dagrun  r
	on d.dag_id = r.dag_id