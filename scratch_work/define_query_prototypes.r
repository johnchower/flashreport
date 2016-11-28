AU_query_prototype <- 
"
/* Count active users */
select flash_report_category, count (distinct ufc.user_id) 
from user_flash_cat ufc
join session_duration_fact sdf
on sdf.user_id=ufc.user_id
where sdf.date_id>=min_date_xyz
	and sdf.date_id<=max_date_xyz
	and sdf.category='Default'
group by ufc.flash_report_category
order by ufc.flash_report_category
;
"
pa_query_prototype <- 
"
/* Count platform actions */
select pfc.flash_report_category as pa_cat
	, ufc.flash_report_category as user_cat
	, count (distinct upaf.id)
from user_platform_action_facts upaf
left join pa_flash_cat pfc
	on pfc.platform_action=upaf.platform_action
left join user_flash_cat ufc
	on ufc.user_id=upaf.user_id
where upaf.date_id>=min_date_xyz
	and upaf.date_id<=max_date_xyz
group by pfc.flash_report_category, ufc.flash_report_category
order by ufc.flash_report_category, pfc.flash_report_category 
;
"
notification_query_prototype <-
"
/* Count Notifications */
select ufc.flash_report_category as user_cat
	, nef.status
	, count (distinct nef.id)
from notification_event_facts nef
left join user_flash_cat ufc
	on ufc.user_id=nef.user_id
where nef.date_id>=min_date_xyz
	and nef.date_id<=max_date_xyz
group by ufc.flash_report_category, nef.status
order by ufc.flash_report_category, nef.status
;
"
query_prototype_list <- list(
  auPrototype = AU_query_prototype
  , paPrototype = pa_query_prototype
  , notificationsPrototype = notification_query_prototype)

devtools::use_data(query_prototype_list, overwrite = T)
