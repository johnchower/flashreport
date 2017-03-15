/* CREATE TEMPORARY TABLE user_flash_cat AS */
WITH uc_seq AS (
SELECT user_id
     , MAX(sequence_number) AS max_sequence
     , MIN(sequence_number) AS min_sequence
FROM user_connected_to_champion_bridges
GROUP BY user_id
), 
user_max_updated_date AS (
SELECT id AS user_id
        , max(updated_date_id) AS max_updated_date
FROM public.user_dimensions
GROUP BY id
),
user_max_updated_date_time AS (
SELECT ud.id AS user_id
        , umud.max_updated_date
        , max(ud.updated_time_id) AS max_updated_time
FROM public.user_dimensions ud
left join user_max_updated_date umud
ON ud.id = umud.user_id
WHERE ud.updated_date_id = umud.max_updated_date
GROUP BY ud.id, umud.max_updated_date
),
user_max_active_date AS (
SELECT id AS user_id
        , max(active_date_id) AS max_active_date
FROM public.user_dimensions
GROUP BY id
),
user_max_active_date_time AS (
SELECT ud.id AS user_id
        , umad.max_active_date
        , max(ud.active_time_id) AS max_active_time
FROM public.user_dimensions ud
left join user_max_active_date umad
ON ud.id = umad.user_id
WHERE ud.active_date_id = umad.max_active_date
GROUP BY ud.id, umad.max_active_date
),
user_dimensions_single AS (
SELECT *
FROM public.user_dimensions u
inner join user_max_updated_date_time umudt
ON umudt.user_id = u.id
AND umudt.max_updated_date = u.updated_date_id
AND umudt.max_updated_time = u.updated_time_id
inner join user_max_active_date_time umadt
ON umadt.user_id = u.id
AND umadt.max_active_date = u.active_date_id
AND umadt.max_active_time = u.active_time_id
),
user_connected_to_remarkable AS (
SELECT 
        u.id AS user_id
        , sum(
                case 
                when c.champion_id=55 then 1
                else 0 
                end
        ) AS connected_to_remarkable
FROM user_dimensions_single u
LEFT JOIN user_connected_to_champion_bridges c
ON u.id=c.user_id
group by u.id
), user_internal_legit AS (
SELECT u.id AS user_id
        , u.email AS user_email
        , u.email IN
          (SELECT DISTINCT email FROM user_dimensions_single WHERE account_type = 'Internal User') 
            AS legit_internal_user
FROM user_dimensions_single u
), 
user_flash_facts AS (
SELECT	u.id AS user_id
        , u.email AS user_email
        , u.account_type AS account_type
        , c.champion_id AS first_champion_id
        , cd.name AS first_champion_name 
        , ucr.connected_to_remarkable=1 AS connected_to_remarkable
        , uil.legit_internal_user
FROM 	user_dimensions_single u
LEFT JOIN user_connected_to_champion_bridges c
        ON u.id=c.user_id
LEFT JOIN uc_seq 
        ON uc_seq.user_id=u.id
LEFT JOIN champion_dimensions cd
        ON cd.id=c.champion_id
LEFT JOIN user_connected_to_remarkable ucr
        ON ucr.user_id=u.id
LEFT JOIN user_internal_legit uil
        ON uil.user_id=u.id
WHERE uc_seq.min_sequence=c.sequence_number
ORDER BY u.id
), 
ufc AS (
SELECT uff.user_id AS user_id
        , (
                case 
                when uff.legit_internal_user 
                        AND uff.user_email IS NOT NULL
                        then 'Internal'
                when uff.first_champion_id IN 
                        (3,4,88,20,6,26,56,39,43) 
                        AND NOT u.account_type='Internal User'
                        AND NOT uff.connected_to_remarkable
                        AND uff.user_email IS NOT NULL
                        then uff.first_champion_name
                when uff.first_champion_id IN 
                        (5,182,34,136,45,69,95,94,93,29,83) 
                        AND NOT u.account_type='Internal User'
                        AND NOT uff.connected_to_remarkable
                        AND uff.user_email IS NOT NULL
                        then 'Cru'
                when uff.first_champion_id IN 
                        (92,14,91,32) 
                        AND NOT u.account_type='Internal User'
                        AND NOT uff.connected_to_remarkable
                        AND uff.user_email IS NOT NULL
                        then 'CFP'
                when uff.first_champion_id IN 
                        (2,7) 
                        AND NOT u.account_type='Internal User'
                        AND NOT uff.connected_to_remarkable
                        AND uff.user_email IS NOT NULL
                        then 'CeDAR'	
                when uff.first_champion_id IN 
                        (37,24,42) 
                        AND NOT u.account_type='Internal User'
                        AND NOT uff.connected_to_remarkable
                        AND uff.user_email IS NOT NULL
                        then 'Date Night'	
                when uff.first_champion_id IN 
                        (11,23,98) 
                        AND NOT u.account_type='Internal User'
                        AND NOT uff.connected_to_remarkable
                        AND uff.user_email IS NOT NULL
                        then 'TYRO'
                when uff.connected_to_remarkable
                        AND uff.user_email IS NOT NULL
                        AND NOT u.account_type='Internal User'
                        then 'Remarkable!'
                when uff.first_champion_id NOT IN 
                        (1,2,3,4,5
                        ,6,7,9,11,13
                        ,14,20,23,24,26
                        ,29,31,32,34,43
                        ,44,45,53,55,56
                        ,69,83,88,91,92
                        ,93,94,95,98,108
                        ,128,130,132,136,137
                        ,77,57,54,51,50
                        ,81,78,65,171,18
                        ,79,41,16,33,17
                        ,113,116,115,48,152
                        ,147,47,39) 
                        AND NOT u.account_type='Internal User'
                        AND NOT uff.connected_to_remarkable
                        AND uff.user_email IS NOT NULL
                        then 'Other'
                when uff.user_email IS NULL
                        then 'Guest'
                else 'Uncategorized' 
                end
        ) AS flash_report_category
FROM user_flash_facts uff
LEFT JOIN user_dimensions_single u
        ON u.id=uff.user_id
),
results AS (
SELECT * FROM ufc
GROUP BY user_id, flash_report_category
ORDER BY user_id, flash_report_category
)
SELECT *
FROM results
;
