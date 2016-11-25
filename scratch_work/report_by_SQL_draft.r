glootility::connect_to_redshift()

library(RPostgreSQL)
library(glootility)

dbSendQuery(redshift_connection$con, "
  /* Platform action grouping */
  CREATE TEMPORARY TABLE pa_flash_cat 
  (
          platform_action VARCHAR
          , flash_report_category VARCHAR
  )
  ;

  INSERT INTO pa_flash_cat (platform_action, flash_report_category)
  VALUES
          ('Accepted User Connection','Connect')
          , ('Account Created','Uncategorized')
          , ('Added Champion Analytics Widget','Other actions')
          , ('Added Champion Assessment Analytics Widget','Other actions')
          , ('Added Champion Cohort Overview Widget','Other actions')
          , ('Added Champion Program Analytics Widget','Other actions')
          , ('Added Collection to Queue','Uncategorized')
          , ('Added Moment to Queue','Uncategorized')
          , ('Added To-Do Item from Content','To-do')
          , ('Added Tree to Queue','Uncategorized')
          , ('Added User Program Progress Widget','Other actions')
          , ('Added User Space Widget','Other actions')
          , ('Answered Assessment Item','Consume')
          , ('Assigned To-Do Item','To-do')
          , ('Became Champion Member','Other actions')
          , ('Became Organization Member','Other actions')
          , ('Champion Membership Invitation Accepted','Other actions')
          , ('Champion Membership Invitation Declined','Other actions')
          , ('Clicked Button on Page','Other actions')
          , ('Clicked Collection Card on Page','Uncategorized')
          , ('Clicked External Link Card on Page','Uncategorized')
          , ('Clicked Moment Card on Page','Uncategorized')
          , ('Clicked Page Card on Page','Uncategorized')
          , ('Clicked Program Card on Page','Uncategorized')
          , ('Clicked Social Icon on Page','Other actions')
          , ('Cloned Program','Uncategorized')
          , ('Commented on Group Space Post','Space')
          , ('Commented on Private Space Post','Space')
          , ('Commented on Shared Space Post','Space')
          , ('Commented on Timeline Post','Feed')
          , ('Commented on Unactivated Space Post','Space')
          , ('Completed To-Do Item','To-do')
          , ('Connected to Champion','Connect')
          , ('Created AddToQueueElement Element','Uncategorized')
          , ('Created AssessmentMetricPrerequisite Prerequisite','Uncategorized')
          , ('Created AssessmentResultPrerequisite Prerequisite','Uncategorized')
          , ('Created BlockquoteElement Element','Uncategorized')
          , ('Created ChampionConnectElement Element','Uncategorized')
          , ('Created CohortConnectElement Element','Uncategorized')
          , ('Created ConfigureChampionDashboardElement Element','Uncategorized')
          , ('Created ConfigureUserDashboardElement Element','Uncategorized')
          , ('Created ContentNode Node','Uncategorized')
          , ('Created CreateSpaceElement Element','Uncategorized')
          , ('Created DisplayChildrenElement Element','Uncategorized')
          , ('Created EparachuteElement Element','Uncategorized')
          , ('Created FileListElement Element','Uncategorized')
          , ('Created FormElement Element','Uncategorized')
          , ('Created FormReferenceElement Element','Uncategorized')
          , ('Created Group Space','Uncategorized')
          , ('Created HeaderElement Element','Uncategorized')
          , ('Created HorizontalRuleElement Element','Uncategorized')
          , ('Created ImageElement Element','Uncategorized')
          , ('Created IntervalSinceStartPrerequisite Prerequisite','Uncategorized')
          , ('Created JoinGroupSpaceElement Element','Uncategorized')
          , ('Created LandingPageElement Element','Uncategorized')
          , ('Created ListElement Element','Uncategorized')
          , ('Created MomentElement Element','Uncategorized')
          , ('Created Note in Content','Create')
          , ('Created OnboardingElement Element','Uncategorized')
          , ('Created ParagraphElement Element','Uncategorized')
          , ('Created PlatformLinkElement Element','Uncategorized')
          , ('Created Private Collection','Create')
          , ('Created Private Moment','Create')
          , ('Created Private Space','Uncategorized')
          , ('Created Program','Create')
          , ('Created Public Collection','Create')
          , ('Created Public Moment','Create')
          , ('Created QuestionElement Element','Uncategorized')
          , ('Created QuizElement Element','Uncategorized')
          , ('Created ReferenceNode Node','Uncategorized')
          , ('Created ResourceListElement Element','Uncategorized')
          , ('Created ResultElement Element','Uncategorized')
          , ('Created Shared Space','Uncategorized')
          , ('Created SoundcloudElement Element','Uncategorized')
          , ('Created ToDoListElement Element','Uncategorized')
          , ('Created Unactivated Space','Uncategorized')
          , ('Created UnlockDatePrerequisite Prerequisite','Uncategorized')
          , ('Created UserImageElement Element','Uncategorized')
          , ('Created UserNoteElement Element','Uncategorized')
          , ('Created VideoElement Element','Uncategorized')
          , ('Created VideoListElement Element','Uncategorized')
          , ('Created VideoPlaylistElement Element','Uncategorized')
          , ('Created WebLinkElement Element','Uncategorized')
          , ('Created a New Dashboard','Connect')
          , ('Deleted Element','Uncategorized')
          , ('Deleted Node','Uncategorized')
          , ('Deleted Prerequisite','Uncategorized')
          , ('Deleted Program','Uncategorized')
          , ('Deleted To-Do Item','To-do')
          , ('FamilyLife - Clicked Donate Button','Consume')
          , ('FamilyLife - Clicked Resource Link','Consume')
          , ('FamilyLife - Listened to Broadcast','Consume')
          , ('Followed Champion','Uncategorized')
          , ('Followed User','Connect')
          , ('Invited User To Group Space','Invite')
          , ('Invited User To Private Space','Invite')
          , ('Invited User To Shared Space','Invite')
          , ('Invited User To Unactivated Space','Uncategorized')
          , ('Joined Group Space','Space')
          , ('Joined Private Space','Space')
          , ('Joined Shared Space','Space')
          , ('Joined Unactivated Space','Space')
          , ('Left Group Space','Space')
          , ('Left Private Space','Space')
          , ('Left Shared Space','Space')
          , ('Left Unactivated Space','Space')
          , ('Listened to Broadcast on Page','Consume')
          , ('Made Collection Private','Create')
          , ('Made Collection Public','Create')
          , ('Made Moment Private','Create')
          , ('Made Moment Public','Create')
          , ('Post to Timeline','Feed')
          , ('Posted to Group Space','Space')
          , ('Posted to Private Space','Space')
          , ('Posted to Shared Space','Space')
          , ('Progressed Through Content','Consume')
          , ('Rated Champion','Other actions')
          , ('Rated Program','Other actions')
          , ('Requested User Connection','Connect')
          , ('Sent Tree Invitation','Invite')
          , ('Shared Champion to Group Space','Space')
          , ('Shared Champion to Shared Space','Space')
          , ('Shared Collection to Group Space','Space')
          , ('Shared Collection to Private Space','Space')
          , ('Shared Collection to Shared Space','Space')
          , ('Shared Collection to Timeline','Feed')
          , ('Shared Form Response to Group Space','Space')
          , ('Shared Form Response to Private Space','Space')
          , ('Shared Form Response to Shared Space','Space')
          , ('Shared Form Response to Timeline','Feed')
          , ('Shared LandingPage to Group Space','Space')
          , ('Shared LandingPage to Private Space','Space')
          , ('Shared LandingPage to Shared Space','Space')
          , ('Shared LandingPage to Timeline','Feed')
          , ('Shared Link to Group Space','Space')
          , ('Shared Link to Private Space','Space')
          , ('Shared Link to Shared Space','Space')
          , ('Shared Link to Timeline','Feed')
          , ('Shared Media to Group Space','Space')
          , ('Shared Media to Private Space','Space')
          , ('Shared Media to Shared Space','Space')
          , ('Shared Media to Timeline','Feed')
          , ('Shared Moment to Group Space','Space')
          , ('Shared Moment to Private Space','Space')
          , ('Shared Moment to Shared Space','Space')
          , ('Shared Moment to Timeline','Feed')
          , ('Shared Note to Group Space','Space')
          , ('Shared Note to Private Space','Space')
          , ('Shared Note to Shared Space','Space')
          , ('Shared Note to Timeline','Feed')
          , ('Shared Post to Group Space','Space')
          , ('Shared Post to Private Space','Space')
          , ('Shared Post to Shared Space','Space')
          , ('Shared Post to Timeline','Feed')
          , ('Shared Program to Group Space','Space')
          , ('Shared Program to Private Space','Space')
          , ('Shared Program to Shared Space','Space')
          , ('Shared Program to Timeline','Feed')
          , ('Shared Result to Group Space','Space')
          , ('Shared Result to Private Space','Space')
          , ('Shared Result to Shared Space','Space')
          , ('Shared Result to Timeline','Feed')
          , ('Space Membership Invitation Accepted','Space')
          , ('Space Membership Invitation Declined','Space')
          , ('Started Content','Consume')
          , ('Started Moment','Consume')
          , ('Started Session','Uncategorized')
          , ('Unpublished Program','Uncategorized')
          , ('Updated Node','Uncategorized')
          , ('Updated Prerequisite','Uncategorized')
          , ('Updated AddToQueueElement Element','Uncategorized')
          , ('Updated AssessmentMetricPrerequisite Prerequisite','Uncategorized')
          , ('Updated AssessmentResultPrerequisite Prerequisite','Uncategorized')
          , ('Updated BlockquoteElement Element','Uncategorized')
          , ('Updated ChampionConnectElement Element','Uncategorized')
          , ('Updated CohortConnectElement Element','Uncategorized')
          , ('Updated ContentNode Node','Uncategorized')
          , ('Updated CreateSpaceElement Element','Uncategorized')
          , ('Updated DisplayChildrenElement Element','Uncategorized')
          , ('Updated EparachuteElement Element','Uncategorized')
          , ('Updated FormElement Element','Uncategorized')
          , ('Updated FormReferenceElement Element','Uncategorized')
          , ('Updated HeaderElement Element','Uncategorized')
          , ('Updated HorizontalRuleElement Element','Uncategorized')
          , ('Updated ImageElement Element','Uncategorized')
          , ('Updated IntervalSinceStartPrerequisite Prerequisite','Uncategorized')
          , ('Updated JoinGroupSpaceElement Element','Uncategorized')
          , ('Updated LandingPageElement Element','Uncategorized')
          , ('Updated ListElement Element','Uncategorized')
          , ('Updated MomentElement Element','Uncategorized')
          , ('Updated OnboardingElement Element','Uncategorized')
          , ('Updated ParagraphElement Element','Uncategorized')
          , ('Updated PlatformLinkElement Element','Uncategorized')
          , ('Updated Program','Uncategorized')
          , ('Updated QuestionElement Element','Uncategorized')
          , ('Updated QuizElement Element','Uncategorized')
          , ('Updated ReferenceNode Node','Uncategorized')
          , ('Updated ResourceListElement Element','Uncategorized')
          , ('Updated ResultElement Element','Uncategorized')
          , ('Updated SoundcloudElement Element','Uncategorized')
          , ('Updated ToDoListElement Element','Uncategorized')
          , ('Updated UnlockDatePrerequisite Prerequisite','Uncategorized')
          , ('Updated UserImageElement Element','Uncategorized')
          , ('Updated UserNoteElement Element','Uncategorized')
          , ('Updated VideoElement Element','Uncategorized')
          , ('Updated VideoPlaylistElement Element','Uncategorized')
          , ('Updated WebLinkElement Element','Uncategorized')
          , ('User Added Page','Consume')
          , ('User Onboarded','Invite')
          , ('User Removed Page','Consume')
;
")

dbSendQuery(redshift_connection$con, "
  /* user_flash_facts */
  CREATE TEMPORARY TABLE user_flash_cat AS
          WITH uc_seq AS (
          SELECT user_id
               , MAX(sequence_number) AS max_sequence
               , MIN(sequence_number) AS min_sequence
          FROM user_connected_to_champion_bridges
          GROUP BY user_id
          ), user_connected_to_remarkable AS(
          SELECT 
                  u.id AS user_id
                  , sum(
                          case 
                          when c.champion_id=55 then 1
                          else 0 
                          end
                  ) AS connected_to_remarkable
          FROM user_dimensions u
          LEFT JOIN user_connected_to_champion_bridges c
          ON u.id=c.user_id
          group by u.id
          ), user_internal_legit AS(
          SELECT u.id AS user_id
                  , u.email AS user_email
                  , u.email IN 
                          ('ashiemke@tangogroup.com'
                                  ,'abooysen@tangogroup.com'
                                  ,'alauderdale@tangogroup.com'
                                  ,'ataujenis@tangogroup.com'
                                  ,'ashafer@tangogroup.com'
                                  ,'apreger@tangogroup.com'
                                  ,'becky@tangogroup.com'
                                  ,'bgreeno@tangogroup.com'
                                  ,'btremper@tangogroup.com'
                                  ,'bboylan@tangogroup.com'
                                  ,'bbonifiled@tangogroup.com'
                                  ,'bschafer@tangogroup.com'
                                  ,'byoung.hsp@gmail.com'
                                  ,'broberts@tangogroup.com'
                                  ,'bgrayless@tangogroup.com'
                                  ,'bjohnson@tangogroup.com'
                                  ,'bmcevoy@tangogroup.com'
                                  ,'bsaltshalcomb@tangogroup.com'
                                  ,'bturney@tangogroup.com'
                                  ,'carolynleeturney@gmail.com'
                                  ,'cgoodroe@tangogroup.com'
                                  ,'cjoyce@tangogroup.com'
                                  ,'cmarks@tangogroup.com'
                                  ,'csmola@tangogroup.com'
                                  ,'chebets@tangogroup.com'
                                  ,'crogers@tangogroup.com'
                                  ,'corey@capelio.com'
                                  ,'dgunter@tangogroup.com'
                                  ,'dclements@tangogroup.com'
                                  ,'dmingo@tangogroup.com'
                                  ,'drouch@tangogroup.com'
                                  ,'dkline@tangogroup.com'
                                  ,'dwilson@tangogroup.com'
                                  ,'ehahn@tangogroup.com'
                                  ,'elehnert@tangogroup.com'
                                  ,'etoy@tangogroup.com'
                                  ,'eschurter@tangogroup.com'
                                  ,'eoneil@tangogroup.com'
                                  ,'eguin@tangogroup.com'
                                  ,'enava@tangogroup.com'
                                  ,'eshirk@tangogroup.com'
                                  ,'eswanson@tangogroup.com'
                                  ,'hellerbach@tangogroup.com'
                                  ,'jstewart@tangogroup.com'
                                  ,'jjacobsen@tangogroup.com'
                                  ,'jvandenberge@tangogroup.com'
                                  ,'jswearingen@tangogroup.com'
                                  ,'jtilley@tangogroup.com'
                                  ,'jwenell@tangogroup.com'
                                  ,'jeffcaliguire@tangogroup.com'
                                  ,'jfray@tangogroup.com'
                                  ,'jbojar@tangogroup.com'
                                  ,'jfishbaugh@tangogroup.com'
                                  ,'jandres@tangogroup.com'
                                  ,'jhinegardner@tangogroup.com'
                                  ,'jmaron@tangogroup.com'
                                  ,'jhower@tangogroup.com'
                                  ,'jcaliguire@tangogroup.com'
                                  ,'jkuster@tangogroup.com'
                                  ,'jcoombs@tangogroup.com'
                                  ,'jofray@tangogroup.com'
                                  ,'jneighbors@tangogroup.com'
                                  ,'jashby@tangogroup.com'
                                  ,'jchadbourne@tangogroup.com'
                                  ,'jvallelonga+notesting@tangogroup.com'
                                  ,'ksmith@tangogroup.com'
                                  ,'kjeremko@tangogroup.com'
                                  ,'kaubertot@tangogroup.com'
                                  ,'kclingersmith@tangogroup.com'
                                  ,'kmorrison@tangogroup.com'
                                  ,'lweiner@tangogroup.com'
                                  ,'lbowdey@tangogroup.com'
                                  ,'lmashkouri@tangogroup.com'
                                  ,'lmarks@tangogroup.com'
                                  ,'lswanson@tangogroup.com'
                                  ,'mgraham@tangogroup.com'
                                  ,'mlupa@tangogroup.com'
                                  ,'msmay@tangogroup.com'
                                  ,'mdavis@tangogroup.com'
                                  ,'mmcconnell@tangogroup.com'
                                  ,'mkatz@tangogroup.com'
                                  ,'mgubba@tangogroup.com'
                                  ,'mtrubacz@tangogroup.com'
                                  ,'mindy.caliguire@tangogroup.com'
                                  ,'mlarsen@tangogroup.com'
                                  ,'mgingrich@tangogroup.com'
                                  ,'nsmith@tangogroup.com'
                                  ,'nykema@tanogroup.com'
                                  ,'nick.smith@tangogroup.com'
                                  ,'nvalencia@tangogroup.com'
                                  ,'nclark@tangogroup.com'
                                  ,'pceron@tangogroup.com'
                                  ,'pkeller@tangogroup.com'
                                  ,'peterhan777@gmail.com'
                                  ,'plarson@tangogroup.com'
                                  ,'rhughey@tangogroup.com'
                                  ,'rschirmer@tangogroup.com'
                                  ,'rmoses@tangogroup.com'
                                  ,'rcarpenter@tangogroup.com'
                                  ,'rortiz@tangogroup.com'
                                  ,'rholdeman@tangogroup.com'
                                  ,'Ryanrtr@gmail.com'
                                  ,'sunrein@tangogroup.com'
                                  ,'smccord@tangogroup.com'
                                  ,'styson@tangogroup.com'
                                  ,'beck@tangogroup.com'
                                  ,'smalone@tangogroup.com'
                                  ,'ssymmank@tangogroup.com'
                                  ,'sconnolly17@gmail.com'
                                  ,'staylor@tangogroup.com'
                                  ,'shannon@bertucc.io'
                                  ,'slindow@tangogroup.com'
                                  ,'serskine86@gmail.com'
                                  ,'smefford@tangogroup.com'
                                  ,'slaflora@tangogroup.com'
                                  ,'sdilla@gmail.com'
                                  ,'stuart@tangogroup.com'
                                  ,'selam@tangogroup.com'
                                  ,'swehrly@tangogroup.com'
                                  ,'srinehart@tangogroup.com'
                                  ,'twolters@tangogorup.com'
                                  ,'tclary@tangogroup.com'
                                  ,'trichards@tangogroup.com'
                                  ,'ttuck@tangogroup.com'
                                  ,'tgroom@tangogroup.com'
                                  ,'tyler.r.briggs89@gmail.com'
                                  ,'tory@tangogroup.com'
                                  ,'vvigil@tangogroup.com'
                                  ,'wlorenzen@tangogroup.com'
                                  ,'wemerson@tangogroup.com')
                  AS legit_internal_user
          FROM user_dimensions u
          ), user_flash_facts AS(
          SELECT	u.id AS user_id
                  , u.email AS user_email
                  , u.account_type AS account_type
                  , c.champion_id AS first_champion_id
                  , cd.name AS first_champion_name 
                  , ucr.connected_to_remarkable=1 AS connected_to_remarkable
                  , uil.legit_internal_user
          FROM 	user_dimensions u
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
          ), ufc AS(
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
          LEFT JOIN user_dimensions u
                  ON u.id=uff.user_id
          )
                  select * from ufc
  ;
")

