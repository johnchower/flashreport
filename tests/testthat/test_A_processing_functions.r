library(RPostgreSQL)

# Connect to redshift
glootility::connect_to_redshift()

# Define temporary tables that future queries will use.
dbSendQuery(redshift_connection$con, 
  flashreport::query_user_flash_cat
)
dbSendQuery(redshift_connection$con,
  flashreport::query_pa_flash_cat
)

# Define date ranges and query types to get results for.
run_date <- as.Date('2017-01-06')
min_date <- as.Date('2016-01-01')
days_between <- as.numeric(run_date - min_date)
min_week <- ceiling(days_between/7)
weeks_back <- min_week:1
start_dates <- run_date - 7*weeks_back
end_dates <- start_dates + 6
year_beginning <- as.Date('2016-01-01')

date_ranges <- data.frame(
  range_types = 
    c(
      rep('week', times = length(weeks_back))
      , rep('ytd', times = length(weeks_back))
    )
  , max_dates = rep(end_dates, times = 2)
  , stringsAsFactors = F
)

query_types <- paste0(c('au', 'pa', 'notifications'), 'Query')

long_flash_report <- flashreport::get_results(date_ranges, query_types)

test_that("get_results returns results.",{
  expect_is(long_flash_report
            , 'data.frame')
  colnames_to_test <- colnames(long_flash_report)
  expected_colnames <- c('user_group'
                         , 'date_range'
                         , 'variable'
                         , 'value')
  expect_equal(colnames_to_test[order(colnames_to_test)]
               , expected_colnames[order(expected_colnames)])
  user_groups_to_test <- unique(long_flash_report$user_group)
  expected_user_groups <- c("Uncategorized"
                            , "CFP"
                            , "CeDAR"
                            , "Compassion International"
                            , "Cru"
                            , "FamilyLife"
                            , "Internal"
                            , "Other"
                            , "REVEAL for Me"
                            , "Remarkable!"
                            , "TYRO"
                            , "Date Night"
                            , "UMI Connection"
                            , "InteGREAT"
                            , "Pacific Dental Services"
                            , "Family Bridges"
                            , "Business As Unusual"
                            , "Stadia"
                            , "Guest"
                            , NA
                            )
  expect_equal(user_groups_to_test[order(user_groups_to_test)]
               , expected_user_groups[order(expected_user_groups)])
  variables_to_test <- unique(long_flash_report$variable)
  expected_variables <- unique(c("active_users_week"
                          , "platform_actions_NA"
                          , "platform_actions_Uncategorized"
                          , "platform_actions_Connect"
                          , "platform_actions_Consume"
                          , "platform_actions_Create"
                          , "platform_actions_Feed"
                          , "platform_actions_Invite"
                          , "platform_actions_Other actions"
                          , "platform_actions_Space"
                          , "platform_actions_To-do"
                          , "notifications_Read"
                          , "notifications_Unobserved"
                          , "notifications_Unread"
                          , "notifications_NA"
                          , "active_users_ytd"
                          ))
  expect_equal(variables_to_test[order(variables_to_test)]
               , expected_variables[order(expected_variables)])
})

long_flash_report_dates_formatted <- 
  flashreport::format_LFR_dates(long_flash_report ) 

test_that("format_LFR_dates does its job",{
  expect_is(long_flash_report_dates_formatted
            , 'data.frame')
  colnames_to_test <- colnames(long_flash_report_dates_formatted)
  expected_colnames <- c('user_group'
                         , 'date_range'
                         , 'variable'
                         , 'value')
  expect_equal(colnames_to_test[order(colnames_to_test)]
               , expected_colnames[order(expected_colnames)])
  user_groups_to_test <- unique(long_flash_report_dates_formatted$user_group)
  expected_user_groups <- c("Uncategorized"
                            , "CFP"
                            , "CeDAR"
                            , "Compassion International"
                            , "Cru"
                            , "FamilyLife"
                            , "Internal"
                            , "Other"
                            , "REVEAL for Me"
                            , "Remarkable!"
                            , "TYRO"
                            , "Date Night"
                            , "UMI Connection"
                            , "InteGREAT"
                            , "Pacific Dental Services"
                            , "Family Bridges"
                            , "Business As Unusual"
                            , "Stadia"
                            , "Guest"
                            , NA
                            )
  expect_equal(user_groups_to_test[order(user_groups_to_test)]
               , expected_user_groups[order(expected_user_groups)])
  variables_to_test <- unique(long_flash_report_dates_formatted$variable)
  expected_variables <- unique(c("active_users_week"
                          , "platform_actions_NA"
                          , "platform_actions_Uncategorized"
                          , "platform_actions_Connect"
                          , "platform_actions_Consume"
                          , "platform_actions_Create"
                          , "platform_actions_Feed"
                          , "platform_actions_Invite"
                          , "platform_actions_Other actions"
                          , "platform_actions_Space"
                          , "platform_actions_To-do"
                          , "notifications_Read"
                          , "notifications_Unobserved"
                          , "notifications_Unread"
                          , "notifications_NA"
                          , "active_users_ytd"
                          ))
  expect_equal(variables_to_test[order(variables_to_test)]
               , expected_variables[order(expected_variables)])
  dates_to_test <- unique(long_flash_report_dates_formatted$date_range)
  expect_is(dates_to_test
            , "character")
  date_formats <- grepl(pattern = "[0-9]{4}/[0-9]{2}/[0-9]{2}"
                        , x = dates_to_test)
  expect_true(all(date_formats))
})

long_flash_report_2 <- 
  flashreport::curate_user_groups(long_flash_report_dates_formatted)

test_that("curate_user_groups does its job",{
  expect_is(long_flash_report_2
            , 'data.frame')
  colnames_to_test <- colnames(long_flash_report_2)
  expected_colnames <- c('user_group'
                         , 'date_range'
                         , 'variable'
                         , 'value')
  expect_equal(colnames_to_test[order(colnames_to_test)]
               , expected_colnames[order(expected_colnames)])
  user_groups_to_test <- unique(long_flash_report_2$user_group)
  expected_user_groups <- c("CFP"
                            , "CeDAR"
                            , "Compassion International"
                            , "Cru"
                            , "FamilyLife"
                            , "Internal"
                            , "Other"
                            , "REVEAL for Me"
                            , "Remarkable!"
                            , "TYRO"
                            , "Date Night"
                            , "UMI Connection"
                            , "InteGREAT"
                            , "Pacific Dental Services"
                            , "Family Bridges"
                            , "Business As Unusual"
                            , "Stadia"
                            )
  expect_equal(user_groups_to_test[order(user_groups_to_test)]
               , expected_user_groups[order(expected_user_groups)])
  variables_to_test <- unique(long_flash_report_2$variable)
  expected_variables <- unique(c("active_users_week"
                          , "platform_actions_Connect"
                          , "platform_actions_Consume"
                          , "platform_actions_Create"
                          , "platform_actions_Feed"
                          , "platform_actions_Invite"
                          , "platform_actions_Other actions"
                          , "platform_actions_Space"
                          , "platform_actions_To-do"
                          , "notifications_Read"
                          , "notifications_Unobserved"
                          , "notifications_Unread"
                          , "notifications_NA"
                          , "active_users_ytd"
                          ))
  expect_equal(variables_to_test[order(variables_to_test)]
               , expected_variables[order(expected_variables)])
  dates_to_test <- unique(long_flash_report_2$date_range)
  expect_is(dates_to_test
            , "character")
  date_formats <- grepl(pattern = "[0-9]{4}/[0-9]{2}/[0-9]{2}"
                        , x = dates_to_test)
  expect_true(all(date_formats))
})

long_flash_report_subaggregate <- 
  flashreport::summarise_by_subaggregate(long_flash_report_2)

test_that("summarise_by_subaggregate does its job",{
  expect_is(long_flash_report_subaggregate
            , 'data.frame')
  colnames_to_test <- colnames(long_flash_report_subaggregate)
  expected_colnames <- c('user_group'
                         , 'date_range'
                         , 'variable'
                         , 'value')
  expect_equal(colnames_to_test[order(colnames_to_test)]
               , expected_colnames[order(expected_colnames)])
  user_groups_to_test <- unique(long_flash_report_subaggregate$user_group)
  expected_user_groups <- c("Total Content"
                            , "Total Enterprise"
                            , "Total Other")
  expect_equal(user_groups_to_test[order(user_groups_to_test)]
               , expected_user_groups[order(expected_user_groups)])
  variables_to_test <- unique(long_flash_report_subaggregate$variable)
  expected_variables <- unique(c("active_users_week"
                          , "platform_actions_Connect"
                          , "platform_actions_Consume"
                          , "platform_actions_Create"
                          , "platform_actions_Feed"
                          , "platform_actions_Invite"
                          , "platform_actions_Other actions"
                          , "platform_actions_Space"
                          , "platform_actions_To-do"
                          , "notifications_Read"
                          , "notifications_Unobserved"
                          , "notifications_Unread"
                          , "notifications_NA"
                          , "active_users_ytd"
                          ))
  expect_equal(variables_to_test[order(variables_to_test)]
               , expected_variables[order(expected_variables)])
  dates_to_test <- unique(long_flash_report_subaggregate$date_range)
  expect_is(dates_to_test
            , "character")
  date_formats <- grepl(pattern = "[0-9]{4}/[0-9]{2}/[0-9]{2}"
                        , x = dates_to_test)
  expect_true(all(date_formats))
})

long_flash_report_isFL <- 
  flashreport::summarise_by_isFL(long_flash_report_2)

test_that("summarise_by_isFL does its job",{
  expect_is(long_flash_report_isFL
            , 'data.frame')
  colnames_to_test <- colnames(long_flash_report_isFL)
  expected_colnames <- c('user_group'
                         , 'date_range'
                         , 'variable'
                         , 'value')
  expect_equal(colnames_to_test[order(colnames_to_test)]
               , expected_colnames[order(expected_colnames)])
  user_groups_to_test <- unique(long_flash_report_isFL$user_group)
  expected_user_groups <- c("All But FamilyLife")
  expect_equal(user_groups_to_test[order(user_groups_to_test)]
               , expected_user_groups[order(expected_user_groups)])
  variables_to_test <- unique(long_flash_report_isFL$variable)
  expected_variables <- unique(c("active_users_week"
                          , "platform_actions_Connect"
                          , "platform_actions_Consume"
                          , "platform_actions_Create"
                          , "platform_actions_Feed"
                          , "platform_actions_Invite"
                          , "platform_actions_Other actions"
                          , "platform_actions_Space"
                          , "platform_actions_To-do"
                          , "notifications_Read"
                          , "notifications_Unobserved"
                          , "notifications_Unread"
                          , "notifications_NA"
                          , "active_users_ytd"
                          ))
  expect_equal(variables_to_test[order(variables_to_test)]
               , expected_variables[order(expected_variables)])
  dates_to_test <- unique(long_flash_report_isFL$date_range)
  expect_is(dates_to_test
            , "character")
  date_formats <- grepl(pattern = "[0-9]{4}/[0-9]{2}/[0-9]{2}"
                        , x = dates_to_test)
  expect_true(all(date_formats))
})

long_flash_report_aggregate <- 
  flashreport::summarise_in_aggregate(long_flash_report_2)

test_that("summarise_in_aggregate does its job",{
  expect_is(long_flash_report_aggregate
            , 'data.frame')
  colnames_to_test <- colnames(long_flash_report_aggregate)
  expected_colnames <- c('user_group'
                         , 'date_range'
                         , 'variable'
                         , 'value')
  expect_equal(colnames_to_test[order(colnames_to_test)]
               , expected_colnames[order(expected_colnames)])
  user_groups_to_test <- unique(long_flash_report_aggregate$user_group)
  expected_user_groups <- c("Total")
  expect_equal(user_groups_to_test[order(user_groups_to_test)]
               , expected_user_groups[order(expected_user_groups)])
  variables_to_test <- unique(long_flash_report_aggregate$variable)
  expected_variables <- unique(c("active_users_week"
                          , "platform_actions_Connect"
                          , "platform_actions_Consume"
                          , "platform_actions_Create"
                          , "platform_actions_Feed"
                          , "platform_actions_Invite"
                          , "platform_actions_Other actions"
                          , "platform_actions_Space"
                          , "platform_actions_To-do"
                          , "notifications_Read"
                          , "notifications_Unobserved"
                          , "notifications_Unread"
                          , "notifications_NA"
                          , "active_users_ytd"
                          ))
  expect_equal(variables_to_test[order(variables_to_test)]
               , expected_variables[order(expected_variables)])
  dates_to_test <- unique(long_flash_report_aggregate$date_range)
  expect_is(dates_to_test
            , "character")
  date_formats <- grepl(pattern = "[0-9]{4}/[0-9]{2}/[0-9]{2}"
                        , x = dates_to_test)
  expect_true(all(date_formats))
})

long_flash_report_3 <- rbind(long_flash_report_2
                                 , long_flash_report_subaggregate
                                 , long_flash_report_isFL
                                 , long_flash_report_aggregate)

long_flash_report_WAU_pct <- 
  flashreport::calculate_WAU_percentage(long_flash_report_3)

test_that("calculate_WAU_percentage does its job",{
  object_to_test <- long_flash_report_WAU_pct
  expect_is(object_to_test
            , 'data.frame')
  colnames_to_test <- colnames(object_to_test)
  expected_colnames <- c('user_group'
                         , 'date_range'
                         , 'variable'
                         , 'value')
  expect_equal(colnames_to_test[order(colnames_to_test)]
               , expected_colnames[order(expected_colnames)])
  user_groups_to_test <- unique(object_to_test$user_group)
  expected_user_groups <- c("CFP"
                            , "CeDAR"
                            , "Compassion International"
                            , "Cru"
                            , "FamilyLife"
                            , "Internal"
                            , "Other"
                            , "REVEAL for Me"
                            , "Remarkable!"
                            , "TYRO"
                            , "Date Night"
                            , "UMI Connection"
                            , "InteGREAT"
                            , "Pacific Dental Services"
                            , "Family Bridges"
                            , "Business As Unusual"
                            , "Stadia"
                            , "Total Content"
                            , "Total Enterprise"
                            , "Total Other"
                            , "Total"
                            , "All But FamilyLife"
                            )
  expect_equivalent(user_groups_to_test[order(user_groups_to_test)]
                    , expected_user_groups[order(expected_user_groups)])
  variables_to_test <- unique(object_to_test$variable)
  expected_variables <- c("active_users_WAU_pct")
  expect_equivalent(as.character(variables_to_test[order(variables_to_test)])
                    , expected_variables[order(expected_variables)])
  dates_to_test <- unique(object_to_test$date_range)
  expect_is(dates_to_test
            , "character")
  date_formats <- grepl(pattern = "[0-9]{4}/[0-9]{2}/[0-9]{2}"
                        , x = dates_to_test)
  expect_true(all(date_formats))
})

long_flash_report_total_actions <- 
  flashreport::calculate_total_actions(long_flash_report_3)

test_that("calculate_total_actions does its job",{
  object_to_test <- long_flash_report_total_actions
  expect_is(object_to_test
            , 'data.frame')
  colnames_to_test <- colnames(object_to_test)
  expected_colnames <- c('user_group'
                         , 'date_range'
                         , 'variable'
                         , 'value')
  expect_equal(colnames_to_test[order(colnames_to_test)]
               , expected_colnames[order(expected_colnames)])
  user_groups_to_test <- unique(object_to_test$user_group)
  expected_user_groups <- c("CFP"
                            , "CeDAR"
                            , "Compassion International"
                            , "Cru"
                            , "FamilyLife"
                            , "Internal"
                            , "Other"
                            , "REVEAL for Me"
                            , "Remarkable!"
                            , "TYRO"
                            , "Date Night"
                            , "UMI Connection"
                            , "InteGREAT"
                            , "Pacific Dental Services"
                            , "Family Bridges"
                            , "Business As Unusual"
                            , "Stadia"
                            , "Total Content"
                            , "Total Enterprise"
                            , "Total Other"
                            , "Total"
                            , "All But FamilyLife"
                            )
  expect_equivalent(user_groups_to_test[order(user_groups_to_test)]
                    , expected_user_groups[order(expected_user_groups)])
  variables_to_test <- unique(object_to_test$variable)
  expected_variables <- c("platform_actions_total")
  expect_equivalent(variables_to_test[order(variables_to_test)]
                    , expected_variables[order(expected_variables)])
  dates_to_test <- unique(object_to_test$date_range)
  expect_is(dates_to_test
            , "character")
  date_formats <- grepl(pattern = "[0-9]{4}/[0-9]{2}/[0-9]{2}"
                        , x = dates_to_test)
  expect_true(all(date_formats))
})

long_flash_report_actions_per_AU <-
  flashreport::calculate_actions_per_AU(
    long_flash_report_3
    , long_flash_report_total_actions
  )

test_that("calculate_actions_per_AU does its job",{
  object_to_test <- long_flash_report_actions_per_AU
  expect_is(object_to_test
            , 'data.frame')
  colnames_to_test <- colnames(object_to_test)
  expected_colnames <- c('user_group'
                         , 'date_range'
                         , 'variable'
                         , 'value')
  expect_equal(colnames_to_test[order(colnames_to_test)]
               , expected_colnames[order(expected_colnames)])
  user_groups_to_test <- unique(object_to_test$user_group)
  expected_user_groups <- c("CFP"
                            , "CeDAR"
                            , "Compassion International"
                            , "Cru"
                            , "FamilyLife"
                            , "Internal"
                            , "Other"
                            , "REVEAL for Me"
                            , "Remarkable!"
                            , "TYRO"
                            , "Date Night"
                            , "UMI Connection"
                            , "InteGREAT"
                            , "Pacific Dental Services"
                            , "Family Bridges"
                            , "Business As Unusual"
                            , "Stadia"
                            , "Total Content"
                            , "Total Enterprise"
                            , "Total Other"
                            , "Total"
                            , "All But FamilyLife"
                            )
  expect_equivalent(user_groups_to_test[order(user_groups_to_test)]
                    , expected_user_groups[order(expected_user_groups)])
  variables_to_test <- unique(object_to_test$variable)
  expected_variables <- c("platform_actions_per_active_user")
  expect_equal(as.character(variables_to_test[order(variables_to_test)])
               , expected_variables[order(expected_variables)])
  dates_to_test <- unique(object_to_test$date_range)
  expect_is(dates_to_test
            , "character")
  date_formats <- grepl(pattern = "[0-9]{4}/[0-9]{2}/[0-9]{2}"
                        , x = dates_to_test)
  expect_true(all(date_formats))
})

long_flash_report_NRR <- 
  flashreport::calculate_NRR(long_flash_report_3)

test_that("calculate_NRR does its job",{
  object_to_test <- long_flash_report_NRR
  expect_is(object_to_test
            , 'data.frame')
  colnames_to_test <- colnames(object_to_test)
  expected_colnames <- c('user_group'
                         , 'date_range'
                         , 'variable'
                         , 'value')
  expect_equal(colnames_to_test[order(colnames_to_test)]
               , expected_colnames[order(expected_colnames)])
  user_groups_to_test <- unique(object_to_test$user_group)
  expected_user_groups <- c("CFP"
                            , "CeDAR"
                            , "Compassion International"
                            , "Cru"
                            , "FamilyLife"
                            , "Internal"
                            , "Other"
                            , "REVEAL for Me"
                            , "Remarkable!"
                            , "TYRO"
                            , "Date Night"
                            , "UMI Connection"
                            , "InteGREAT"
                            , "Pacific Dental Services"
                            , "Family Bridges"
                            , "Business As Unusual"
                            , "Stadia"
                            , "Total Content"
                            , "Total Enterprise"
                            , "Total Other"
                            , "Total"
                            , "All But FamilyLife"
                            )
  expect_equivalent(user_groups_to_test[order(user_groups_to_test)]
                    , expected_user_groups[order(expected_user_groups)])
  variables_to_test <- unique(object_to_test$variable)
  expected_variables <- c("notifications_response_rate")
  expect_equal(variables_to_test[order(variables_to_test)]
               , expected_variables[order(expected_variables)])
  dates_to_test <- unique(object_to_test$date_range)
  expect_is(dates_to_test
            , "character")
  date_formats <- grepl(pattern = "[0-9]{4}/[0-9]{2}/[0-9]{2}"
                        , x = dates_to_test)
  expect_true(all(date_formats))
})

RPostgreSQL::dbDisconnect(conn = redshift_connection$con)
