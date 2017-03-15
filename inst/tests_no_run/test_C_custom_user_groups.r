library(RPostgreSQL)
library(dplyr)

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
run_date <- as.Date("2017-01-06")
min_date <- as.Date("2016-01-01")
days_between <- as.numeric(run_date - min_date)
min_week <- ceiling(days_between / 7)
weeks_back <- min_week:1
start_dates <- run_date - 7 * weeks_back
end_dates <- start_dates + 6
year_beginning <- as.Date("2016-01-01")

date_ranges <- data.frame(
  range_types =
    c(
      rep("week", times = length(weeks_back))
      , rep("ytd", times = length(weeks_back))
    )
  , max_dates = rep(end_dates, times = 2)
  , stringsAsFactors = F
)

query_types <- paste0(c("au", "pa", "notifications"), "Query")

user_group_test <- 1:50
user_group_query_test <-
  paste0("SELECT ud.id AS user_id FROM user_dimensions ud WHERE ud.id IN ("
         , paste(user_group_test, collapse = ",")
         , ")")
user_group_name_test <- "users_1_50"

base_variables <- unique(c("active_users_week"
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
long_flash_report <- get_results(
  date_ranges
  , query_types
  , user_group = user_group_test
  , user_group_name = user_group_name_test
)

long_flash_report_test_query <- flashreport::get_results(
  date_ranges
  , query_types
  , user_group_query = user_group_query_test
  , user_group_name = user_group_name_test
)

test_that("get_results returns correctly filtered results when user_group
          is specified.", {
  object_to_test <- long_flash_report
  expect_is(object_to_test
            , "data.frame")
  colnames_to_test <- colnames(object_to_test)
  expected_colnames <- c("user_group"
                         , "date_range"
                         , "variable"
                         , "value")
  expect_equal(colnames_to_test[order(colnames_to_test)]
               , expected_colnames[order(expected_colnames)])
  user_groups_to_test <- unique(object_to_test$user_group)
  expected_user_groups <- user_group_name_test
  expect_equal(user_groups_to_test[order(user_groups_to_test)]
               , expected_user_groups[order(expected_user_groups)])
  variables_to_test <- unique(object_to_test$variable)
  expected_variables <- base_variables
  expect_equal(variables_to_test[order(variables_to_test)]
               , expected_variables[order(expected_variables)])
  expected_active_users_ytd <- length(unique(user_group_test))
  active_users_ytd_to_test <- object_to_test %>%
    filter(variable == "active_users_ytd") %>% {
      .$value
    } %>%
    max
  expect_lte(object = active_users_ytd_to_test
                       , expected = expected_active_users_ytd)
  testthat::expect_equal(object = dplyr::arrange(long_flash_report_test_query
                                               , user_group
                                               , date_range
                                               , variable
                                               , value)
                         , expected = dplyr::arrange(object_to_test
                                               , user_group
                                               , date_range
                                               , variable
                                               , value)
                         )
})

# Postprocess results.
long_flash_report_dates_formatted <-
  flashreport::format_LFR_dates(long_flash_report )

test_that("format_LFR_dates does its job", {
  expect_is(long_flash_report_dates_formatted
            , "data.frame")
  colnames_to_test <- colnames(long_flash_report_dates_formatted)
  expected_colnames <- c("user_group"
                         , "date_range"
                         , "variable"
                         , "value")
  expect_equal(colnames_to_test[order(colnames_to_test)]
               , expected_colnames[order(expected_colnames)])
  user_groups_to_test <- unique(long_flash_report_dates_formatted$user_group)
  expected_user_groups <- user_group_name_test
  expect_equal(user_groups_to_test[order(user_groups_to_test)]
               , expected_user_groups[order(expected_user_groups)])
  variables_to_test <- unique(long_flash_report_dates_formatted$variable)
  expected_variables <- base_variables
  expect_equal(variables_to_test[order(variables_to_test)]
               , expected_variables[order(expected_variables)])
  dates_to_test <- unique(long_flash_report_dates_formatted$date_range)
  expect_is(dates_to_test
            , "character")
  date_formats <- grepl(pattern = "[0-9]{4}/[0-9]{2}/[0-9]{2}"
                        , x = dates_to_test)
  expect_true(all(date_formats))
})

long_flash_report_3 <- rbind(long_flash_report_dates_formatted)

long_flash_report_WAU_pct <-
  flashreport::calculate_WAU_percentage(long_flash_report_3)

test_that("calculate_WAU_percentage does its job", {
  object_to_test <- long_flash_report_WAU_pct
  expect_is(object_to_test
            , "data.frame")
  colnames_to_test <- colnames(object_to_test)
  expected_colnames <- c("user_group"
                         , "date_range"
                         , "variable"
                         , "value")
  expect_equal(colnames_to_test[order(colnames_to_test)]
               , expected_colnames[order(expected_colnames)])
  user_groups_to_test <- unique(object_to_test$user_group)
  expected_user_groups <- user_group_name_test
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

test_that("calculate_total_actions does its job", {
  object_to_test <- long_flash_report_total_actions
  expect_is(object_to_test
            , "data.frame")
  colnames_to_test <- colnames(object_to_test)
  expected_colnames <- c("user_group"
                         , "date_range"
                         , "variable"
                         , "value")
  expect_equal(colnames_to_test[order(colnames_to_test)]
               , expected_colnames[order(expected_colnames)])
  user_groups_to_test <- unique(object_to_test$user_group)
  expected_user_groups <- user_group_name_test
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

test_that("calculate_actions_per_AU does its job", {
  object_to_test <- long_flash_report_actions_per_AU
  expect_is(object_to_test
            , "data.frame")
  colnames_to_test <- colnames(object_to_test)
  expected_colnames <- c("user_group"
                         , "date_range"
                         , "variable"
                         , "value")
  expect_equal(colnames_to_test[order(colnames_to_test)]
               , expected_colnames[order(expected_colnames)])
  user_groups_to_test <- unique(object_to_test$user_group)
  expected_user_groups <- user_group_name_test
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

# Calculate notifications_response_rate. 
long_flash_report_NRR <-
  flashreport::calculate_NRR(long_flash_report_3)

test_that("calculate_NRR does its job", {
  object_to_test <- long_flash_report_NRR
  expect_is(object_to_test
            , "data.frame")
  colnames_to_test <- colnames(object_to_test)
  expected_colnames <- c("user_group"
                         , "date_range"
                         , "variable"
                         , "value")
  expect_equal(colnames_to_test[order(colnames_to_test)]
               , expected_colnames[order(expected_colnames)])
  user_groups_to_test <- unique(object_to_test$user_group)
  expected_user_groups <- user_group_name_test
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
