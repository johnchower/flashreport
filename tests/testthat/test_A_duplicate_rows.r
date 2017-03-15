print("Running test_A_duplicate_rows.r")
library(RPostgreSQL)
library(dplyr)

# Connect to redshift
glootility::connect_to_redshift()

# Define temporary tables that future queries will use.
dbSendQuery(redshift_connection$con,
  query_user_flash_cat
)
dbSendQuery(redshift_connection$con,
  query_pa_flash_cat
)

object_to_test <- RPostgreSQL::dbGetQuery(conn = redshift_connection$con
                        , statement = "SELECT * FROM user_flash_cat")

test_that("user_flash_cat returns exactly one row per user.", {
  number_of_duplicates <- object_to_test %>%
    group_by(user_id) %>%
    summarise(count_rows = n()) %>%
    filter(count_rows > 1) 
  number_of_duplicates <- number_of_duplicates %>%
    nrow
  expect_equal(object = number_of_duplicates
               , expected = 0)
})

test_that("user_flash_cat returns one flash report category per user.", {
  number_of_duplicates <- object_to_test %>%
    group_by(user_id) %>%
    summarise(count_distinct_flash_report_categories = 
                length(unique(flash_report_category))) %>%
    filter(count_distinct_flash_report_categories > 1) %>%
    nrow
  expect_equal(object = number_of_duplicates
               , expected = 0)
})

object_to_test <- RPostgreSQL::dbGetQuery(conn = redshift_connection$con
                        , statement = "SELECT * FROM pa_flash_cat")
test_that("pa_flash_cat returns results.", {
  expect_is(object_to_test, "data.frame")
  expect_gt(object = nrow(object_to_test)
            , expected = 0)
})
RPostgreSQL::dbDisconnect(conn = redshift_connection$con)
