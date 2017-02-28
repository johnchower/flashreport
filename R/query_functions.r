#' An S4 class to represent a Flash Report query.
#'
#' @slot min_date A Date object, representing the first day of the date ranges
#' for which results are collected.
#' @slot max_date A Date object, representing the final day of the date ranges
#' for which results are collected.
#' @slot user_group integer vector of user_ids to restrict results to. Cannot 
#' specify both user_group and user_group_query.
#' @slot user_group_query Character containing a SQL query that returns a
#' table with a single column, user_id. Cannot specify both user_group and
#' user_group_query.
#' @slot user_group_name Character, a name for the user group you're running 
#' the query on.
#' @slot query_prototype A character. Represents the SQL query that will return
#' the desired results, but the date filters are general, and need to be subbed
#' in using the 'substitute_dates' method.
#' @slot query A character. The actual query to run in Redshift.
#' @slot raw_results A data frame. The results of running "query".
#' @slot final_results A data frame. The formatted results that are ready to be
#' bound to all other query results.
setClass(
  Class = "FlashReportQuery"
  , slots = c(
      min_date = "Date"
      , max_date = "Date"
      , user_group = "integer"
      , user_group_query = "character"
      , user_group_name = "character"
      , query_prototype = "character"
      , query = "character"
      , raw_results = "data.frame"
      , final_results = "data.frame"
    )
  , prototype = list(min_date = as.Date("2016-01-01"))
) 
#' An S4 class to represent an 'active users' Flash Report query.
#'
#' @slot range_type A character indicating whether to use a year-to-date range
#' 'ytd', or a weekly range, 'week'.
setClass(
  Class = "auQuery"
  , slots = c(range_type = "character")
  , contains = "FlashReportQuery"
)

#' An S4 class to represent a 'platform action' Flash Report query.
#'
#' @slot range_type A character indicating whether to use a year-to-date range
#' 'ytd', or a weekly range, 'week'.
setClass(
  Class = "paQuery"
  , slots = c(range_type = "character")
  , contains = "FlashReportQuery"
)

#' An S4 class to represent a 'notifications' Flash Report query.
#'
#' @slot range_type A character indicating whether to use a year-to-date range
#' 'ytd', or a weekly range, 'week'.
setClass(
  Class = "notificationsQuery"
  , slots = c(range_type = "character")
  , contains = "FlashReportQuery"
)

#' A generic function to generate the correct min_date given the max_date and
#' range_type of an object that inherits from FlashReportQuery.
#'
#' @param frq An object that inherits from FlashReportQuery.
#' @return A FlashReportQuery object of the same subtype that was input.
#' @export
get_min_date <- function(frq) 0
setGeneric("get_min_date")

#' @describeIn get_min_date Get the min date of an object that inherits from
#' FlashReportQuery which only has a max date and a range_type defined.
setMethod("get_min_date"
          , signature(frq = "FlashReportQuery")
          , definition = function(frq){
            if(frq@range_type == 'week'){
              minDate <- frq@max_date - 6
            } else {
              minDate <- as.Date('2016-01-01')
            }
            frq@min_date <- minDate
            return(frq)
          }
)                     

#' A generic function to grab the correct query prototype and stick it into the
#' query_prototype slot of a FlashReport query.
#' 
#' @param frq An object that inherits from FlashReportQuery.
#' @return A FlashReportQuery object of the same subtype that was input.
#' @export
get_prototype <- function(frq) 0
setGeneric("get_prototype")

#' @describeIn get_prototype Get the query prototype for an active users query.
setMethod("get_prototype"
          , signature(frq = "auQuery")
          , definition = function(frq){
            user_group_exists <- length(frq@user_group) != 0
            user_group_query_exists <- length(frq@user_group_query) != 0
            user_group_name_exists <- length(frq@user_group_name) != 0
            if (!user_group_exists
                & !user_group_query_exists
                & !user_group_name_exists){
              frq@query_prototype <- 
                flashreport::query_prototype_list$auPrototype
            } else if (user_group_exists 
                       & user_group_query_exists){
              stop("Must specify at most one of user_group or user_group_query.")
            } else if (!user_group_exists
                       & !user_group_query_exists
                       & user_group_name_exists){
              warning("Did you mean to specify a user group?")
              frq@query_prototype <- 
                flashreport::query_prototype_list$auPrototype
            } else if ((user_group_exists
                       | user_group_query_exists)
                       & !user_group_name_exists){
              stop("Must specify a name for your user group.")
            } else {
              if (user_group_exists) {
                frq@user_group_query <- 
                  paste0(
                    "SELECT id as user_id FROM user_dimensions WHERE id IN ("
                    , paste(frq@user_group, collapse = ",")
                    , ")"
                  )
              }
              frq@query_prototype <- 
                flashreport::query_prototype_list$auCustomPrototype
            }
            return(frq)
          })

#' @describeIn get_prototype Get the query prototype for a platform actions 
#' query.
setMethod("get_prototype"
          , signature(frq = "paQuery")
          , definition = function(frq){
            user_group_exists <- length(frq@user_group) != 0
            user_group_query_exists <- length(frq@user_group_query) != 0
            user_group_name_exists <- length(frq@user_group_name) != 0
            if (!user_group_exists
                & !user_group_query_exists
                & !user_group_name_exists){
              frq@query_prototype <- 
                flashreport::query_prototype_list$paPrototype
            } else if (user_group_exists 
                       & user_group_query_exists){
              stop("Must specify at most one of user_group or user_group_query.")
            } else if (!user_group_exists
                       & !user_group_query_exists
                       & user_group_name_exists){
              warning("Did you mean to specify a user group?")
              frq@query_prototype <- 
                flashreport::query_prototype_list$paPrototype
            } else if ((user_group_exists
                       | user_group_query_exists)
                       & !user_group_name_exists){
              stop("Must specify a name for your user group.")
            } else {
              if (user_group_exists) {
                frq@user_group_query <- 
                  paste0(
                    "SELECT id as user_id FROM user_dimensions WHERE id IN ("
                    , paste(frq@user_group, collapse = ",")
                    , ")"
                  )
              }
              frq@query_prototype <- 
                flashreport::query_prototype_list$paCustomPrototype
            }
            return(frq)
          })

#' @describeIn get_prototype Get the query prototype for a notifications query.
setMethod("get_prototype"
          , signature(frq = "notificationsQuery")
          , definition = function(frq){
            user_group_exists <- length(frq@user_group) != 0
            user_group_query_exists <- length(frq@user_group_query) != 0
            user_group_name_exists <- length(frq@user_group_name) != 0
            if (!user_group_exists
                & !user_group_query_exists
                & !user_group_name_exists){
              frq@query_prototype <- 
                flashreport::query_prototype_list$notificationsPrototype
            } else if (user_group_exists 
                       & user_group_query_exists){
              stop("Must specify at most one of user_group or user_group_query.")
            } else if (!user_group_exists
                       & !user_group_query_exists
                       & user_group_name_exists){
              warning("Did you mean to specify a user group?")
              frq@query_prototype <- 
                flashreport::query_prototype_list$notificationsPrototype
            } else if ((user_group_exists
                       | user_group_query_exists)
                       & !user_group_name_exists){
              stop("Must specify a name for your user group.")
            } else {
              if (user_group_exists) {
                frq@user_group_query <- 
                  paste0(
                    "SELECT id as user_id FROM user_dimensions WHERE id IN ("
                    , paste(frq@user_group, collapse = ",")
                    , ")"
                  )
              }
              frq@query_prototype <- 
                flashreport::query_prototype_list$notificationsCustomPrototype
            }
            return(frq)
          })

#' A generic function to substitute the correct dates into the query_prototype
#' slot of a FlashReportQuery object, and return the result in the query slot.
#'
#' @param frq A FlashReportQuery object.
#' @return A FlashReportQuery object.
#' @export
substitute_dates <- function(frq) 0
setGeneric("substitute_dates")

#' @describeIn substitute_dates Substitute the min_date and max_date slots into
#' the query_prototype slot. Return result in the query slot.
setMethod("substitute_dates"
          , signature(frq = "FlashReportQuery")
          , definition = function(frq){
            qp <- frq@query_prototype
            numeric_dates <- gsub(pattern = "-"
                                     , replacement = ""
                                     , x = c(frq@min_date, frq@max_date))
            actual_query_0 <- gsub(pattern = "min_date_xyz"
                                 , replacement = min(numeric_dates)
                                 , x = qp)
            actual_query <-  gsub(pattern = "max_date_xyz"
                                 , replacement = max(numeric_dates)
                                 , x = actual_query_0)
            frq@query <- actual_query
            return(frq)
          })

#' A generic function to substitute the correct user_group_query and
#' user_group_name into the query slot of a FlashReportQuery object
#' , and return the result in the query slot.
#'
#' @param frq A FlashReportQuery object.
#' @return A FlashReportQuery object.
#' @export
substitute_user_group_name <- function(frq) 0
setGeneric("substitute_user_group_name")

#' @describeIn substitute_user_group_name Substitute the user_group and 
#' user_group_names into the query slot. Return result in the query slot.
setMethod("substitute_user_group_name"
          , signature(frq = "FlashReportQuery")
          , definition = function(frq){
            user_group_query_exists <- length(frq@user_group_query) != 0
            user_group_name_exists <- length(frq@user_group_name) != 0
            if (user_group_query_exists
                & !user_group_name_exists) {
              stop("Must specify a name for your user group.")
            } else if (!user_group_query_exists
                       & user_group_name_exists){
              warning("Did you mean to specify a user group?")
            } else if (user_group_query_exists
                       & user_group_name_exists){
              current_query <- frq@query
              query_with_user_group_name <- 
                gsub(pattern = "xyz_user_group_name_xyz"
                     , replacement = paste0("'", frq@user_group_name, "'")
                     , x = current_query)
              query_with_user_group <- 
                gsub(pattern = "xyz_user_group_query_xyz"
                     , replacement = frq@user_group_query
                     , x = query_with_user_group_name)
              frq@query <- query_with_user_group
            }
            return(frq)
          })

#' A generic function to run the query in the query slot of a FlashReportQuery
#' object, and return the results in the raw_results slot.
#'
#' @param frq A FlashReportQuery object.
#' @param ... Placeholder for database connection object.
#' @return A FlashReportQuery object.
#' @import RPostgreSQL DBI
#' @export
run_query <- function(frq, ...) 0
setGeneric("run_query")

#' @describeIn run_query Run the query slot against the redshift database and
#' return the results.
#'
#' @param con The database connection object to run the query through. Defaults
#' to insightsdev.
setMethod("run_query"
          , signature(frq = "FlashReportQuery")
          , definition = function(frq, con = redshift_connection$con){
            frq@raw_results <- RPostgreSQL::dbGetQuery(conn = con, frq@query)
            return(frq)
          })

#' A generic function to format the raw_results slot of a FlashReportQuery
#' object into a standardized format that can be rbinded with all other
#' results.
#'
#' @param frq An object that inherits from FlashReportQuery.
#' @return A FlashReportQuery object of the same subtype that was input. The
#' final_results slot contains a data.frame with columns (user_group,
#' date_range, variable, value).
#' @importFrom magrittr %>%
#' @export
format_raw_results <- function(frq) 0
setGeneric("format_raw_results")

#' @describeIn format_raw_results Format results of an Active Users query.
setMethod("format_raw_results"
          , signature(frq = "auQuery")
          , definition = function(frq){
            finals <- frq@raw_results %>%
              dplyr::rename(user_group = flash_report_category
                            , value = count) %>%
              dplyr::mutate(
                variable = paste0("active_users_", frq@range_type)
                , date_range = frq@max_date
              ) %>%
              dplyr::select(user_group, date_range, variable, value)
            frq@final_results <- finals
            return(frq) 
          })

#' @describeIn format_raw_results Format results of a platform action query.
setMethod("format_raw_results"
          , signature(frq = "paQuery")
          , definition = function(frq){
            finals <- frq@raw_results %>%
              dplyr::rename(user_group = user_cat
                            , value = count) %>%
              dplyr::mutate(
                variable = paste0("platform_actions_", pa_cat)
                , date_range = frq@max_date 
              ) %>%
              dplyr::select(user_group, date_range, variable, value)
            frq@final_results <- finals
            if(frq@range_type == 'week'){
              return(frq) 
            } else { 
              frq@final_results <- data.frame()
              return(frq) 
            }
          })

#' @describeIn format_raw_results Format results of a notifications query.
setMethod("format_raw_results"
          , signature(frq = "notificationsQuery")
          , definition = function(frq){
            finals <- frq@raw_results %>%
              dplyr::rename(user_group = user_cat
                            , value = count) %>%
              dplyr::mutate(
                date_range = frq@max_date
                , variable = paste0("notifications_", status)
              ) %>%
              dplyr::select(user_group, date_range, variable, value)
            frq@final_results <- finals
            if(frq@range_type == 'week'){
              return(frq) 
            } else { 
              frq@final_results <- data.frame()
              return(frq) 
            }
          })
