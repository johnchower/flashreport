#' An S4 class to represent a Flash Report query.
#'
#' @slot min_date A Date object, representing the first day of the date ranges
#' for which results are collected.
#' @slot max_date A Date object, representing the final day of the date ranges
#' for which results are collected.
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
      , query_prototype = "character"
      , query = "character"
      , raw_results = "data.frame"
      , final_results = "data.frame"
    )
)

#' An S4 class to represent an 'active users' Flash Report query.
#'
#' @slot dummy A dummy slot to differentiate this class from the
#' FlashReportQuery class.
setClass(
  Class = "auQuery"
  , slots = c(dummy = "character")
  , contains = "FlashReportQuery"
)

#' An S4 class to represent a 'platform action' Flash Report query.
#'
#' @slot dummy A dummy slot to differentiate this class from the
#' FlashReportQuery class.
setClass(
  Class = "paQuery"
  , slots = c(dummy = "character")
  , contains = "FlashReportQuery"
)


#' An S4 class to represent a 'notifications' Flash Report query.
#'
#' @slot dummy A dummy slot to differentiate this class from the
#' FlashReportQuery class.
setClass(
  Class = "notificationsQuery"
  , slots = c(dummy = "character")
  , contains = "FlashReportQuery"
)

#' A generic function to grab the correct query prototype and stick it into the
#' query_prototype slot of a FlashReport query.
#' 
#' @param frq An object that inherits from FlashReportQuery.
#' @return A FlashReportQuery object of the same subtype that was input.
get_prototype <- function(frq) 0
setGeneric("get_prototype")

setMethod("get_prototype"
          , signature(frq = "auQuery")
          , definition = function(frq){
            frq@query_prototype <- flashreport::query_prototype_list$auPrototype
            return(frq)
          })

setMethod("get_prototype"
          , signature(frq = "paQuery")
          , definition = function(frq){
            frq@query_prototype <- flashreport::query_prototype_list$paPrototype
            return(frq)
          })

setMethod("get_prototype"
          , signature(frq = "notificationsQuery")
          , definition = function(frq){
            frq@query_prototype <- flashreport::query_prototype_list$notificationsPrototype
            return(frq)
          })

#' A generic function to substitute the correct dates into the query_prototype
#' slot of a FlashReportQuery object, and return the result in the query slot.
#'
#' @param frq A FlashReportQuery object.
#' @return A FlashReportQuery object.
substitute_dates <- function(frq) 0
setGeneric("substitute_dates")

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

#' A generic function to run the query in the query slot of a FlashReportQuery
#' object, and return the results in the raw_results slot.
#'
#' @param frq A FlashReportQuery object.
#' @param ... Placeholder for database connection object.
#' @return A FlashReportQuery object.
#' @import RPostgreSQL DBI
run_query <- function(frq, ...) 0
setGeneric("run_query")

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
format_raw_results <- function(frq) 0
setGeneric("format_raw_results")

setMethod("format_raw_results"
          , signature(frq = "auQuery")
          , definition = function(frq){
            finals <- frq@raw_results %>%
              dplyr::rename(user_group = flash_report_category
                            , value = count) %>%
              dplyr::mutate(variable = "active_users"
                            , date_range = paste(frq@min_date, "_", frq@max_date) ) %>%
              dplyr::select(user_group, date_range, variable, value)
            frq@final_results <- finals
            return(frq) 
          })

setMethod("format_raw_results"
          , signature(frq = "paQuery")
          , definition = function(frq){
            finals <- frq@raw_results %>%
              dplyr::rename(user_group = user_cat
                            , value = count) %>%
              dplyr::mutate(variable = pa_cat
                            , date_range = paste(frq@min_date, "_", frq@max_date) ) %>%
              dplyr::select(user_group, date_range, variable, value)
            frq@final_results <- finals
            return(frq) 
          })

setMethod("format_raw_results"
          , signature(frq = "notificationsQuery")
          , definition = function(frq){
            finals <- frq@raw_results %>%
              dplyr::rename(user_group = user_cat
                            , variable = status
                            , value = count) %>%
              dplyr::mutate(date_range = paste(frq@min_date, "_", frq@max_date) ) %>%
              dplyr::select(user_group, date_range, variable, value)
            frq@final_results <- finals
            return(frq) 
          })
