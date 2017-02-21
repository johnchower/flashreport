#' A function to perform all queries and dump results into a long data.frame.
#'
#' @param date_ranges A data frame with columns range_types, max_dates where
#' each row represents a date range.
#' @param query_types A character vector describing the types of queries to
#' run. Appropriate entries correspond to the names of S4 query type classes
#' defined in R/query_functions.r (currently 'auQuery', 'paQuery',
#' 'notificationsQuery' are the only valid options).
#' @return A data frame with columns user_group (character), date_range (date
#' ending), variable (character), value (numeric).
#' @importFrom magrittr %>%
#' @importFrom methods new
#' @export
get_results <- function(date_ranges, query_types){
  long_flash_report <- data.frame()
  for(i in 1:nrow(date_ranges)){
    for(queryType in query_types){
      range_i <- date_ranges[i,]
      maxDate <- range_i$max_dates
      rangeType <- range_i$range_types
      FRQ <- new(Class = queryType
                 , max_date = maxDate
                 , range_type = rangeType)
      results <- FRQ %>%
        {flashreport::get_min_date(.)} %>%
        {flashreport::get_prototype(.)} %>%
        {flashreport::substitute_dates(.)} %>%
        {flashreport::run_query(.)} %>%
        {flashreport::format_raw_results(.)} %>%
        {.@final_results}
      
      long_flash_report <- rbind(long_flash_report, results)
    }
  }
  return(long_flash_report)
}

#' Formats dates in the results of get_results to a specified standard.
#' 
#' @param long_flash_report The result of calling get_results.
#' @return A data frame equivalent to the input, except that the dates are in a
#' different format.
#' @export
format_LFR_dates <- function(long_flash_report){
  dplyr::mutate(
    long_flash_report
    , date_range =
        gsub(pattern = "-", replacement = "/",
          as.character(date_range)
        )
  )
}

#' Removes undesireable user groups from the final report.
#'
#' @param long_flash_report_dates_formatted The result of calling
#' format_LFR_dates.
#' @return A data frame equivalent to the input, except that certain
#' user_groups have been filtered out.
#' @importFrom magrittr %>%
#' @export
curate_user_groups <- function(long_flash_report_dates_formatted){
  long_flash_report_dates_formatted %>%
    dplyr::filter(
      !is.na(user_group)
      , user_group != "Uncategorized"
      , !is.null(user_group)
      , user_group != "Guest"
      , variable != "platform_actions_NA"
      , variable != "platform_actions_Uncategorized"
    ) %>%
    dplyr::select(user_group, date_range, variable, value)
}

#' Summarises results by subaggregate.
#'
#' @param long_flash_report_2 The result of calling curate_user_groups.
#' @param subaggregate_df A data frame that matches each user group to an
#' appropriate subaggregate (content champions, enterprise champions, other
#' champions, and internal users).
#' @return A summarised version of long_flash_report_2. Same column structure,
#' but fewer rows.
#' @importFrom magrittr %>%
#' @export
summarise_by_subaggregate <- function(long_flash_report_2
                                      , subaggregate_df = flashreport::subaggregates){
  long_flash_report_2 %>%
    dplyr::left_join(subaggregate_df
                     , by = "user_group") %>%
    dplyr::group_by(subaggregate, date_range, variable) %>%
    dplyr::summarise(value = sum(value)) %>%
    dplyr::rename(user_group = subaggregate) %>%
    dplyr::select(user_group, date_range, variable, value) %>%
    {dplyr::ungroup(.)}
}

#' Summarises results in aggregate.
#'
#' @param long_flash_report_2 The result of calling curate_user_groups.
#' @return A summarised version of long_flash_report_2. Same column structure,
#' but fewer rows.
#' @importFrom magrittr %>%
#' @export
summarise_in_aggregate <- function(long_flash_report_2){
  long_flash_report_2 %>%
    dplyr::group_by(date_range, variable) %>%
    dplyr::summarise(value = sum(value)) %>%
    dplyr::mutate(user_group = "Total") %>%
    dplyr::select(user_group, date_range, variable, value) %>%
    {dplyr::ungroup(.)}
}

#' Calculate WAU percentage for each user group, subaggregate, and aggregate,
#' and for each date range.
#'
#' @param long_flash_report_3 The rbinded results of
#' summarise_by_subaggregate, summarise_in_aggregate, and curate_user_groups.
#' @return A data frame with the same column structure as the input, but with
#' additional rows corresponding to the WAU percent variable.
#' @importFrom magrittr %>%
#' @export
calculate_WAU_percentage <- function(long_flash_report_3){
  long_flash_report_3 %>%
    dplyr::filter(grepl(pattern = "active_user", x = variable)) %>%
    reshape2::dcast(date_range + user_group ~ variable
                    , value.var = 'value') %>%
    dplyr::mutate(
      active_users_WAU_pct = active_users_week/active_users_ytd
    ) %>%
    reshape2::melt(id.vars = c("date_range", "user_group")
         , variable.name = "variable"
         , value.name = "value") %>%
    dplyr::filter(variable == "active_users_WAU_pct")
}

#' Calculate total actions for each user group, subaggregate, and aggregate,
#' and for each date range.
#'
#' @param long_flash_report_3 The rbinded results of
#' summarise_by_subaggregate, summarise_in_aggregate, and curate_user_groups.
#' @return A data frame with the same column structure as the input, but with
#' additional rows corresponding to the total actions variable.
#' @importFrom magrittr %>%
#' @export
calculate_total_actions <- function(long_flash_report_3){
  long_flash_report_3 %>%
    dplyr::filter(grepl(pattern = "platform_actions", x = variable)) %>%
    dplyr::group_by(date_range, user_group) %>%
    dplyr::summarise(value = sum(value)) %>%
    dplyr::mutate(variable = "platform_actions_total") %>%
    {dplyr::ungroup(.)}
}

#' Calculate average actions per WAU for each user group, subaggregate, 
#' and aggregate, and for each date range.
#'
#' @param long_flash_report_3 The rbinded results of
#' summarise_by_subaggregate, summarise_in_aggregate, and curate_user_groups.
#' @param long_flash_report_total_actions The result of calling
#' calculate_total_actions.
#' @return A data frame with the same column structure as the input, but with
#' additional rows corresponding to the actions_per_AU variable.
#' @importFrom magrittr %>%
#' @export
calculate_actions_per_AU <- function(long_flash_report_3
                                     , long_flash_report_total_actions){
  rbind(long_flash_report_total_actions
          , long_flash_report_3) %>%
    dplyr::filter(variable == "platform_actions_total"
           | variable == "active_users_week") %>%
    reshape2::dcast(date_range + user_group ~ variable
                    , value.var = 'value') %>%
    dplyr::mutate(
      platform_actions_per_active_user 
        = platform_actions_total/active_users_week
    ) %>%
    reshape2::melt(id.vars = c("date_range", "user_group")
         , variable.name = "variable"
         , value.name = "value") %>%
    dplyr::filter(variable == "platform_actions_per_active_user")
}

#' Calculate notifications_response_rate for each user group, subaggregate, and aggregate,
#' and for each date range.
#'
#' @param long_flash_report_3 The rbinded results of
#' summarise_by_subaggregate, summarise_in_aggregate, and curate_user_groups.
#' @return A data frame with the same column structure as the input, but with
#' additional rows corresponding to the notifications_response_rate variable.
#' @importFrom magrittr %>%
#' @export
calculate_NRR <- function(long_flash_report_3){
  long_flash_report_3 %>%
    dplyr::filter(grepl(pattern = "notifications", x = variable)) %>%
    dplyr::group_by(date_range, user_group) %>%
    dplyr::mutate(notifications_events_total = sum(value)) %>%
    dplyr::filter(variable == "notifications_Read") %>%
    {dplyr::ungroup(.)} %>%
    dplyr::mutate(notifications_response_rate = value/notifications_events_total) %>%
    dplyr::select(user_group, date_range, value = notifications_response_rate) %>%
    dplyr::mutate(variable = "notifications_response_rate")
}
