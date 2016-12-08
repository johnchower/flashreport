starttime <- Sys.time()
glootility::connect_to_redshift()

library(RPostgreSQL)
library(glootility)
library(flashreport)
library(dplyr)
library(reshape2)

dbSendQuery(redshift_connection$con, 
  query_user_flash_cat
)

dbSendQuery(redshift_connection$con,
  query_pa_flash_cat
)

# dbSendQuery(redshift_connection$con,
#   query_subaggregates 
# )

run_date <- as.Date('2016-12-02')
weeks_back <- 1:46
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
      get_min_date %>%
      get_prototype %>%
      substitute_dates %>%
      run_query %>%
      format_raw_results %>%
      {.@final_results}
    
    long_flash_report <- rbind(long_flash_report, results)
  }
}


long_flash_report_2 <- long_flash_report %>%
  mutate(
    date_range =
      gsub(pattern = "-", replacement = "/",
        as.character(date_range)
      )
  ) %>%
  filter(
    !is.na(user_group)
    , user_group != "Uncategorized"
    , !is.null(user_group)
    , user_group != "Guest"
    , variable != "platform_actions_NA"
    , variable != "platform_actions_Uncategorized"
  ) %>%
  dplyr::select(user_group, date_range, variable, value)

long_flash_report_subaggregate <- long_flash_report_2 %>%
  dplyr::left_join(flashreport::subaggregates
                   , by = "user_group") %>%
  dplyr::group_by(subaggregate, date_range, variable) %>%
  dplyr::summarise(value = sum(value)) %>%
  dplyr::rename(user_group = subaggregate) %>%
  dplyr::select(user_group, date_range, variable, value) %>%
  ungroup

long_flash_report_aggregate <- long_flash_report_2 %>%
  dplyr::group_by(date_range, variable) %>%
  dplyr::summarise(value = sum(value)) %>%
  dplyr::mutate(user_group = "Total") %>%
  dplyr::select(user_group, date_range, variable, value) %>%
  ungroup

long_flash_report_3 <- rbind(long_flash_report_2
                                 , long_flash_report_subaggregate
                                 , long_flash_report_aggregate)

long_flash_report_WAU_pct <- long_flash_report_3 %>%
  filter(grepl(pattern = "active_user", x = variable)) %>%
  dcast(date_range + user_group ~ variable, value.var = 'value') %>%
  mutate(active_users_WAU_pct = active_users_week/active_users_ytd) %>%
  melt(id.vars = c("date_range", "user_group")
       , variable.name = "variable"
       , value.name = "value") %>%
  filter(variable == "active_users_WAU_pct")

long_flash_report_total_actions <- long_flash_report_3 %>%
  filter(grepl(pattern = "platform_actions", x = variable)) %>%
  group_by(date_range, user_group) %>%
  summarise(value = sum(value)) %>%
  mutate(variable = "platform_actions_total") %>%
  ungroup

long_flash_report_actions_per_AU <-
  rbind(long_flash_report_total_actions
        , long_flash_report_3) %>%
  filter(variable == "platform_actions_total"
         | variable == "active_users_week") %>%
  dcast(date_range + user_group ~ variable, value.var = 'value') %>%
  mutate(platform_actions_per_active_user = platform_actions_total/active_users_week) %>%
  melt(id.vars = c("date_range", "user_group")
       , variable.name = "variable"
       , value.name = "value") %>%
  filter(variable == "platform_actions_per_active_user")

long_flash_report_NRR <- long_flash_report_3 %>%
  filter(grepl(pattern = "notifications", x = variable)) %>%
  group_by(date_range, user_group) %>%
  mutate(notifications_events_total = sum(value)) %>%
  filter(variable == "notifications_Read") %>%
  ungroup %>%
  mutate(notifications_response_rate = value/notifications_events_total) %>%
  select(user_group, date_range, value = notifications_response_rate) %>%
  mutate(variable = "notifications_response_rate")

long_flash_report_final <- rbind(long_flash_report_3
                                 , long_flash_report_WAU_pct
                                 , long_flash_report_total_actions
                                 , long_flash_report_actions_per_AU
                                 , long_flash_report_NRR)

.env$view(long_flash_report_final)

dbDisconnect(redshift_connection$con)
endtime <- Sys.time()
endtime - starttime
