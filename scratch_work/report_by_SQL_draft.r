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

run_date <- as.Date('2016-11-25')
weeks_back <- c(1,2,4,6)
start_dates <- run_date - 7*weeks_back
end_dates <- start_dates + 6
year_beginning <- as.Date('2016-01-01')

date_ranges <- data.frame(
  names = c(paste0('wk', weeks_back),
             paste0('ytd', weeks_back))
  , range_beginning = c(start_dates
                       , rep(year_beginning, times = length(weeks_back)))
  , range_ending  = rep(end_dates, times = 2)
  , stringsAsFactors = F
)

query_types <- paste0(c('au', 'pa', 'notifications'), 'Query')

long_flash_report <- data.frame()
for(i in 1:nrow(date_ranges)){
  for(queryType in query_types){
    range_i <- date_ranges[i,]
    minDate <- range_i$range_beginning
    maxDate <- range_i$range_ending
    range_type <- ifelse(as.numeric(maxDate - minDate) == 6 
                         , 'week'
                         , 'ytd')
    FRQ <- new(Class = queryType
               , min_date = minDate
               , max_date = maxDate)
    results <- FRQ %>%
      get_prototype %>%
      substitute_dates %>%
      run_query %>%
      format_raw_results %>%
      {.@final_results}
    
    long_flash_report <- rbind(long_flash_report, results)
  }
}

short_flash_report <- long_flash_report %>%
  dcast(user_group ~ date_range + variable, value.var = 'value')

end_dates <- seq.Date(from = as.Date('2016-01-07')
                      , to = as.Date('2016-11-24')
                      , by = 7)
week_start_dates <- end_dates - 6

yearly_WAU_pcts <- data.frame()
for(i in 1:length(end_dates)){
  endDate <- end_dates[i]
  weekStartDate <- week_start_dates[i]

  week_query <- new("auQuery"
                    , min_date = weekStartDate
                    , max_date = endDate)

  year_query <- new("auQuery"
                    , min_date = year_beginning
                    , max_date = endDate)

  WAU_count <- week_query %>%
      get_prototype %>%
      substitute_dates %>%
      run_query %>%
      format_raw_results %>%
      {.@final_results} %>%
      filter(!is.na(user_group)
             , user_group != "Uncategorized"
             , user_group != "Guest") %>%
      {.$value} %>%
      sum

  AU_count <- year_query %>%
      get_prototype %>%
      substitute_dates %>%
      run_query %>%
      format_raw_results %>%
      {.@final_results} %>%
      filter(!is.na(user_group)
             , user_group != "Uncategorized"
             , user_group != "Guest") %>%
      {.$value} %>%
      sum
  result <- data.frame(WAU_pct = WAU_count/AU_count
                       , end_date = endDate
                       , stringsAsFactors = F)
  yearly_WAU_pcts <- rbind(yearly_WAU_pcts, result)
}    

dbDisconnect(redshift_connection$con)
