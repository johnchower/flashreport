#!/Users/johnhower/anaconda/envs/flashReportV1/bin Rscript
args_in <- commandArgs(trailingOnly=T)

library(RPostgreSQL)

# Parse arguments

option_list <- flashreport::define_option_list()
opt_parser <- optparse::OptionParser(option_list)
opt <- optparse::parse_args(opt_parser)

# Connect to redshift
 
glootility::connect_to_redshift()

# driver <- DBI::dbDriver("PostgreSQL")
 
# connection <- RPostgreSQL::dbConnect(
#                 driver
#                 , dbname = 'insightsbeta'
#                 , host = opt$host
#                 , port = opt$port
#                 , user = opt$user
#                 , password = opt$pass
#               ) 
 
# assign("redshift_connection"
#        , list(drv = driver, con = connection)
#        , envir = .GlobalEnv)

# Define temporary tables that future queries will use.

dbSendQuery(redshift_connection$con, 
  flashreport::query_user_flash_cat
)

dbSendQuery(redshift_connection$con,
  flashreport::query_pa_flash_cat
)

# Define date ranges and query types to get results for.

run_date <- as.Date(opt$rundate)
weeks_back <- as.numeric(opt$minweek):as.numeric(opt$maxweek)
start_dates <- run_date - 7*weeks_back
end_dates <- start_dates + 6
year_beginning <- as.Date(opt$yearbeginning)

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

# Run queries and put results into a long data frame.
long_flash_report <- flashreport::get_results(date_ranges, query_types)

# Postprocess results.
long_flash_report_dates_formatted <- 
  flashreport::format_LFR_dates(long_flash_report ) 

long_flash_report_2 <- 
  flashreport::curate_user_groups(long_flash_report_dates_formatted)

long_flash_report_subaggregate <- 
  flashreport::summarise_by_subaggregate(long_flash_report_2)

long_flash_report_aggregate <- 
  flashreport::summarise_in_aggregate(long_flash_report_2)

long_flash_report_3 <- rbind(long_flash_report_2
                                 , long_flash_report_subaggregate
                                 , long_flash_report_aggregate)

# Calculate WAU percentage for each user group, subaggregate, and aggregate,
# and for each date range.

long_flash_report_WAU_pct <- 
  flashreport::calculate_WAU_percentage(long_flash_report_3)

# Calculate total actions for each user group, subaggregate, and aggregate,
# and for each date range.

long_flash_report_total_actions <- 
  flashreport::calculate_total_actions(long_flash_report_3)

# Calculate average actions per WAU for each user group, subaggregate, and aggregate,
# and for each date range.

long_flash_report_actions_per_AU <-
  flashreport::calculate_actions_per_AU(
    long_flash_report_3
    , long_flash_report_total_actions
  )

# Calculate notifications_response_rate for each user group, subaggregate, and aggregate,
# and for each date range.

long_flash_report_NRR <- 
  flashreport::calculate_NRR(long_flash_report_3)

long_flash_report_final <- rbind(long_flash_report_3
                                 , long_flash_report_WAU_pct
                                 , long_flash_report_total_actions
                                 , long_flash_report_actions_per_AU
                                 , long_flash_report_NRR)

write.csv(long_flash_report_final
          , file = paste0(opt$outloc, "/", opt$outname, ".csv")
          , row.names = F)
dbDisconnect(redshift_connection$con)
