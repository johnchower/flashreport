library(RPostgreSQL)

# Parse arguments

optionList <-   list(
    optparse::make_option(
      opt_str =  '--host'
      , type = 'character'
      , default = 'localhost'
      , help = 'Hostname for database connection'
    ) ,
    optparse::make_option(
      opt_str = '--port'
      , type = 'character'
      , default = '5441'
      , help = 'Port for database connection'
    ) ,
    optparse::make_option(
      opt_str = '--user'
      , type = 'character'
      , default = NULL
      , help = 'User name for database connection'
    ) ,
    optparse::make_option(
      opt_str = '--pass'
      , type = 'character'
      , default = NULL
      , help = 'Password for database connection'
    ) ,
    optparse::make_option(
      opt_str = '--rundate'
      , type = 'character'
      , default = as.character(Sys.Date())
      , help = 'The most recent date to include in the analysis. 
        Must be entered in the form yyyy-mm-dd. Defaults to current date.'
    ) ,
    optparse::make_option(
      opt_str = '--minweek'
      , type = 'integer'
      , default = 1
      , help = 'The latest week to include in the analysis. 
      If set to 1, then the most recent week in the analysis will be the week 
      preceding the rundate (not inclusive). If set to 2, then the most recent 
      week in the analysis will be the week before the week preceding the rundate. 
      [default = %default]'
    ) ,
    optparse::make_option(
      opt_str = '--maxweek'
      , type = 'integer'
      , default = 1
      , help = 'The earliest week to include in the analysis.
      Works the same way as minweek. Together, rundate, minweek, 
      and maxweek determine the overall date range reported in the results. 
      For example, rundate = 2016-12-09, minweek = 1, maxweek = 2 will give 
      results for the weeks (2016-12-02 - 2016-12-08) and (2016-11-25 - 2016-12-07). 
      [default = %default]'
    ) ,
    optparse::make_option(
      opt_str = '--yearbeginning'
      , type = 'character'
      , default = '2016-01-01'
      , help = 'User name for database connection'
    ) ,
    optparse::make_option(
      opt_str = '--outloc'
      , type = 'character'
      , default = NULL
      , help = 'Location to save the output. 
      Enter as /path/to/output not /path/to/output/'
    ) ,
    optparse::make_option(
      opt_str = '--outname'
      , type = 'character'
      , default = NULL
      , help = 'Name of output csv file. Enter as name_of_output not name_of_output.csv'
    ) 
  )
opt_parser <- optparse::OptionParser(option_list = optionList)
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
