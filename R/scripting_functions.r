#' Defines the option list for the master script to call.
#'
#' @return A list with entries host, port, user, pass, rundate,
#' minweek, maxweek, yearbeginning, outloc, outname 
#' @import optparse
define_option_list <- function(){
  option_list <- list(
    optparse::make_option(
      opt_str =  '--host'
      , type = 'character'
      , default = 'localhost'
      , help = 'Hostname for database connection'
    )
    ,
    optparse::make_option(
      opt_str = '--port'
      , type = 'character'
      , default = '5441'
      , help = 'Port for database connection'
    )
    ,
    optparse::make_option(
      opt_str = '--user'
      , type = 'character'
      , default = NULL
      , help = 'User name for database connection'
    )
    ,
    optparse::make_option(
      opt_str = '--pass'
      , type = 'character'
      , default = NULL
      , help = 'Password for database connection'
    )
    ,
    optparse::make_option(
      opt_str = '--rundate'
      , type = 'character'
      , default = as.character(Sys.Date())
      , help = 'The most recent date to include in the analysis. Must be entered in the form yyyy-mm-dd. Defaults to current date.'
    )
    ,
    optparse::make_option(
      opt_str = '--minweek'
      , type = 'integer'
      , default = 1
      , help = 'The latest week to include in the analysis. If set to 1, then the most recent week in the analysis will be the week preceding the rundate (not inclusive.) If set to 2, then the most recent week in the analysis will be the week before the week preceding the rundate. [default = %default]'
    )
    ,
    optparse::make_option(
      opt_str = '--maxweek'
      , type = 'integer'
      , default = 1
      , help = 'The earliest week to include in the analysis. Works the same way as minweek. Together, rundate, minweek, and maxweek determine the overall date range reported in the results. For example, rundate = 2016-12-09, minweek = 1, maxweek = 2 will give results for the weeks (2016-12-02 - 2016-12-08) and (2016-11-25 - 2016-12-07). [default = %default]'
    )
    ,
    optparse::make_option(
      opt_str = '--yearbeginning'
      , type = 'character'
      , default = '2016-01-01'
      , help = 'User name for database connection'
    )
    ,
    optparse::make_option(
      opt_str = '--outloc'
      , type = 'character'
      , default = NULL
      , help = 'Location to save the output. Enter as /path/to/output not /path/to/output/'
    )
    ,
    optparse::make_option(
      opt_str = '--outname'
      , type = 'character'
      , default = NULL
      , help = 'Name of output csv file. Enter as name_of_output not name_of_output.csv'
    )
    ,
  )

  opt_parser = optparse::OptionParser(option_list)
  parse_args(opt_parser)
}
