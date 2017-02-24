proj_root <- rprojroot::find_root(rprojroot::has_dirname('flashreport'))

path_to_data <- 'scratch_work/champion_subaggregates.csv'
full_path_to_data <- paste(proj_root, path_to_data, sep = '/')
subaggregates <- read.csv(file = full_path_to_data
                          , stringsAsFactors = F)
devtools::use_data(subaggregates
                   , overwrite = T
                   , pkg = proj_root)

path_to_data <- 'scratch_work/champion_isFL.csv'
full_path_to_data <- paste(proj_root, path_to_data, sep = '/')
isFL <- read.csv(file = full_path_to_data
                          , stringsAsFactors = F)
devtools::use_data(isFL
                   , overwrite = T
                   , pkg = proj_root)
