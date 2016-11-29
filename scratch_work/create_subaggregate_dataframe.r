subaggregates <- read.csv(file = '../scratch_work/champion_subaggregates.csv'
                          , stringsAsFactors = F)

devtools::use_data(subaggregates, overwrite = T)
