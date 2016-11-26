# platform_action_categories <- 
pa_classes <- 
  paste0(
    readLines('~/Projects/flashreport/scratch_work/pa_classes')
    , collapse = ""
  ) 

devtools::use_data(pa_classes, overwrite = T)
