gloo_internal_emails <- 
  print(
    paste0(
      readLines(con = '~/Projects/flashreport/scratch_work/internal_emails')
      , collapse = ""
    )
  )

devtools::use_data(gloo_internal_emails, overwrite = T)
