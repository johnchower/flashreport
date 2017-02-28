proj_root <- rprojroot::find_root(rprojroot::has_dirname("flashreport"))
path_to_data <- "scratch_work/internal_emails"
full_path_to_data <- paste(proj_root, path_to_data, sep = "/")

gloo_internal_emails <-
  print(
    paste0(
      readLines(con = full_path_to_data)
      , collapse = ""
    )
  )

devtools::use_data(gloo_internal_emails, overwrite = T)
