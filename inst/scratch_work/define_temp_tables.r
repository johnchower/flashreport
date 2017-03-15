library(dplyr)

query_pa_flash_cat <- 

  paste0("CREATE TEMPORARY TABLE pa_flash_cat 
            (platform_action VARCHAR 
            , flash_report_category VARCHAR);"
          , "INSERT INTO pa_flash_cat 
              (platform_action, flash_report_category)
              VALUES"
          , flashreport::pa_classes
          , ";"
  )

devtools::use_data(query_pa_flash_cat, overwrite = T)

query_user_flash_cat <- 
  gsub(pattern = "xyz_gloo_internal_email_xyz"
       , replacement = flashreport::gloo_internal_emails
       , x = query_user_flash_cat_sub)

devtools::use_data(query_user_flash_cat, overwrite = T)
