library(dplyr)
devtools::load_all()

query_pa_flash_cat <-
  paste0(flashreport::query_pa_flash_cat_insert
         , ";"
         , gsub(pattern = "xyz_pa_classes_xyz"
              , replacement = flashreport::pa_classes
              , x = flashreport::query_pa_flash_cat_insert)
         , ";")

devtools::use_data(query_pa_flash_cat, overwrite = T)

query_user_flash_cat <-
  gsub(pattern = "xyz_gloo_internal_email_xyz"
       , replacement = flashreport::gloo_internal_emails
       , x = query_user_flash_cat_sub)

devtools::use_data(query_user_flash_cat, overwrite = T)
