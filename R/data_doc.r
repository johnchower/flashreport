#' A string containing all internal emails for Gloo employees.
#' 
#' @format A length-one character vector.
"gloo_internal_emails"

#' A string containing all (platform_action, flash_report_category) pairs. 
#'
#' @format A length-one character vector.
"pa_classes"

#' A data.frame containing all (user_group, subaggregate) pairs.
#'
#' @format A length-one character vector.
"subaggregates"

#' A data.frame containing all (user_group, isFL) pairs.
#'
#' @format A length-one character vector.
"isFL"


#' A list of strings containing all query prototypes.
#'
#' @format A list of strings
"query_prototype_list"

#' A generic query to create a temporary table in Redshift that matches users
#' to their Flash Report Categories.
#'
#' @format string
"query_user_flash_cat"

#' A generic query to create a temporary table in Redshift that matches
#' platform actions to their Flash Report Categories.
#'
#' @format string
"query_pa_flash_cat"
