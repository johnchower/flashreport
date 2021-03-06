% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/processing_functions.r
\name{summarise_by_isFL}
\alias{summarise_by_isFL}
\title{Summarises results by isFL.}
\usage{
summarise_by_isFL(long_flash_report_2, isFL_df = flashreport::isFL)
}
\arguments{
\item{long_flash_report_2}{The result of calling curate_user_groups.}

\item{isFL_df}{A data frame that matches each user group to an
appropriate isFL (content champions, enterprise champions, other
champions, and internal users).}
}
\value{
A summarised version of long_flash_report_2. Same column structure,
but fewer rows.
}
\description{
Summarises results by isFL.
}

