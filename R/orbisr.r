## Tools for working with Orbis bulk data
## ================================================================================



## --------------------------------------------------------------------------------
## Load or Install Packages (for testing)
## --------------------------------------------------------------------------------
## for(pkg in c('pbapply'
##            , "stringi"
##            , 'stringr'
##            , 'data.table'
##            , 'dplyr'
##            , 'magrittr'
##            , "XML"
##            , "xml2"
##            , "readr"))
##     if(!require(pkg, character.only = TRUE)) {
##         install.packages(pkg, repos = 'http://cloud.r-project.org')
##         library(pkg, character.only = TRUE)
##     }
## --------------------------------------------------------------------------------



## Define functions
## ================================================================================

#' Filter tables of Orbis bulk data
#'
#' @description
#' Similar to dplyr::filter but for tables of Orbis bulk data saved in multiple .rds files
#' @param orbis.data.path A path to directory with .rds files containing Orbis specific table from Orbis Bulk Data.
#' @param ... A filtering conditions to fetch certain rows. (See dplyr::filter)
#' @return A data.table with a subset of a table from Orbis Bulk Data.
#' @import pbapply magrittr data.table
#' @export
#' @examples
#' none yet...
#' @md
orbis.data.filter <- function(orbis.data.path, ...) {
    orbis.data.path %>%
        file.path(list.files(.)) %>%
        ## extract(8) %>% 
        pblapply(function(orbis.data.file.path)
            orbis.data.file.path %>%
            readRDS %>% 
            filter(...)) %>%
        rbindlist %>% 
        return
}



