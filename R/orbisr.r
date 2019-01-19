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
##            ))
##     if(!require(pkg, character.only = TRUE)) {
##         install.packages(pkg, repos = 'http://cloud.r-project.org')
##         library(pkg, character.only = TRUE)
##     }
## --------------------------------------------------------------------------------






## ================================================================================
## Utilites functions
## ================================================================================

## Calculates number of lines a file has
get.file.nlines <- function(file.name, dir.path = getwd()) {
    file.name %>%
        file.path(dir.path, .) %>% 
        paste("grep -c $", .) %>%
        system(intern = TRUE) %>%
        as.numeric
}


is.0 <- function(x) length(x) == 0

## ================================================================================
## Main functions
## ================================================================================

## --------------------------------------------------------------------------------
#' Read orbis raw data table to many .rds files
#' 
#' @description
#' Designed to be run from the direcrory where the data is.
#' @param orbis.data.raw.file File name of raw Orbis data
#' @param orbis.data.path A path to directory with raw data. Default is working directory.
#' @param orbis.data.codes.file Name of the .csv file with codes. Default is the same as orbis.data.raw.file but with .csv
#' @param orbis.data.codes.dir A path to .csv file with codes. Default is ./orbis-var-names
#' @param orbis.select.fields A character vector with felds (code) to select. Default is all.
#' @param orbis.data.raw.nlines Number of lined in the raw data file. Default is calculate with grep
#' @param orbis.data.raw.skip.lines The header of raw data. The default is 2.
#' @param orbis.batch.nlines Number of lines to read in batch. The default is 10^7
#' @param orbis.batch.path Path for saving .rds files. The default is dir same as 'orbis.data.raw.file'
#' @return A vector of .rds file names
#' @import magrittr data.table stringr dplyr
#' @export
#' @examples
#' none yet...
#' @md
orbis.save.rds <- function(orbis.data.raw.file
                         , orbis.data.path = getwd()
                         , orbis.data.codes.file = character(0)
                         , orbis.data.codes.dir = file.path(orbis.data.path, "orbis-var-names")
                         , orbis.select.fields = character(0)
                         , orbis.data.raw.nlines = 0
                         , orbis.data.raw.skip.lines = 2
                         , orbis.batch.nlines = 10^7
                         , orbis.batch.path = character(0)
                           ) {
    ## Calculate number of lines a raw Orbis data file has
    if(orbis.data.raw.nlines == 0) {
        message("Calculating the lenght of raw Orbis data file...")
        orbis.data.raw.nlines <-
            get.file.nlines(file.name = orbis.data.raw.file
                          , dir.path = orbis.data.path)
        if(orbis.data.raw.nlines %>% is.0) {
            message("Can not get the length of raw Orbis data file.")
            message("Exiting funciton...")
            return() %>% invisible
        }
        message("The lenght of raw data file is "
              , orbis.data.raw.nlines
              , " lines.")
    }
    ## Get raw file name without extention
    orbis.data.raw.file.noext <-
        orbis.data.raw.file %>%
        str_replace("\\.txt$", "")
    ## Find codes for Orbis raw data table
    if(orbis.data.codes.file %>% is.0) {
        orbis.data.codes.file <-
            orbis.data.raw.file.noext %>%
            paste0(".csv")
    }
    ## Read codes for Orbis raw data table
    orbis.data.description <- 
        orbis.data.codes.file %>%
        file.path(orbis.data.codes.dir, .) %>% 
        read.csv(stringsAsFactors = FALSE) %>%
        filter(!is.na(var.name) & var.name != "",)
    if(orbis.select.fields %>% is.0) {
        orbis.select.fields <- orbis.data.description$var.name
    }
    orbis.data.description %<>%
        filter(var.name %in% orbis.select.fields)
    ## Set format for rds files numbering
    batch.file.format <- paste0("%0", nchar(orbis.data.raw.nlines), "d")
    ## Set start read rows for fread
    rows.skip <- seq(from = orbis.data.raw.skip.lines
                   , to = orbis.data.raw.nlines
                   , by = orbis.batch.nlines)
    rows.read <- rows.skip[-1] %>%
        c(orbis.data.raw.nlines) %>%
        '-'(rows.skip)
    ## write batches to .rds
    sapply(1:length(rows.read), function(i) {
        ## extract batch
        message("==============================================================================")
        message("Reading lines from ", rows.skip[i])
        started <- Sys.time()
        message("Started: ", date())
        orbis.data.batch <-
            orbis.data.raw.file %>% 
            file.path(orbis.data.path, .) %>% 
            fread(nrows = rows.read[i]
                , header = FALSE
                , skip = rows.skip[i]
                , showProgress = TRUE
                , select = orbis.data.description$col
                , strip.white = FALSE
                , quote = ""
                , sep = "\t"
                , stringsAsFactors = FALSE
                , colClasses = list(character = orbis.data.description$col))
        ## name columns
        names(orbis.data.batch) <- orbis.data.description$var.name
        ## save batch
        message("Batch loaded!")
        ## Make a dir for saving .rds
        if(orbis.batch.path %>% is.0) {
            orbis.batch.path <-
                orbis.data.raw.file.noext %>%
                paste0(".rds") %>% 
                file.path(orbis.data.path, .) %T>%
                dir.create(showWarnings = FALSE)
        }
        batch.file.path <- paste0(orbis.batch.path %>% file.path(orbis.data.raw.file.noext)
                                , "-"
                                , sprintf(batch.file.format, rows.skip[i] + 1), "-"  # add padding
                                , sprintf(batch.file.format, rows.skip[i] + rows.read[i])
                                , ".rds")
        message("Saving RDS: ", batch.file.path)
        saveRDS(orbis.data.batch, batch.file.path)
        message("Done! (in ", as.numeric(Sys.time() - started) %>% round, " minutes)")
        batch.file.path
    }) %>% return
}


## --------------------------------------------------------------------------------


## orbis.save.rds("key-financials-usd.txt", orbis.select.fields = c("emp", "rev"))


## orbis.save.rds("key-financials-usd.txt") %>% unlist


## orbis.save.rds("orbis-harmonize.r")

# --------------------------------------------------------------------------------
#' Unpacks Orbis RAR files.
#'
#' Assumes that it is running from the orbis data directory
#' @param rarfile File name to unrar. (without .rar extention)
#' @param rardir Default is in working directory "orbis-world-2017-09-13"
#' @param exdir A path where to extract zip file. Default is in working directory "patview-data-tsv"
#' @return Unzipped file path.
#' @import magrittr stringr
#' @export
#' @examples
#' none yet...
#' @md
orbis.unrar.file <- function(rarfile
                           , rardir = file.path(getwd(), "orbis-world-2017-09-13")
                           , exdir = getwd()) {
    file.name <- rarfile %>%
        basename %>% 
        str_replace_all("_", "-") %>%
        tolower
    if(str_detect(exdir %>% list.files, file.name) %>% any) {
        message("Seems lile file '", rarfile, ".rar' is already extracted. Exiting.")
    } else {
        rarfile %>%
            paste0(".rar") %>% 
            file.path(rardir,.) %>%
            normalizePath %>% 
            paste0('7z x -o"', exdir, '" "',.,'"') %>% 
            system
        message("File extracted.")
        rarfile %>%
            basename %>% 
            paste0(".txt") %>%
            file.rename(tolower(str_replace_all(.,"_", "-")))
    }
    file.name %>%
        paste0(".txt")
}



## --------------------------------------------------------------------------------
#' Filter tables of Orbis bulk data
#'
#' @description
#' Similar to dplyr::filter but for tables of Orbis bulk data saved in multiple .rds files
#' @param orbis.data.path A path to directory with .rds files containing Orbis specific table from Orbis Bulk Data.
#' @param ... A filtering conditions to fetch certain rows. (See dplyr::filter)
#' @param file.pattern A pattern for getting a file or a set of files (data batches)
#' @param cols Which column to select. Default is all columns.
#' @param progress.bar Whether to show progress bar (with pbapply package). Default is TRUE
#' @return A data.table with a subset of a table from Orbis Bulk Data.
#' @import pbapply magrittr data.table dplyr
#' @export
#' @examples
#' none yet...
#' @md
orbis.filter <- function(orbis.data.path
                       , ...
                       , file.pattern = NULL
                       , cols = character(0)
                       , progress.bar = TRUE) {
    if(progress.bar) {
        orbis.data.path %>%
            file.path(list.files(., pattern = file.pattern)) %>%
            pblapply(function(orbis.data.file.path)
                orbis.data.file.path %>%
                readRDS %>% 
                dplyr::filter(...) %>%
                dplyr::select(if(cols %>% is.0) everything() else cols)) %>%
            rbindlist(fill = TRUE) %>% 
            return
    } else {
        orbis.data.path %>%
            file.path(list.files(., pattern = file.pattern)) %>%
            lapply(function(orbis.data.file.path)
                orbis.data.file.path %>%
                readRDS %>% 
                dplyr::filter(...) %>%
                dplyr::select(if(cols %>% is.0) everything() else cols)) %>%
            rbindlist(fill = TRUE) %>% 
            return
    }}
## --------------------------------------------------------------------------------
