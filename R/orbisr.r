#' Tools for working with Orbis bulk data
#'
#' ================================================================================
#'
#' The only functions you're likely to need from \pkg{orbisr} for deployment of Orbis
#' database are \code{\link{orbis.save.rds}} and \code{\link{orbis.unrar.txt}}.
#' 
#' For working with deploied Orbis database you will need only \code{\link{orbis.filter}}
"_PACKAGE"

## ================================================================================
## Utilites functions
## ================================================================================

## Calculates number of lines a file has
get.file.nlines <- function(file.name
                          , dir.path = getwd()
                          , command = "grep -c $") {
  ## check if the programm (first word in command) is available
  if(Sys.which(str_extract(command, "^[^\\s]+")) != "") {
    file.name %>%
      file.path(dir.path, .) %>% 
      paste(command, .) %>%
      system(intern = TRUE) %>%
      as.numeric
  } else message("Cannot find grep programm. Consider installing grep first")
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
#' @param orbis.harmonize.cols Which columns to harmonize. (Requires harmonizer package.)
#' @param harmonizer.progress.by (Requires harmonizer package.) Numeric value that is used to split the org.names vector for showing percentage of completion. Default is 0 meaning not to split the vector and thus does not show progress percentage. Designed to be used for long strings.
#' @param harmonizer.quite (Requires harmonizer package.) Logical value indicating whether or not print messages about procedures progress.
#' @param harmonizer.procedures (Requires harmonizer package.) List of harmonization procedures. Each procedure can be specified as a string representing procedure name (see details for procedure names) or as a list where the first element should be procedure name (string) and other elements will passed as arguments to this procedure.
#' @return A vector of .rds file names.
#' @import magrittr data.table stringr dplyr harmonizer
#' @export
#' @md
orbis.save.rds <- function(orbis.data.raw.file
                         , orbis.data.path = getwd()
                         , orbis.data.codes.file = character(0)
                         , orbis.data.codes.dir = 
                             file.path(getwd(), "orbis-var-names")
                         , orbis.select.fields = character(0)
                         , orbis.data.raw.nlines = NA
                         , orbis.data.raw.skip.lines = 2
                         , orbis.batch.nlines = 10^7
                         , orbis.batch.path = character(0)
                         , batch.file.path.prefix = ""
                         , batch.file.path.sufix = ""
                         , save.rds = TRUE
                         , return.invisible = FALSE
                         , orbis.harmonize.cols = character(0)
                         , harmonizer.progress.by = 10^5
                         , harmonizer.quite = FALSE
                         , harmonizer.procedures = 
                             list(list("toascii", FALSE)
                                , "remove.brackets"
                                , "toupper"
                                , "apply.nber"
                                , "remove.spaces")
                           ) {
  ## Calculate number of lines a raw Orbis data file has
  if(is.na(orbis.data.raw.nlines) & orbis.batch.nlines != Inf) {
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
  ## Set start read rows for fread
rows.skip <-
  if(orbis.batch.nlines != Inf ) {
    seq(from = orbis.data.raw.skip.lines
      , to = orbis.data.raw.nlines
      , by = orbis.batch.nlines)
  } else orbis.data.raw.skip.lines
rows.read <-
  if(orbis.batch.nlines != Inf | !is.na(orbis.data.raw.nlines)) {
    rows.skip[-1] %>%
      c(orbis.data.raw.nlines) %>%
      '-'(rows.skip)
  } else orbis.data.raw.nlines
  ## write batches to .rds
  batch <- 
    lapply(1:length(rows.read), function(i) {
      ## extract batch
      message("==============================================================================")
      message("Reading lines from ", rows.skip[i])
      started <- Sys.time()
      message("Started: ", date())
      ## read batch
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
      message("Batch loaded!")
      ## rename columns
      names(orbis.data.batch) <- orbis.data.description$var.name
      ## harmonization
      if(!is.0(orbis.harmonize.cols)) {
        sapply(orbis.harmonize.cols, function(col) {
          message("Harmonizing '", col, "' column...")
          orbis.data.batch[[paste0(col, ".harmonized")]] <<- 
            orbis.data.batch[[col]] %>%
            harmonize(progress.by = harmonizer.progress.by
                    , quite = harmonizer.quite
                    , procedures = harmonizer.procedures)
          message("Harmonized '", col, "' column! Yey!")
        })
      }
      ## save batch
      if(save.rds) {
        ## Make a dir for saving .rds
        if(orbis.batch.path %>% is.0) {
          orbis.batch.path <-
            orbis.data.raw.file.noext %>%
            paste0(".rds") %>% 
            file.path(orbis.data.path, .) %T>%
            dir.create(showWarnings = FALSE)
        }
        batch.file.name.lines <-
          if(orbis.batch.nlines != Inf) {
            batch.file.format <- paste0("%0", nchar(orbis.data.raw.nlines), "d")
            paste0(".lines-"
                 , sprintf(batch.file.format, rows.skip[i] + 1)
                 , "-"
                 , sprintf(batch.file.format, rows.skip[i] + rows.read[i]))
          } else ""
        batch.file.path <-
          paste0(file.path(orbis.batch.path, orbis.data.raw.file.noext)
               , batch.file.name.prefix
               , batch.file.name.sufix
               , batch.file.name.lines
               , ".rds")
        message("Saving RDS: ", batch.file.path)
        saveRDS(orbis.data.batch, batch.file.path)
      }
      message("Done! (in ", as.numeric(Sys.time() - started) %>% round, " minutes)")
      ## return
      if(save.rds) batch.file.path else orbis.data.batch
    })
  if(return.invisible) return(batch) %>% invisible
  else return(batch)
}

## --------------------------------------------------------------------------------


## orbis.save.rds("key-financials-usd.txt", orbis.select.fields = c("emp", "rev"))
## orbis.save.rds("key-financials-usd.txt") %>% unlist
## orbis.save.rds("orbis-harmonize.r")



orbis.data.raw.nlines = 100
orbis.data.raw.skip.lines = 2
orbis.batch.nlines = Inf


rows.skip <-
  if(orbis.batch.nlines != Inf ) {
    seq(from = orbis.data.raw.skip.lines
      , to = orbis.data.raw.nlines
      , by = orbis.batch.nlines)
  } else orbis.data.raw.skip.lines
rows.read <-
  if(orbis.batch.nlines != Inf | !is.na(orbis.data.raw.nlines)) {
    rows.skip[-1] %>%
      c(orbis.data.raw.nlines) %>%
      '-'(rows.skip)
  } else orbis.data.raw.nlines

# --------------------------------------------------------------------------------
#' Unpacks Orbis RAR files.
#'
#' By default assumes that it is running from the orbis data directory. Also it chances output file name to lowercase and "_" becomes "-".
#' @param rarfile File name to unrar. (without .rar extention)
#' @param rardir Default is in working directory "orbis-world-2017-09-13"
#' @param exdir A path where to extract zip file. Default is in working directory "patview-data-tsv"
#' @param unrar.command Command to unpack the archive (befault is "7z x -o")
#' @return Unzipped file path.
#' @import magrittr stringr
#' @export
#' @examples
#' none yet...
#' @md
orbis.unrar.txt <- function(rarfile
                          , rardir = file.path(getwd(), "orbis-world-2017-09-13")
                          , exdir = getwd()
                          , unrar.command = "7z x -o") {
  if(Sys.which(str_extract(command, "^[^\\s]+")) != "") {
    file.name <- rarfile %>%
      basename %>% 
      str_replace_all("_", "-") %>%
      tolower
    if(str_detect(exdir %>% list.files, file.name) %>% any) {
      message("Seems lile file '", rarfile, ".rar' is already extracted. Exiting.")
    } else {
      message("Unpacking the file - ", rarfile, "...")
      rarfile %>%
        paste0(".rar") %>% 
        file.path(rardir,.) %>%
        normalizePath %>% 
        paste0(unrar.command,' "', exdir, '" "',.,'"') %>% 
        system
      message("File extracted.")
      rarfile %>%
        basename %>% 
        paste0(".txt") %>%
        file.rename(tolower(str_replace_all(.,"_", "-")))
    }
    file.name %>%
      paste0(".txt")
  } else message("Command ", unrar.command," is not awailable.")
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
                       , files.pattern = NULL
                       , cols = character(0)
                       , progress.bar = TRUE) {
  orbis.files <- orbis.data.path %>%
    file.path(list.files(.
                       , pattern = files.pattern))
  if(progress.bar) {
    orbis.files %>% 
      pblapply(function(orbis.data.file.path)
        orbis.data.file.path %>%
        readRDS %>% 
        dplyr::filter(...) %>%
        dplyr::select(if(cols %>% is.0) everything() else cols)) %>%
      rbindlist(fill = TRUE) %>% 
      return
  } else {
    orbis.files %>% 
      lapply(function(orbis.data.file.path)
        orbis.data.file.path %>%
        readRDS %>% 
        dplyr::filter(...) %>%
        dplyr::select(if(cols %>% is.0) everything() else cols)) %>%
      rbindlist(fill = TRUE) %>% 
      return
  }}
## --------------------------------------------------------------------------------
