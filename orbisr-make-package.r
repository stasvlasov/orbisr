## Creates OrbisR package


## --------------------------------------------------------------------------------
## Load or Install Packages
## --------------------------------------------------------------------------------
for(pkg in c('pbapply'
           , 'stringr'
           , 'data.table'
           , 'dplyr'
           , 'magrittr'
           , 'devtools'
           , 'roxygen2'))
    if(!require(pkg, character.only = TRUE)) {
        install.packages(pkg, repos = 'http://cloud.r-project.org', quiet = TRUE)
        library(pkg, character.only = TRUE)
    }
## --------------------------------------------------------------------------------


## Making a package
## --------------------------------------------------------------------------------
## setwd("~/org/data/orbis/orbisr")

## Updates package info
person("Stas", "Vlasov", 
       email = "s.vlasov@uvt.nl", 
       role  = c("aut", "cre")) %>% 
    {paste0("'",., "'")} %>%
    {options(devtools.desc.author = .)}



## Assume that it runs from "harmonizer" directory
list(Title  = "Tools for working with Orbis Bulk database"
   , Date = "2019-01-11"
   , License = "MIT License"
   , Imports = paste("pbapply"
                   , "data.table"
                   , "magrittr"
                   , "stringr"
                   , "dplyr"
                   , sep = ", ")
   , Depends = "R (>= 3.4.1)"
   , Description = "Set of functions that help to prepare, to load into R session and to search Orbis Bulk data"
   , References = "BvD Orbis - Detailed global private company information - https://www.bvdinfo.m/en-us/our-products/company-information/international-products/orbis") %>% 
    {setup(rstudio = FALSE
         , description = .)}


## Update name spaces and documentation for functions
roxygenise()

document()



## Testing
## --------------------------------------------------------------------------------
detach('orbisr')
remove.packages('orbisr')

install(".")

## install_github("stasvlasov/harmonizer")
library('orbisr')








