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
           , "usethis"
           , 'roxygen2'))
    if(!require(pkg, character.only = TRUE)) {
        install.packages(pkg, repos = 'http://cloud.r-project.org', quiet = TRUE)
        library(pkg, character.only = TRUE)
    }
## --------------------------------------------------------------------------------


## Making a package
## --------------------------------------------------------------------------------

## Assume that it runs from package directory
use_description(fields =
                    list(Title  = "Tools for working with Orbis Bulk database"
                       , Date = "2019-03-18"
                       , "Authors@R" = 'as.person("Stanislav Vlasov <s.vlasov@uvt.nl> [aut, cre]")'
                       , License = "MIT License"
                       , Imports = paste("pbapply"
                                       , "data.table"
                                       , "magrittr"
                                       , "stringr"
                                       , "dplyr"
                                       , sep = ", ")
                       , Depends = "R (>= 3.4.1)"
                       , Description = "Set of functions that help to prepare, to load into R session and to search Orbis Bulk data"
                       , References = "BvD Orbis - Detailed global private company information - https://www.bvdinfo.m/en-us/our-products/company-information/international-products/orbis"))


## document()  # This function is a wrapper for the ‘roxygen2::roxygenize()’ but also load the package

roxygenize()



## Testing
## --------------------------------------------------------------------------------
detach("orbisr")
remove.packages("orbisr")

install(".")

library('orbisr')

## install_github("stasvlasov/orbiser")







