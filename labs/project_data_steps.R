#
# Author:     Jesse Lecy
# Maintainer: Cristian Nuno
# Date:       March 21, 2021
# Purpose:    Create custom functions to pre-process the LTDB raw data files
#

# load necessary packages ----
library( dplyr )
library( here )
library( knitr )
library( pander )

# create custom cleaning function ----
# convert variables to numeric
# and remove missing values placeholders;
# impute missing values with mean
clean_x <- function( x )
{
  x <- as.numeric( x )
  x[ x == -999 ] <- NA
  mean.x <- mean( x, na.rm=T )
  x[ is.na(x) ] <- mean.x
  return(x)
}

# apply the clean var x function to all columns 
clean_d <- function( d, start_column )
{
  these <- start_column:ncol(d)
  d[ these ] <- lapply( d[ these ], clean_x )
  
  return( d )
}

# FIX VARIABLE NAMES
# input dataframe
# standardize variable names 
# output data frame with fixed names
fix_names <- function( d )
{
  nm <- names( d )
  nm <- tolower( nm )
  
  nm[ nm == "statea"  ] <- "state"
  nm[ nm == "countya" ] <- "county"
  nm[ nm == "tracta"  ] <- "tract"
  nm[ nm == "trtid10" ] <- "tractid"
  nm[ nm == "mar-70"  ] <- "mar70"
  nm[ nm == "mar-80"  ] <- "mar80"
  nm[ nm == "mar-90"  ] <- "mar90"
  nm[ nm == "mar.00"  ] <- "mar00"
  nm[ nm == "x12.mar" ] <- "mar12"  
  nm <- gsub( "sp1$", "", nm )
  nm <- gsub( "sp2$", "", nm )
  nm <- gsub( "sf3$", "", nm )
  nm <- gsub( "sf4$", "", nm )
  
  # nm <- gsub( "[0-9]{2}$", "", nm )
  
  names( d ) <- nm
  return( d )
}

# FIX TRACT IDS
# put into format: SS-CCC-TTTTTT
fix_ids <- function( x )
{
  x <- stringr::str_pad( x, 11, pad = "0" )
  state <- substr( x, 1, 2 )
  county <- substr( x, 3, 5 )
  tract <- substr( x, 6, 11 )
  x <- paste( "fips", state, county, tract, sep="-" )
  return(x)
}


tidy_up_data <- function( file.name )
{
  # store the file path as a character vector
  path <- paste0( "data/raw/", file.name )
  # read in the file path using here::here()
  d <- read.csv( here::here(path), colClasses="character" ) 
  type <- ifelse( grepl( "sample", file.name ), "sample", "full" )
  year <- substr( file.name, 10, 13 )
  
  # fix names 
  d <- fix_names( d )
  
  # fix leading zero problem in tract ids
  d$tractid <- fix_ids( d$tractid )
  
  # drop meta-vars
  drop.these <- c("state", "county", "tract", "placefp10",
                  "cbsa10", "metdiv10", "ccflag10", 
                  "globd10", "globg10","globd00", "globg00",
                  "globd90", "globg90","globd80", "globg80")
  d <- d[ ! names(d) %in% drop.these ]
  
  # column position where variables start after IDs
  d <- clean_d( d, start_column=2 )
  
  # add year and type (sample/full)
  d <- data.frame( year, type, d, stringsAsFactors=F )
  
  return( d )
}

build_year <- function( fn1, fn2, year )
{ 
  
  d1 <- tidy_up_data( fn1 )
  d1 <- select( d1, - type )
  
  d2 <- tidy_up_data( fn2 )
  d2 <- select( d2, - type )
  
  d3 <- merge( d1, d2, by=c("year","tractid"), all=T )
  
  # store the file path as a character vector
  file.name <- paste0( "data/rodeo/LTDB-", year, ".rds" )
  # export the object to the file path from above using here::here()
  saveRDS( d3, here::here( file.name ) )
  
}

# store crosswalk URL location
# TODO: should should a local copy in case this URL goes bad
URL <- "https://data.nber.org/cbsa-msa-fips-ssa-county-crosswalk/cbsatocountycrosswalk.csv"
cw <- read.csv( URL, colClasses="character" )

# state column subset 
keep.these <- c( "countyname","state","fipscounty", 
                 "msa","msaname", 
                 "cbsa","cbsaname",
                 "urban" )
# filter crosswalk
cw <- dplyr::select( cw, keep.these )

# export results
saveRDS( cw, here::here( "data/raw/cbsa-crosswalk.rds") )

extract_metadata <- function( file.name )
{
  # store the file path as a character vector
  path <- paste0( "data/raw/", file.name )
  # import the file using the file path inside of here::here()
  d <- read.csv( here::here( path ), colClasses="character" ) 
  type <- ifelse( grepl( "sample", file.name ), "sample", "full" )
  year <- substr( file.name, 10, 13 )
  
  # fix names 
  d <- fix_names( d )
  
  # fix leading zero problem in tract ids
  d$tractid <- fix_ids( d$tractid )
  
  # drop meta-vars
  keep.these <- c("tractid","state", "county", "tract", "placefp10",
                  "cbsa10", "metdiv10", "ccflag10", 
                  "globd10", "globg10","globd00", "globg00",
                  "globd90", "globg90","globd80", "globg80")
  d <- d[ names(d) %in% keep.these ]
  return( d )
}


f.1970 <- "LTDB_Std_1970_fullcount.csv"
f.1980 <- "LTDB_Std_1980_fullcount.csv"
f.1990 <- "LTDB_Std_1990_fullcount.csv"
f.2000 <- "LTDB_Std_2000_fullcount.csv"

meta.d.2000 <- extract_metadata( file.name=f.2000 )

meta.d.1990 <- extract_metadata( file.name=f.1990 )
meta.d.1990 <- select( meta.d.1990, tractid, globd90, globg90 )

meta.d.1980 <- extract_metadata( file.name=f.1980 )
meta.d.1980 <- select( meta.d.1980, tractid, globd80, globg80 )

meta.d <- merge( meta.d.2000, meta.d.1990, all=T )
meta.d <- merge( meta.d, meta.d.1980, all=T )

meta.d$fipscounty <- paste0( substr( meta.d$tractid, 6, 7 ), 
                             substr( meta.d$tractid, 9, 11 ) )

# drop duplicate counties
cw <- cw[ ! duplicated(cw$fipscounty) , ]

# merge fips data onto metadata
meta.d <- merge( meta.d, cw, by="fipscounty", all.x=T )

# save metadata
saveRDS( meta.d, here::here( "data/rodeo/LTDB-META-DATA.rds" ) )

