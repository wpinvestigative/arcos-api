#* @apiTitle Washington Post ARCOS API
#* @apiDescription API to access the DEA's ARCOS data prepared by [The Washington Post](https://www.washingtonpost.com/graphics/2019/investigations/dea-pain-pill-database/). 
#*   Check out the R package [ARCOS](https://github.com/wpinvestigative/arcos), that's a wrapper for this API.
#*   Use "WaPo" as the key or use any of the keys [listed here](https://github.com/wpinvestigative/arcos/blob/master/keys/keys.txt). 
#*   We reserve the right to discontinue these keys and invite users to sign up for their own individual one in the future if the need arises.
#*   There are three categories of data offered in this API. 1) Raw and slices of the raw data  
#*   2) Summarized data (based on our [filters and criteria](https://www.washingtonpost.com/national/2019/07/18/how-download-use-dea-pain-pills-database/)) for easier analysis 
#*   and 3) Supplemental data, such as county, census tracts, and latitude and longitude for chain and retail pharmacies. Please refer to [our guidelines](https://www.washingtonpost.com/national/2019/07/18/how-download-use-dea-pain-pills-database/) for using this data.

library(tidyverse)
library(jsonlite)
library(stringr)
library(vroom)

# Loading in custom data
keys <- readRDS("data/keys.RDS")
list_of_keys <- keys$key
county_relationship_file <- readRDS("data/county_fips.RDS")
county_relationship_states <- county_relationship_file %>% pull(BUYER_STATE) %>% unique()
county_relationship_counties <- county_relationship_file %>% pull(BUYER_COUNTY) %>% unique()
county_relationship_fips <- county_relationship_file %>% pull(countyfips) %>% unique()

### County data

#' Returns all data by county as a tsv
#' @param state If provided, filter the data to only this state (e.g. 'WV')
#' @param county If provided, filter the data to only this county (e.g. 'Mingo')
#' @param key Key needed to make query successful
#' @param res Response object (leave blank)
#' @tag raw
#' @get /county_data
function(state, county, key, res){
  
  if (missing(key)) {
    return(list(error="Authentication required. Did you include an API key?"))
  } else {
    if (key %in% list_of_keys) {
      
      base_url <- "https://www.washingtonpost.com/wp-stat/dea-pain-pill-database/summary/arcos-"
      
      state_abb <- str_to_upper(state)
      county_name <- str_to_upper(county)
      #county_name <- gsub("-", " ", county_name)
      county_name <- gsub("%20", " ", county_name)
      county_relationship_file_only <- county_relationship_file %>% 
        filter(BUYER_STATE==state_abb & BUYER_COUNTY==county_name)
      
      if (!state_abb %in% county_relationship_states | !county_name %in% county_relationship_counties) {
        return(list(error="No such place. Do you have the county or state name spelled right? https://www.washingtonpost.com/graphics/2019/investigations/dea-pain-pill-database/#download-resources"))
        
      } else {
        county_fips <- county_relationship_file_only %>% pull(countyfips)
        
        state_abb <- str_to_lower(state_abb)
        county_name <- str_to_lower(county_name)
        county_name <- gsub(" ", "-", county_name)
        
        url <- paste0(base_url, state_abb, "-", county_name, "-", county_fips, "-itemized.tsv")
        
        #df <- vroom(url)
        #return(df)
        res$status <- 302
        res$setHeader('Location', url)
        return(res);
      }
      
      
    } else {
      return(list(error="Authentication required. Did you include an API key?"))
    }
  }
}


### County populations

#' Returns historical county population data
#' @param state If provided, filter the data to only this state (e.g. 'WV')
#' @param county If provided, filter the data to only this county (e.g. 'Mingo')
#' @param key Key needed to make query successful
#' @tag supplemental
#' @get /county_population
function(state, county, key){
  
  
  if (missing(key)) {
    return(list(error="Authentication required. Did you include an API key?"))
  } else {
    if (key %in% list_of_keys) {
      
      df <- read_csv("data/pop_counties_20062012.csv")
      
      # Filter if the state was specified
      if (!missing(state)){
        state <- str_to_upper(state)
        if (state %in% county_relationship_states) {
          df <- filter(df, BUYER_STATE == state)
        } else {
          return(list(error="No such place. Did you abbreviate the state correctly? https://www.washingtonpost.com/graphics/2019/investigations/dea-pain-pill-database/#download-resources"))
        }
      }
      
      # Filter if the county was specified
      if (!missing(county)){
        county <- str_to_upper(county)
        if (county %in% county_relationship_counties) {
          df <- filter(df, BUYER_COUNTY == county)
        } else {
          return(list(error="No such place. Did you spell the county correctly? https://www.washingtonpost.com/graphics/2019/investigations/dea-pain-pill-database/#download-resources"))
        }
      }
      
      return(df)
      
    } else {
      return(list(error="Authentication required. Did you include an API key?"))
    }
    
  }
  
}


### Not pharmacies

#' Returns list of 330+ BUYER_DEA_NOs that we've identified as mail order or hospitals and not retail or chain pharmacies
#' @param key Key needed to make query successful
#' @tag supplemental
#' @get /not_pharmacies
function(key){
  
  
  if (missing(key)) {
    return(list(error="Authentication required. Did you include an API key?"))
  } else {
    if (key %in% list_of_keys) {
      
      df <- read_csv("data/not_pharms.csv")
      
      return(df)
      
    } else {
      return(list(error="Authentication required. Did you include an API key?"))
    }
    
  }
  
}


### County populations

#' Returns historical statepopulation data
#' @param state If provided, filter the data to only this state (e.g. 'WV')
#' @param key Key needed to make query successful
#' @tag supplemental
#' @get /state_population
function(state, key){
  
  
  if (missing(key)) {
    return(list(error="Authentication required. Did you include an API key?"))
  } else {
    if (key %in% list_of_keys) {
      
      df <- read_csv("data/pop_states_20062012.csv")
      
      # Filter if the state was specified
      if (!missing(state)){
        state <- str_to_upper(state)
        if (state %in% county_relationship_states) {
          df <- filter(df, BUYER_STATE == state)
        } else {
          return(list(error="No such place. Did you abbreviate the state correctly? https://www.washingtonpost.com/graphics/2019/investigations/dea-pain-pill-database/#download-resources"))
        }
      }
      
      return(df)
      
    } else {
      return(list(error="Authentication required. Did you include an API key?"))
    }
    
  }
  
}

### County data via fips code

#' Returns all data by county FIPS code as a tsv
#' @param fips If provided, filter the data to only this county (e.g. '01001' for Autauga, Alabama)
#' @param key Key needed to make query successful
#' @param res Response object
#' @tag raw
#' @get /county_fips_data
function(fips, key, res){
  
  if (missing(key)) {
    return(list(error="Authentication required. Did you include an API key?"))
  } else {
    if (key %in% list_of_keys) {
      
      base_url <- "https://www.washingtonpost.com/wp-stat/dea-pain-pill-database/summary/arcos-"
      
      #state_abb <- str_to_upper(state)
      #county_name <- str_to_upper(county)
      
      county_relationship_file_only <- county_relationship_file %>% 
        filter(countyfips==fips)
      
      if (!fips %in% county_relationship_fips) {
        return(list(error="No such place. Did you mistype the six-character FIPS code?"))
        
      } else {
        state_abb <- county_relationship_file_only %>% pull(BUYER_STATE) %>% str_to_lower()
        county_name <- county_relationship_file_only %>% pull(BUYER_COUNTY) %>% str_to_lower()
        
        url <- paste0(base_url, state_abb, "-", county_name, "-", fips, "-itemized.tsv")
        
        #df <- vroom(url)
        # return(df)
        ## instead of reading the DF locally and returning it, we'll just 302 temp. redirect users to the WWW Page
        res$status <- 302
        res$setHeader('Location', url);
        return(res)
      }
      
    } else {
      return(list(error="Authentication required. Did you include an API key?"))
    }
  }
}


### Pharmacy location data below

#' Returns pharmacy latitude and longitude data
#' @param key Key needed to make query successful
#' @param state If provided, filter the data to only this state (e.g. 'WV')
#' @param county If provided, filter the data to only this county (e.g. 'Mingo')
#' @tag supplemental
#' @get /pharmacy_latlon
function(state, county, key){
  
  if (missing(key)) {
    return(list(error="Authentication required. Did you include an API key?"))
  } else {
    if (key %in% list_of_keys) {
      
      df <- read_csv("data/pharmacies_latlon.csv")
      
      # Filter if the state was specified
      if (!missing(state)){
        state <- str_to_upper(state)
        if (state %in% county_relationship_states) {
          df <- filter(df, BUYER_STATE == state)
        } else {
          return(list(error="No such place. Did you abbreviate the state correctly? https://www.washingtonpost.com/graphics/2019/investigations/dea-pain-pill-database/#download-resources"))
        }
      }
      
      # Filter if the county was specified
      if (!missing(county)){
        county <- str_to_upper(county)
        if (county %in% county_relationship_counties) {
          df <- filter(df, BUYER_COUNTY == county)
        } else {
          return(list(error="No such place. Did you spell the county correctly? https://www.washingtonpost.com/graphics/2019/investigations/dea-pain-pill-database/#download-resources"))
        }
      }
      
      return(df)
      
    } else {
      return(list(error="Authentication required. Did you include an API key?"))
    }
    
  }
}


#' Returns pharmacy county FIPS id number
#' @param key Key needed to make query successful
#' @param state If provided, filter the data to only this state (e.g. 'WV')
#' @param county If provided, filter the data to only this county (e.g. 'Mingo')
#' @tag supplemental
#' @get /pharmacy_counties
function(state, county, key){
  
  if (missing(key)) {
    return(list(error="Authentication required. Did you include an API key?"))
  } else {
    if (key %in% list_of_keys) {
      
      df <- read_csv("data/pharmacies_counties.csv")
      
      # Filter if the state was specified
      if (!missing(state)){
        state <- str_to_upper(state)
        if (state %in% county_relationship_states) {
          df <- filter(df, BUYER_STATE == state)
        } else {
          return(list(error="No such place. Did you abbreviate the state correctly? https://www.washingtonpost.com/graphics/2019/investigations/dea-pain-pill-database/#download-resources"))
        }
      }
      
      # Filter if the county was specified
      if (!missing(county)){
        county <- str_to_upper(county)
        if (county %in% county_relationship_counties) {
          df <- filter(df, BUYER_COUNTY == county)
        } else {
          return(list(error="No such place. Did you spell the county correctly? https://www.washingtonpost.com/graphics/2019/investigations/dea-pain-pill-database/#download-resources"))
        }
      }
      
      return(df)
      
    } else {
      return(list(error="Authentication required. Did you include an API key?"))
    }
    
  }
}


#' Returns pharmacy census tracts FIPS code
#' @param key Key needed to make query successful
#' @param state If provided, filter the data to only this state (e.g. 'WV')
#' @param county If provided, filter the data to only this county (e.g. 'Mingo')
#' @tag supplemental
#' @get /pharmacy_tracts
function(state, county, key){
  
  if (missing(key)) {
    return(list(error="Authentication required. Did you include an API key?"))
  } else {
    if (key %in% list_of_keys) {
      
      df <- read_csv("data/pharmacies_tracts.csv")
      
      # Filter if the state was specified
      if (!missing(state)){
        state <- str_to_upper(state)
        if (state %in% county_relationship_states) {
          df <- filter(df, BUYER_STATE == state)
        } else {
          return(list(error="No such place. Did you abbreviate the state correctly? https://www.washingtonpost.com/graphics/2019/investigations/dea-pain-pill-database/#download-resources"))
        }
      }
      
      # Filter if the county was specified
      if (!missing(county)){
        county <- str_to_upper(county)
        if (county %in% county_relationship_counties) {
          df <- filter(df, BUYER_COUNTY == county)
        } else {
          return(list(error="No such place. Did you spell the county correctly? https://www.washingtonpost.com/graphics/2019/investigations/dea-pain-pill-database/#download-resources"))
        }
      }
      
      return(df)
      
    } else {
      return(list(error="Authentication required. Did you include an API key?"))
    }
    
  }
}


#' Returns pharmacy core-based statistical area FIPS code
#' @param key Key needed to make query successful
#' @param state If provided, filter the data to only this state (e.g. 'WV')
#' @param county If provided, filter the data to only this county (e.g. 'Mingo')
#' @tag supplemental
#' @get /pharmacy_cbsa
function(state, county, key){
  
  if (missing(key)) {
    return(list(error="Authentication required. Did you include an API key?"))
  } else {
    if (key %in% list_of_keys) {
      
      df <- read_csv("data/pharmacies_cbsa.csv")
      
      # Filter if the state was specified
      if (!missing(state)){
        state <- str_to_upper(state)
        if (state %in% county_relationship_states) {
          df <- filter(df, BUYER_STATE == state)
        } else {
          return(list(error="No such place. Did you abbreviate the state correctly? https://www.washingtonpost.com/graphics/2019/investigations/dea-pain-pill-database/#download-resources"))
        }
      }
      
      # Filter if the county was specified
      if (!missing(county)){
        county <- str_to_upper(county)
        if (county %in% county_relationship_counties) {
          df <- filter(df, BUYER_COUNTY == county)
        } else {
          return(list(error="No such place. Did you spell the county correctly? https://www.washingtonpost.com/graphics/2019/investigations/dea-pain-pill-database/#download-resources"))
        }
      }
      
      return(df)
      
    } else {
      return(list(error="Authentication required. Did you include an API key?"))
    }
    
  }
}



### Pharmacy data

#' Returns pharmacy list and location data
#' @param key Key needed to make query successful
#' @param buyer_dea_no Required number (e.g. 'AB0454176')
#' @param res Response object (leave blank)
#' @tag raw
#' @get /pharmacy_data
function(buyer_dea_no, key, res){
  
  if (missing(key)) {
    return(list(error="Authentication required. Did you include an API key?"))
  } else {
    if (key %in% list_of_keys) {
      
      dea_nos <- read_csv("data/pharmacies_dea_nos.csv")
      
      if (!missing(buyer_dea_no)) {
        
        buyer_dea <- str_to_upper(buyer_dea_no)
        
        if (buyer_dea %in% dea_nos$BUYER_DEA_NO) {
          file_link <- dea_nos %>% filter(BUYER_DEA_NO == buyer_dea) %>% pull(filename)
          
          url <- paste0("https://www.washingtonpost.com/wp-stat/dea-pain-pill-database/bulk/pharmacy/", file_link, ".csv")
          #df <- vroom(url)
          #return(df)
          
          res$status <- 302
          res$setHeader('Location', url)
          return(res);
          
          
        } else {
          return(list(error="BUYER_DEA_NO not found"))
          
        }
        
      } else {
        return(list(error="BUYER_DEA_NO not found"))
      }
      
    } else {
      return(list(error="Authentication required. Did you include an API key?"))
    }
    
  }
}


### All buyers' addresses listed in the ARCOS database 

#' Returns buyer details (mail order, pharmacy, retail, practitioner, etc) 
#' @param key Key needed to make query successful
#' @param state If provided, filter the data to only this state (e.g. 'WV')
#' @param county If provided, filter the data to only this county (e.g. 'Mingo')
#' @tag raw
#' @get /buyer_details
function(state, county, key){
  
  if (missing(key)) {
    return(list(error="Authentication required. Did you include an API key?"))
  } else {
    if (key %in% list_of_keys) {
      
      df <- read_csv("data/detail_list_buyers.csv")
      
      # Filter if the state was specified
      if (!missing(state)){
        state <- str_to_upper(state)
        if (state %in% county_relationship_states) {
          df <- filter(df, BUYER_STATE == state)
        } else {
          return(list(error="No such place. Did you abbreviate the state correctly? https://www.washingtonpost.com/graphics/2019/investigations/dea-pain-pill-database/#download-resources"))
        }
      }
      
      # Filter if the county was specified
      if (!missing(county)){
        county <- str_to_upper(county)
        if (county %in% county_relationship_counties) {
          df <- filter(df, BUYER_COUNTY == county)
        } else {
          return(list(error="No such place. Did you spell the county correctly? https://www.washingtonpost.com/graphics/2019/investigations/dea-pain-pill-database/#download-resources"))
        }
      }
      
      return(df)
      
    } else {
      return(list(error="Authentication required. Did you include an API key?"))
    }
    
  }
}


### Reporter details

#' Returns Reporter (Manufacturers and Distributors) details such as addresses
#' @param key Key needed to make query successful
#' @param state If provided, filter the data to only this state (e.g. 'WV')
#' @param county If provided, filter the data to only this county (e.g. 'Mingo')
#' @tag raw
#' @get /reporter_details
function(state, county, key){
  
  
  if (missing(key)) {
    return(list(error="Authentication required. Did you include an API key?"))
  } else {
    if (key %in% list_of_keys) {
      
      df <- read_csv("data/detail_list_reporters.csv")
      
      # Filter if the state was specified
      if (!missing(state)){
        state <- str_to_upper(state)
        if (state %in% county_relationship_states) {
          df <- filter(df, REPORTER_STATE == state)
        } else {
          return(list(error="No such place. Did you abbreviate the state correctly? https://www.washingtonpost.com/graphics/2019/investigations/dea-pain-pill-database/#download-resources"))
        }
      }
      
      # Filter if the county was specified
      if (!missing(county)){
        county <- str_to_upper(county)
        if (county %in% county_relationship_counties) {
          df <- filter(df, REPORTER_COUNTY == county)
        } else {
          return(list(error="No such place. Did you spell the county correctly? https://www.washingtonpost.com/graphics/2019/investigations/dea-pain-pill-database/#download-resources"))
        }
      }
      
      return(df)
      
    } else {
      return(list(error="Authentication required. Did you include an API key?"))
    }
    
  }
}


### Annual county dosages

#' Returns seller details such as addresses
#' @param key Key needed to make query successful
#' @param state If provided, filter the data to only this state (e.g. 'WV')
#' @param county If provided, filter the data to only this county (e.g. 'Mingo')
#' @tag summary
#' @get /combined_county_annual
function(state, county, key){
  
  if (missing(key)) {
    return(list(error="Authentication required. Did you put in an API key?"))
  } else {
    if (key %in% list_of_keys) {
      
      county_annual <- read_csv("data/county_annual.csv")
      
      if (!missing(state)) {
        
        state <- str_to_upper(state)
        
        if (state %in% county_relationship_states) {
          county_annual <- filter(county_annual, BUYER_STATE==state)
          
        } else {
          return(list(error="State abbreviation not found"))
          
        }
        
      }
      
      
      if (!missing(county)) {
        
        county <- str_to_upper(county)
        county <- gsub("-", " ", county)
        county <- gsub("%20", " ", county)
        if (county %in% county_relationship_counties) {
          county_annual <- filter(county_annual, BUYER_COUNTY==county)
          
        } else {
          return(list(error="County not found"))
          
        }
        
      }
      
      return(county_annual)
      
    } else {
      return(list(error="Authentication required. Did you include an API key?"))
    }
    
  }
  
}



### Monthly county dosages

#' Returns seller details such as addresses
#' @param key Key needed to make query successful
#' @param state If provided, filter the data to only this state (e.g. 'WV')
#' @param county If provided, filter the data to only this county (e.g. 'Mingo')
#' @tag summary
#' @get /combined_county_monthly
function(state, county, key){
  
  if (missing(key)) {
    return(list(error="Authentication required. Did you include an API key?"))
  } else {
    if (key %in% list_of_keys) {
      
      county_monthly <- read_csv("data/county_monthly.csv")
      
      if (!missing(state)) {
        
        state <- str_to_upper(state)
        
        if (state %in% county_relationship_states) {
          county_monthly <- filter(county_monthly, BUYER_STATE==state)
          
        } else {
          return(list(error="State abbreviation not found"))
          
        }
        
      }
      
      
      if (!missing(county)) {
        
        county <- str_to_upper(county)
        county <- gsub("-", " ", county)
        county <- gsub("%20", " ", county)
        if (county %in% county_relationship_counties) {
          county_monthly <- filter(county_monthly, BUYER_COUNTY==county)
          
        } else {
          return(list(error="County not found"))
          
        }
        
      }
      
      return(county_monthly)
      
    } else {
      return(list(error="Authentication required. Did you include an API key?"))
    }
    
  }
  
}


### Total pills for each pharmacy in a county

#' Returns all pharmacy totals by county as a tsv
#' @param state If provided, filter the data to only this state (e.g. 'WV')
#' @param county If provided, filter the data to only this county (e.g. 'Mingo')
#' @param key Key needed to make query successful
#' @param res Response object (leave blank)
#' @tag summary
#' @get /total_pharmacies_county
function(state, county, key, res){
  
  if (missing(key)) {
    return(list(error="Authentication required. Did you include an API key?"))
  } else {
    if (key %in% list_of_keys) {
      
      base_url <- "https://www.washingtonpost.com/wp-stat/dea-pain-pill-database/summary/arcos-"
      
      state_abb <- str_to_upper(state)
      county_name <- str_to_upper(county)
      #county_name <- gsub("-", " ", county_name)
      county_name <- gsub("%20", " ", county_name)
      county_relationship_file_only <- county_relationship_file %>% 
        filter(BUYER_STATE==state_abb & BUYER_COUNTY==county_name)
      
      if (!state_abb %in% county_relationship_states | !county_name %in% county_relationship_counties) {
        return(list(error="No such place. Do you have the county or state name spelled right? https://www.washingtonpost.com/graphics/2019/investigations/dea-pain-pill-database/#download-resources"))
        
      } else {
        county_fips <- county_relationship_file_only %>% pull(countyfips)
        
        state_abb <- str_to_lower(state_abb)
        county_name <- str_to_lower(county_name)
        county_name <- gsub(" ", "-", county_name)
        
        url <- paste0(base_url, state_abb, "-", county_name, "-", county_fips, "-pharmacy.tsv")
        
        #df <- vroom(url)
        #return(df)
        res$status <- 302
        res$setHeader('Location', url)
        return(res);
      }
      
      
    } else {
      return(list(error="Authentication required. Did you include an API key?"))
    }
  }
}


### Total pills for each Manufacturer to a county

#' Returns all Manufacturer totals by county as a tsv
#' @param state If provided, filter the data to only this state (e.g. 'WV')
#' @param county If provided, filter the data to only this county (e.g. 'Mingo')
#' @param key Key needed to make query successful
#' @param res Response object (leave blank)
#' @tag summary
#' @get /total_manufacturers_county
function(state, county, key, res){
  
  if (missing(key)) {
    return(list(error="Authentication required. Did you include an API key?"))
  } else {
    if (key %in% list_of_keys) {
      
      base_url <- "https://www.washingtonpost.com/wp-stat/dea-pain-pill-database/summary/arcos-"
      
      state_abb <- str_to_upper(state)
      county_name <- str_to_upper(county)
      #county_name <- gsub("-", " ", county_name)
      county_name <- gsub("%20", " ", county_name)
      county_relationship_file_only <- county_relationship_file %>% 
        filter(BUYER_STATE==state_abb & BUYER_COUNTY==county_name)
      
      if (!state_abb %in% county_relationship_states | !county_name %in% county_relationship_counties) {
        return(list(error="No such place. Do you have the county or state name spelled right? https://www.washingtonpost.com/graphics/2019/investigations/dea-pain-pill-database/#download-resources"))
        
      } else {
        county_fips <- county_relationship_file_only %>% pull(countyfips)
        
        state_abb <- str_to_lower(state_abb)
        county_name <- str_to_lower(county_name)
        county_name <- gsub(" ", "-", county_name)
        
        url <- paste0(base_url, state_abb, "-", county_name, "-", county_fips, "-labeler.tsv")
        
        #df <- vroom(url)
        #return(df)
        res$status <- 302
        res$setHeader('Location', url)
        return(res);
      }
      
      
    } else {
      return(list(error="Authentication required. Did you include an API key?"))
    }
  }
}


### Total pills for each Distributor to a county

#' Returns all Distributor totals by county as a tsv
#' @param state If provided, filter the data to only this state (e.g. 'WV')
#' @param county If provided, filter the data to only this county (e.g. 'Mingo')
#' @param key Key needed to make query successful
#' @param res Response object (leave blank)
#' @tag summary
#' @get /total_distributors_county
function(state, county, key, res){
  
  if (missing(key)) {
    return(list(error="Authentication required. Did you include an API key?"))
  } else {
    if (key %in% list_of_keys) {
      
      base_url <- "https://www.washingtonpost.com/wp-stat/dea-pain-pill-database/summary/arcos-"
      
      state_abb <- str_to_upper(state)
      county_name <- str_to_upper(county)
      #county_name <- gsub("-", " ", county_name)
      county_name <- gsub("%20", " ", county_name)
      county_relationship_file_only <- county_relationship_file %>% 
        filter(BUYER_STATE==state_abb & BUYER_COUNTY==county_name)
      
      if (!state_abb %in% county_relationship_states | !county_name %in% county_relationship_counties) {
        return(list(error="No such place. Do you have the county or state name spelled right? https://www.washingtonpost.com/graphics/2019/investigations/dea-pain-pill-database/#download-resources"))
        
      } else {
        county_fips <- county_relationship_file_only %>% pull(countyfips)
        
        state_abb <- str_to_lower(state_abb)
        county_name <- str_to_lower(county_name)
        county_name <- gsub(" ", "-", county_name)
        
        url <- paste0(base_url, state_abb, "-", county_name, "-", county_fips, "-distributor.tsv")
        
        #df <- vroom(url)
        #return(df)
        res$status <- 302
        res$setHeader('Location', url)
        return(res);
      }
      
      
    } else {
      return(list(error="Authentication required. Did you include an API key?"))
    }
  }
}


### Total pills for each pharmacy in a state

#' Returns all pharmacy totals by state as a tsv
#' @param state If provided, filter the data to only this state (e.g. 'WV')
#' @param key Key needed to make query successful
#' @param res Response object (leave blank)
#' @tag summary
#' @get /total_pharmacies_state
function(state, key, res){
  
  if (missing(key)) {
    return(list(error="Authentication required. Did you include an API key?"))
  } else {
    if (key %in% list_of_keys) {
      
      base_url <- "https://www.washingtonpost.com/wp-stat/dea-pain-pill-database/summary/arcos-"
      
      state_abb <- str_to_upper(state)
      if (!state_abb %in% county_relationship_states) {
        return(list(error="No such place. Do you have the state abbreviation spelled right? https://www.washingtonpost.com/graphics/2019/investigations/dea-pain-pill-database/#download-resources"))
        
      } else {
        
        state_abb <- str_to_lower(state_abb)
        
        url <- paste0(base_url, state_abb, "-county-pharmacy.tsv")
        
        #df <- vroom(url)
        #return(df)
        res$status <- 302
        res$setHeader('Location', url)
        return(res);
      }
      
      
    } else {
      return(list(error="Authentication required. Did you include an API key?"))
    }
  }
}


### Total pills for each Manufacturer to a state

#' Returns all Manufacturer totals by state as a tsv
#' @param state If provided, filter the data to only this state (e.g. 'WV')
#' @param key Key needed to make query successful
#' @param res Response object (leave blank)
#' @tag summary
#' @get /total_manufacturers_state
function(state, key, res){
  
  if (missing(key)) {
    return(list(error="Authentication required. Did you include an API key?"))
  } else {
    if (key %in% list_of_keys) {
      
      base_url <- "https://www.washingtonpost.com/wp-stat/dea-pain-pill-database/summary/arcos-"
      
      state_abb <- str_to_upper(state)
      
      if (!state_abb %in% county_relationship_states) {
        return(list(error="No such place. Do you have the state name abbreviation spelled right? https://www.washingtonpost.com/graphics/2019/investigations/dea-pain-pill-database/#download-resources"))
        
      } else {
        
        state_abb <- str_to_lower(state_abb)
        
        url <- paste0(base_url, state_abb, "-county-labeler.tsv")
        
        #df <- vroom(url)
        #return(df)
        res$status <- 302
        res$setHeader('Location', url)
        return(res);
      }
      
      
    } else {
      return(list(error="Authentication required. Did you include an API key?"))
    }
  }
}


### Total pills for each Distributor to a state

#' Returns all Distributor totals by state as a tsv
#' @param state If provided, filter the data to only this state (e.g. 'WV')
#' @param key Key needed to make query successful
#' @param res Response object (leave blank)
#' @tag summary
#' @get /total_distributors_state
function(state, key, res){
  
  if (missing(key)) {
    return(list(error="Authentication required. Did you include an API key?"))
  } else {
    if (key %in% list_of_keys) {
      
      base_url <- "https://www.washingtonpost.com/wp-stat/dea-pain-pill-database/summary/arcos-"
      
      state_abb <- str_to_upper(state)
      
      if (!state_abb %in% county_relationship_states) {
        return(list(error="No such place. Do you have the state abbreviation spelled right? https://www.washingtonpost.com/graphics/2019/investigations/dea-pain-pill-database/#download-resources"))
        
      } else {
        county_fips <- county_relationship_file_only %>% pull(countyfips)
        
        state_abb <- str_to_lower(state_abb)
        
        url <- paste0(base_url, state_abb, "-county-distributor.tsv")
        
        #df <- vroom(url)
        #return(df)
        res$status <- 302
        res$setHeader('Location', url)
        return(res);
      }
      
      
    } else {
      return(list(error="Authentication required. Did you include an API key?"))
    }
  }
}

### Annual pharmacy and practitioner dosages

#' Returns summarized annual dosages of pharmacies and practitioners by state and county
#' @param key Key needed to make query successful
#' @param state If provided, filter the data to only this state (e.g. 'WV')
#' @param county If provided, filter the data to only this county (e.g. 'Mingo')
#' @tag summary
#' @get /combined_buyer_annual
function(state, county, key){
  
  if (missing(key)) {
    return(list(error="Authentication required. Did you include an API key?"))
  } else {
    if (key %in% list_of_keys) {
      
      pharm_annual <- read_csv("data/buyer_annual.csv")
      
      if (!missing(state)) {
        
        state <- str_to_upper(state)
        
        if (state %in% county_relationship_states) {
          pharm_annual <- filter(pharm_annual, BUYER_STATE==state)
          
        } else {
          return(list(error="State abbreviation not found"))
          
        }
        
      }
      
      
      if (!missing(county)) {
        
        county <- str_to_upper(county)
        county <- gsub("-", " ", county)
        county <- gsub("%20", " ", county)
        if (county %in% county_relationship_counties) {
          pharm_annual <- filter(pharm_annual, BUYER_COUNTY==county)
          
        } else {
          return(list(error="County not found"))
          
        }
        
      }
      
      return(pharm_annual)
      
    } else {
      return(list(error="Authentication required. Did you include an API key?"))
    }
    
  }
  
}

### Monthly pharmacy dosages

#' Returns dosages by pharmacy or practitioner by county, state, and year
#' @param key Key needed to make query successful
#' @param state If provided, filter the data to only this state (e.g. 'WV')
#' @param county If provided, filter the data to only this county (e.g. 'Mingo')
#' @param year Filter the data to only this year (e.g. 2009)
#' @tag summary
#' @get /combined_buyer_monthly
function(state, county, year, key){
  
  if (missing(key)) {
    return(list(error="Authentication required. Did you include an API key?"))
  } else {
    if (!key %in% list_of_keys) {
      return(list(error="Authentication required. Did you include an API key?"))
    } else {
      if (!missing(year)){
        
        if (as.numeric(year) <= 2012 & as.numeric(year) >=2006) {
          
          pharm_monthly <- read_csv(paste0("data/buyer_monthly", year, ".csv"))
          
          if (!missing(state)) {
            
            state <- str_to_upper(state)
            
            if (state %in% county_relationship_states) {
              pharm_monthly <- filter(pharm_monthly, BUYER_STATE==state)
              
            } else {
              return(list(error="State abbreviation not found"))
              
            }
            
          }
          
          
          if (!missing(county)) {
            
            county <- str_to_upper(county)
            county <- gsub("-", " ", county)
            county <- gsub("%20", " ", county)
            if (county %in% county_relationship_counties) {
              pharm_monthly <- filter(pharm_monthly, BUYER_COUNTY==county)
              
            } else {
              return(list(error="County not found"))
              
            }
            return(pharm_monthly)
            
          } 
        } else {
          return(list(error="Only data between 2006 and 2012 currently available"))
        }
      } else {
        return(list(error="Year needed"))
      }
    } 
  }
} 


### All Raw

#' Returns all data as a tsv
#' @param key Key needed to make query successful
#' @param res Response object (leave blank)
#' @get /all_the_data
function(key, res){
  
  if (missing(key)) {
    return(list(error="Authentication required. Did you include an API key?"))
  } else {
    if (key %in% list_of_keys) {
      
      url <- "https://d2ty8gaf6rmowa.cloudfront.net/dea-pain-pill-database/bulk/arcos_all_washpost.tsv.gz"
      
      #df <- vroom(url)
      #return(df)
      res$status <- 302
      res$setHeader('Location', url)
      return(res);
      
      
      
    } else {
      return(list(error="Authentication required. Did you include an API key?"))
    }
  }
}

# #' @get /docs/
# function() {
# return(pr$swaggerFile());
# }

#' Health check for ALB (Part of deployment process, not part of the API)
#' @tag z_ignore
#' @get /healthcheck
function() {
  return(list(message="Everything is fine."));
}


#* @filter cors
cors <- function(res) {
  res$setHeader("Access-Control-Allow-Origin", "*")
  plumber::forward()
}

#  pr <- plumb("plumber.R")  # Where 'plumber.R' is the location of the file shown above
#  pr$run(port=8000)
