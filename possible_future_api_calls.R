
### Annual raw data by specific drug and state and county

#' Returns raw data filtered by specific drug and state and county
#' Refer to drug_list() for complete list of drug options
#' @param drug Filter the data to only this drug (e.g. 'OXYCODONE')
#' @param key Key needed to make query successful
#' @param year Filter the data to only this year (e.g. 2009)
#' @param county If provided, filter the data to only this county (e.g. 'Mingo')
#' @param state Filter the data to only this state (e.g. 'WV')
#' @tag raw
#' @get /v1/drug_county_year
function(drug, county, state, year, key){
  
  if (missing(key)) {
    return(list(error="Authentication required. Did you include an API key?"))
  } else {
    if (!key %in% list_of_keys) {
      return(list(error="Authentication required. Did you include an API key?"))
    } else {
      if (!missing(drug) & str_to_upper(drug) %in% drug_types) {  
        
        if (!missing(year)){
          
          if (as.numeric(year) <= 2014 & as.numeric(year) >=2006) {
            
            if (!missing(state)) {
              
              state <- str_to_upper(state)
              
              if (state %in% county_relationship_states) {
                
                if (!missing(county)) {
                  
                  county <- str_to_upper(county)
                  county <- gsub("-", " ", county)
                  county <- gsub("%20", " ", county)
                  if (county %in% county_relationship_counties) {
                    base_url <- "https://wp-stat.s3.amazonaws.com/dea-pain-pill-database/drugs/"
                    
                    state_abb <- str_to_upper(state)
                    drug_lookup <- str_to_upper(gsub(", ", "_", drug))
                    
                    url <- paste0(base_url, drug_lookup, "-", state_abb, "-", year, ".csv")
                    df <- vroom(url)
                    
                    return(df)  
                    
                  } else {
                    return(list(error="County not found"))
                    
                  }
                  
                } 
                
                else  {
                  return(list(error="County not found"))
                  
                }
                else {
                  return(list(error="State abbreviation not found"))
                  
                }
                
                #}
                
              } else {
                return(list(error="Only data between 2006 and 2014 currently available"))
              }
            } else {
              return(list(error="Year needed"))
            }
          } else {
            return(list(error="Drug needed. Use the drug_list() function to see the options."))
            
          }
        }
      }
    } 
    
    
    
    ### Annual raw data by specific drug and state
    
    #' Returns raw data filtered by specific drug and state.
    #' Refer to drug_list() for complete list of drug options
    #' @param drug Filter the data to only this drug (e.g. 'OXYCODONE')
    #' @param key Key needed to make query successful
    #' @param year Filter the data to only this year (e.g. 2009)
    #' @param state Filter the data to only this state (e.g. 'WV')
    #' @tag raw
    #' @get /v1/drug_state_year
    function(drug, state, year, key){
      
      if (missing(key)) {
        return(list(error="Authentication required. Did you include an API key?"))
      } else {
        if (!key %in% list_of_keys) {
          return(list(error="Authentication required. Did you include an API key?"))
        } else {
          if (!missing(drug) & str_to_upper(drug) %in% drug_types) {  
            
            if (!missing(year)){
              
              if (as.numeric(year) <= 2014 & as.numeric(year) >=2006) {
                
                if (!missing(state)) {
                  
                  state <- str_to_upper(state)
                  
                  if (state %in% county_relationship_states) {
                    base_url <- "https://wp-stat.s3.amazonaws.com/dea-pain-pill-database/drugs/"
                    
                    state_abb <- str_to_upper(state)
                    drug_lookup <- str_to_upper(gsub(", ", "_", drug))
                    
                    url <- paste0(base_url, drug_lookup, "-", state_abb, "-", year, ".csv")
                    df <- vroom(url)
                    
                    return(df)                  
                  } else {
                    return(list(error="State abbreviation not found"))
                    
                  }
                  
                }
                
              } else {
                return(list(error="Only data between 2006 and 2014 currently available"))
              }
            } else {
              return(list(error="Year needed"))
            }
          } else {
            return(list(error="Drug needed. Use the drug_list() function to see the options."))
            
          }
        }
      }
    } 
    