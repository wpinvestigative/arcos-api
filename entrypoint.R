# entrypoint.R
library(jsonlite)
library(plumber)
library(stringr)
library(tidyverse)
library(vroom)

# When setting the docs, `index` and `static` function arguments can be supplied
# * via `pr_set_docs()`
# * or through URL query string variables
pr_test <- pr("plumber.R") %>% 
  # (function(pr) pr_get(pr, "/arcos/openapi/", pr$getApiSpec))() %>%
  pr_static("/arcos/__docs__/", "./docs") %>%
  pr_run(
    host='0.0.0.0',
    port = 8000,
    docs = TRUE
  )