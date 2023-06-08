FROM rstudio/plumber

RUN mkdir -p /app/data

WORKDIR /app/

RUN R -e "install.packages(c('jsonlite','plumber','rcpp','rvest','stringr','tidyverse','xml2','vroom'))"

## Copy data
COPY ./data/ /app/data/
COPY ./docs/ /app/docs/

## Copy R files
COPY entrypoint.R /app/entrypoint.R
COPY ./plumber.R /app/plumber.R

EXPOSE 8000

CMD ["Rscript", "entrypoint.R"]
