FROM rocker/r-ver:4.4.1

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl-dev \
    libcurl4-openssl-dev \
    libxml2-dev \
    && rm -rf /var/lib/apt/lists/*

ARG CRAN_REPO=https://cloud.r-project.org

RUN R -e "options(repos = c(CRAN = Sys.getenv('CRAN_REPO', unset = '${CRAN_REPO}')), timeout = 300); \
          install.packages(c('DBI', 'RPresto')); \
          if (!requireNamespace('DBI', quietly=TRUE)) stop('DBI install failed'); \
          if (!requireNamespace('RPresto', quietly=TRUE)) stop('RPresto install failed')"

WORKDIR /app
COPY create_reports.R /app/create_reports.R

CMD ["Rscript", "/app/create_reports.R"]
