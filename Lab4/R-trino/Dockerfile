FROM rocker/r-ver:4.4.1

RUN apt-get update && apt-get install -y --no-install-recommends \
    libssl-dev \
    libcurl4-openssl-dev \
    libxml2-dev \
    && rm -rf /var/lib/apt/lists/*

RUN R -e "install.packages(c('DBI', 'RPresto'), repos='https://packagemanager.posit.co/cran/__linux__/jammy/latest'); \
          if (!requireNamespace('RPresto', quietly=TRUE)) stop('RPresto install failed')"

WORKDIR /app
COPY create_reports.R /app/create_reports.R

CMD ["Rscript", "/app/create_reports.R"]
