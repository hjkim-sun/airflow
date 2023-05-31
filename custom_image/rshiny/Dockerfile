FROM rocker/shiny-verse:latest
COPY . /
WORKDIR /
EXPOSE 3838
CMD R -e 'shiny::runApp("global.R", port=3838, host="0.0.0.0")'
