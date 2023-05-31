if (!require(shiny)) {install.packages("shiny"); library(shiny)}
if (!require(DBI)) {install.packages("DBI"); library(DBI)}
if (!require(glue)) {install.packages("glue"); library(glue)}
if (!require(ggplot2)) {install.packages("ggplot2"); library(ggplot2)}


source("./ui.R", local = TRUE)
source("./server.R", local = TRUE)


shinyApp(ui, server)
