ui <- fluidPage(
	tags$h1("corona19"),
	sidebarPanel(
		dateRangeInput("dates",
			       "Date range",
			       start = as.Date("2023-01-01"),
			       end = Sys.Date()),
		br(),
		br()
	),
	mainPanel(plotOutput("daily_confirmed"), plotOutput("total_confirmed"))
)

