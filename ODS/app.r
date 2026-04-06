library(shiny)
library(rmarkdown)

ui <- fluidPage(
  titlePanel("IATI Data Analysis Demonstration"),
  uiOutput("report_frame")
)

server <- function(input, output, session) {

  # GitHub raw URLs for your files
  xml_url <- "https://raw.githubusercontent.com/Roxy14/ShinyDemos/main/ODS/data/sample_iati.xml"
  rmd_url <- "https://raw.githubusercontent.com/Roxy14/ShinyDemos/main/ODS/iati_analysis.Rmd"

  # Download Rmd to a temp file
  rmd_file <- tempfile(fileext = ".Rmd")
  download.file(rmd_url, rmd_file, quiet = TRUE)

  # Knit the report to a temp HTML file
  out_file <- tempfile(fileext = ".html")
  rmarkdown::render(
    input = rmd_file,
    output_file = out_file,
    params = list(
      iati_file = xml_url,
      group_by = "recipient-country"
    ),
    envir = new.env(parent = globalenv())
  )

  # Make the folder containing the HTML accessible to Shiny
  addResourcePath("reports", dirname(out_file))

  # Show the report in an iframe
  output$report_frame <- renderUI({
    tags$iframe(
      src = paste0("reports/", basename(out_file)),
      width = "100%",
      height = "900px",
      style = "border:none;"
    )
  })
}

shinyApp(ui, server)
