library(shiny)
library(rmarkdown)

# ---- 1. Render the Rmd once, before the app starts ----

project_root <- getwd()

rmd_path  <- file.path(project_root, "notebooks", "iati_analysis.Rmd")
xml_path  <- file.path(project_root, "data", "sample_iati.xml")

# Output goes to a temp file
out_file  <- tempfile(pattern = "iati_report_", fileext = ".html")
out_dir   <- dirname(out_file)

rmarkdown::render(
  input         = rmd_path,
  params        = list(
    iati_file = xml_path,
    group_by  = "recipient-country"
  ),
  output_file   = basename(out_file),
  output_dir    = out_dir,
  quiet         = FALSE,
  knit_root_dir = project_root
)

report_path <- file.path(out_dir, basename(out_file))

# ---- 2. Minimal UI: just show the report ----

ui <- fluidPage(
  titlePanel("IATI Analysis Report"),
  htmlOutput("report")
)

server <- function(input, output, session) {
  output$report <- renderUI({
    if (file.exists(report_path)) {
      includeHTML(report_path)
    } else {
      HTML("<strong>Report file not found.</strong>")
    }
  })
}

shinyApp(ui, server)
