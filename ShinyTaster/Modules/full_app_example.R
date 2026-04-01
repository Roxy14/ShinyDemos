fullAppExampleUI <- function(id) {
  ns <- NS(id)

  div(
    class = "main-container",

    # Page banner
    section_banner("A Small Complete Shiny App", type = "basics"),

    # ------------------------------------------------------------
    # INTRO CARD — explains why this page exists
    # ------------------------------------------------------------
    dark_card(
      h5("Putting the Pieces Together"),
      p("The reactive plot example on the previous page shows small building blocks that would 
         make up a larger Shiny app. Real apps are made by combining many small pieces — modules, 
         reusable UI components, and supporting files — all organised in a simple folder structure. 
         The example below shows how those pieces fit together in a small, complete app.")
    ),

    # ------------------------------------------------------------
    # PROJECT STRUCTURE — shows how a real app is organised
    # ------------------------------------------------------------
    dark_card(
      h5("Project Structure"),
      code_block(
        ns("example_structure"),
"
my_app/
│
├── app.R                 # Main entry point for the app
│
├── modules/              # Feature-level building blocks
│   └── reactive_plot.R   # Contains UI + server for the plot
│
├── components/           # Reusable UI helpers
│   └── dark_card.R       # Same dark card used in this training app
│
└── www/                  # Static assets (CSS, images, JS)
    └── styles.css        # Custom styling for the app
",
        height = "300px"
      )
    ),

    # ------------------------------------------------------------
    # MODULE EXAMPLE — shows the reactive plot as a module
    # ------------------------------------------------------------
    dark_card(
      h5("Module: reactive_plot.R"),
      p("This module contains a simple reactive plot — the same pattern you saw earlier."),
      code_block(
        ns("example_module"),
"
# UI for the reactive plot module
reactivePlotUI <- function(id) {
  ns <- NS(id)
  tagList(
    sliderInput(ns('n'), 'Number of points:', 10, 200, 50),
    plotOutput(ns('plot'))
  )
}

# Server logic for the reactive plot module
reactivePlotServer <- function(id) {
  moduleServer(id, function(input, output, session) {

    # Reactive expression: updates automatically when the slider changes
    n <- reactive({ input$n })

    # Render the plot using the reactive value
    output$plot <- renderPlot({
      plot(rnorm(n()), col = '#4db8ff', pch = 19)
    })
  })
}
",
        height = "500px"
      )
    ),

    # ------------------------------------------------------------
    # FULL APP EXAMPLE — shows how the module is used in app.R
    # ------------------------------------------------------------
    dark_card(
      h5("Example app.R"),
      p("This is the complete app that uses the module and a reusable UI component."),
      code_block(
        ns("example_app"),
"
library(shiny)

# Load the module and a reusable UI component
source('modules/reactive_plot.R')
source('components/dark_card.R')

# UI layout
ui <- fluidPage(
  dark_card(
    h3('My Reactive Plot App'),
    reactivePlotUI('plot1')   # Insert the module's UI
  )
)

# Server logic
server <- function(input, output, session) {
  reactivePlotServer('plot1')  # Activate the module's server logic
}

# Launch the app
shinyApp(ui, server)
",
        height = "500px"
      )
    )
  )
  
}

fullAppExampleServer <- function(id) {
  moduleServer(id, function(input, output, session) {})
}
