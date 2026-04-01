library(shiny)
library(bslib)
library(shinyAce)
library(magrittr)
library(DT)

# Load reusable components
source("components/dark_card.R")
source("components/code_block.R")
source("components/diagram_box.R")
source("components/section_banner.R")

# Load modules
source("modules/home.R")
source("modules/basics.R")
source("modules/reactive_expressions.R")
source("modules/helpers.R")
source("modules/observe.R")
source("modules/observe_event.R")
source("modules/reactive_flow.R")
source("modules/reactive_overview.R")
source("modules/mini_app.R")
source("modules/file_structure.R")       # ⭐ NEW
source("modules/full_app_example.R")     # ⭐ NEW

# Theme
learn_theme <- bs_theme(
  version = 5,
  bootswatch = "flatly",
  primary = "#4db8ff",
  secondary = "#2E7D32",
  base_font = font_google("Inter"),
  heading_font = font_google("Inter"),
  code_font = font_google("Fira Code")
)

# ----------------------------------------------------
# UI
# ----------------------------------------------------
ui <- fluidPage(
  theme = learn_theme,
  
  tags$head(
    tags$link(rel = "stylesheet", type = "text/css", href = "styles.css")
  ),
  
  sidebarLayout(
    sidebarPanel(
      width = 3,
      selectInput(
        "topic",
        "Choose a topic:",
        choices = c(
          "Let's Get Started!",
          "Laying the Groundwork",
          "Reactivity: How Shiny Updates Itself",
          "Watching for Changes: observe()",
          "Buttons & Triggers: observeEvent()",
          "Putting It All Together",
          "How UI and Server Connect",
          "Mini App: Step‑by‑Step Reactive Plot",
          "Complete Mini App Code Example",         # ⭐ NEW
          "This Learning App File Structure"      # ⭐ NEW

        )
      )
    ),
    
    mainPanel(
      width = 9,
      uiOutput("topic_ui")
    )
  )
)

# ----------------------------------------------------
# SERVER
# ----------------------------------------------------
server <- function(input, output, session) {
  
  output$topic_ui <- renderUI({
    switch(input$topic,
           
           "Let's Get Started!" = homeUI("home"),
           "Laying the Groundwork" = basicsUI("basics"),
           "Reactivity: How Shiny Updates Itself" = reactiveExpressionsUI("reactive"),
           "Watching for Changes: observe()" = observeUI("obs"),
           "Buttons & Triggers: observeEvent()" = observeEventUI("obsevent"),
           "Putting It All Together" = reactiveOverviewUI("overview"),
           "How UI and Server Connect" = reactiveFlowUI("flow"),
           
           "Mini App: Step‑by‑Step Reactive Plot" = reactiveMiniAppUI("mini"),
           "Complete Mini App Code Example" = fullAppExampleUI("example"),      # ⭐ NEW
           "This Learning App File Structure" = fileStructureUI("files")       # ⭐ NEW

    )
  })
  
  homeServer("home")
  basicsServer("basics")
  reactiveExpressionsServer("reactive")
  observeServer("obs")
  observeEventServer("obsevent")
  reactiveOverviewServer("overview")
  reactiveFlowServer("flow")
  
  reactiveMiniAppServer("mini")
  fileStructureServer("files")          # ⭐ NEW
  fullAppExampleServer("example")       # ⭐ NEW
}

shinyApp(ui, server)
