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
source("modules/file_structure.R")
source("modules/full_app_example.R")

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
ui <- navbarPage(
  title = "Teach PySpark",
  theme = learn_theme,
  
  # Remove bright blue active tab
  header = tags$head(
    tags$style(HTML("
      .navbar-nav .nav-link.active {
        background-color: #e0e0e0 !important;
        color: #000 !important;
        border-radius: 6px;
      }
      .navbar-nav .nav-link {
        font-weight: 600;
      }
    "))
  ),
  
  tabPanel("1. Let's Get Started!", homeUI("home")),
  tabPanel("2. Laying the Groundwork", basicsUI("basics")),
  tabPanel("3. Reactivity: How Shiny Updates Itself", reactiveExpressionsUI("reactive")),
  tabPanel("4. Watching for Changes: observe()", observeUI("obs")),
  tabPanel("5. Buttons & Triggers: observeEvent()", observeEventUI("obsevent")),
  tabPanel("6. Putting It All Together", reactiveOverviewUI("overview")),
  tabPanel("7. How UI and Server Connect", reactiveFlowUI("flow")),
  tabPanel("8. Mini App: Step‑by‑Step Reactive Plot", reactiveMiniAppUI("mini")),
  tabPanel("9. Complete Mini App Code Example", fullAppExampleUI("example")),
  tabPanel("10. This Learning App File Structure", fileStructureUI("files"))
)

# ----------------------------------------------------
# SERVER
# ----------------------------------------------------
server <- function(input, output, session) {
  
  homeServer("home")
  basicsServer("basics")
  reactiveExpressionsServer("reactive")
  observeServer("obs")
  observeEventServer("obsevent")
  reactiveOverviewServer("overview")
  reactiveFlowServer("flow")
  
  reactiveMiniAppServer("mini")
  fileStructureServer("files")
  fullAppExampleServer("example")
}

shinyApp(ui, server)
