basicsUI <- function(id) {
  ns <- NS(id)

  div(
    class = "main-container",

    # --- Section Banner ---
    section_banner(
      text = "Laying the Groundwork",
      type = "basics"
    ),

    dark_card(
      p("Before diving into details, it helps to understand the big picture: every Shiny app 
         is built from two parts — the UI, which displays things to the user, and the server, 
         which performs the work. Reactivity connects the two, keeping everything in sync 
         automatically. This page introduces that structure so the rest of Shiny makes sense.")
    ),

    dark_card(
      h5("The Structure of a Shiny App"),
      p("Every Shiny app follows this pattern:"),

      # --- 3/4 WIDTH CODE BLOCK ---
      div(
        class = "three-quarter",
        code_block(
          ns("code_structure"),
          paste(
"library(shiny)

ui <- fluidPage(
  textInput('name', 'Enter your name'),
  textOutput('greeting')
)

server <- function(input, output, session) {
  output$greeting <- renderText({
    paste('Hello', input$name)
  })
}

shinyApp(ui, server)", 
            collapse = "\n"
          ),
          height = "300px"
        )
      )
    ),

    dark_card(
      h5("How UI, Reactivity, and Server Work Together"),
      p("This diagram shows how information flows through a Shiny app. 
         The UI collects inputs, the server performs work, and the reactive system 
         keeps everything synchronized automatically."),

      # --- HALF-SIZE DIAGRAM ---
      div(
        class = "half-diagram",
        diagram_box("ui_server_diagram.png")
      )
    ),

    dark_card(
      h5("Live Demo: Your First Reactive App"),
      p("Type your name and watch the output update automatically."),
      uiOutput(ns("demo"))
    )
  )
}


basicsServer <- function(id) {
  moduleServer(id, function(input, output, session) {

    # --- Simple reactive demo for the Basics page ---
    output$demo <- renderUI({
      tagList(
        textInput(session$ns("name"), "Enter your name"),
        textOutput(session$ns("greeting"))
      )
    })

    output$greeting <- renderText({
      req(input$name)
      paste("Hello", input$name)
    })

  })
}
