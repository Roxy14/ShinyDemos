reactiveMiniAppUI <- function(id) {
  ns <- NS(id)

  tagList(

    section_banner("Let’s Build a Mini App: Reactive Patterns", type = "basics"),

    dark_card(
      h5("What We’re Building"),
      p("We’ll build a small app that demonstrates the three core reactive patterns in Shiny:"),
      tags$ul(
        tags$li(strong("Reactive values"), " — update automatically"),
        tags$li(strong("Event-driven updates"), " — update only when triggered"),
        tags$li(strong("Side effects"), " — respond to changes without producing output")
      ),
      p("Each step builds on the last so you can clearly see how Shiny’s reactivity works.")
    ),

    # ------------------------------------------------------------
    # STEP 1 — REACTIVE PLOT
    # ------------------------------------------------------------
    dark_card(
      h5("Step 1: A Reactive Plot (Automatic Updates)"),
      p("This plot updates automatically whenever the slider changes. 
         This is the most common and natural way to build reactive outputs in Shiny."),

      sliderInput(ns("n1"), "Number of points:", min = 10, max = 200, value = 50),
      plotOutput(ns("plot1")),

      h6("Code"),
      code_block(
        ns("code_step1"),
"
n <- reactive({ input$n1 })

output$plot1 <- renderPlot({
  plot(rnorm(n()), col = '#4db8ff', pch = 19)
})
",
        height = "220px"
      )
    ),

    # ------------------------------------------------------------
    # STEP 2 — EVENT-DRIVEN PLOT
    # ------------------------------------------------------------
    dark_card(
      h5("Step 2: A Button-Controlled Plot (Event-Driven Updates)"),
      p("Here the plot updates only when the user clicks the button. 
         This is useful when you want the user to control when work happens."),

      sliderInput(ns("n2"), "Number of points:", min = 10, max = 200, value = 50),
      actionButton(ns("go2"), "Update Plot"),
      plotOutput(ns("plot2")),

      h6("Code"),
      code_block(
        ns("code_step2"),
"
vals <- reactiveValues(n = 50)

observeEvent(input$go2, {
  vals$n <- input$n2
})

output$plot2 <- renderPlot({
  plot(rnorm(vals$n), col = '#2E7D32', pch = 19)
})
",
        height = "260px"
      )
    ),

    # ------------------------------------------------------------
    # STEP 3 — OBSERVE() SIDE EFFECT
    # ------------------------------------------------------------
    dark_card(
      h5("Step 3: observe() for Side Effects"),
      p("observe() is used for side effects — things that happen in response to changes, 
         but do not produce values for outputs. Here we log a message every time the slider changes."),

      sliderInput(ns("n3"), "Move the slider to trigger observe()", min = 1, max = 100, value = 50),
      verbatimTextOutput(ns("log")),

      h6("Code"),
      code_block(
        ns("code_step3"),
"
vals <- reactiveValues(log = \"Waiting for changes...\")

observe({
  vals$log <- paste(\"Slider changed to\", input$n3)
})

output$log <- renderText(vals$log)
",
        height = "240px"
      )
    )
  )
}



# ============================================================
# SERVER
# ============================================================

reactiveMiniAppServer <- function(id) {
  moduleServer(id, function(input, output, session) {

    # ------------------------------------------------------------
    # STEP 1 — reactive()
    # ------------------------------------------------------------
    n1 <- reactive({ input$n1 })

    output$plot1 <- renderPlot({
      plot(rnorm(n1()), col = "#4db8ff", pch = 19)
    })


    # ------------------------------------------------------------
    # STEP 2 — observeEvent()
    # ------------------------------------------------------------
    vals2 <- reactiveValues(n = 50)

    observeEvent(input$go2, {
      vals2$n <- input$n2
    })

    output$plot2 <- renderPlot({
      plot(rnorm(vals2$n), col = "#2E7D32", pch = 19)
    })


    # ------------------------------------------------------------
    # STEP 3 — observe() side effect
    # ------------------------------------------------------------
    vals3 <- reactiveValues(log = "Waiting for changes...")

    observe({
      vals3$log <- paste("Slider changed to", input$n3)
    })

    output$log <- renderText(vals3$log)
  })
}
