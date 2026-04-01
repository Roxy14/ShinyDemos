# modules/reactive_graph.R

reactiveGraphUI <- function(id) {
  ns <- NS(id)
  
  tagList(
    section_banner("The Reactive Graph: Your Data Pipeline with Auto‑Updates", type = "reactive"),
    
    dark_card(
      h3("Two Analogies, One Clear Mental Model"),
      p("The reactive graph can feel abstract, so we explain it using two analogies together:"),
      
      tags$ul(
        tags$li(strong("A data pipeline"), " — shows the structure and dependencies."),
        tags$li(strong("A kitchen"), " — makes the idea intuitive and memorable.")
      ),
      
      p("Both describe the same thing: how Shiny decides what needs to re-run when something changes."),
      
      diagram_box(
        title = "Hybrid Analogy Overview",
        content = tagList(
          p("Inputs → Raw Data / Ingredients"),
          p("Reactives → Transformations / Prep Stations"),
          p("Outputs → Reports / Final Dishes")
        )
      )
    ),
    
    
    dark_card(
      h3("Inputs = Raw Data (or Ingredients)"),
      p("Inputs behave like raw data sources. When they change, anything downstream must update."),
      
      tags$ul(
        tags$li(code("input$dataset"), " → like loading a new CSV"),
        tags$li(code("input$filter"), " → like changing a dplyr filter"),
        tags$li(code("input$threshold"), " → like adjusting a model parameter")
      ),
      
      p("In the kitchen analogy, these are your ingredients — change them, and any dish using them must be updated.")
    ),
    
    
    dark_card(
      h3("Reactive Expressions = Data Transformations (or Prep Stations)"),
      p("A reactive expression is a transformation step. It processes inputs and produces a new value."),
      
      code_block(
        id = ns("transform_example"),
        code =
"filtered_data <- reactive({
  dplyr::filter(dataset(), value > input$threshold)
})"
      ),
      
      p("In the kitchen analogy, this is a prep station — chopping, mixing, boiling.")
    ),
    
    
    dark_card(
      h3("Outputs = Reports, Visualisations (or Final Dishes)"),
      p("Outputs are the final results: plots, tables, summaries. They depend on reactives, which depend on inputs."),
      
      code_block(
        id = ns("output_example"),
        code =
"output$plot <- renderPlot({
  ggplot(filtered_data(), aes(x, y)) + geom_line()
})"
      ),
      
      diagram_box(
        title = "Dependency Chain",
        content = tagList(
          p("Input → Transformation → Output"),
          p("input$threshold → filtered_data() → output$plot")
        )
      )
    ),
    
    
    dark_card(
      h3("The Reactive Graph = Your Dependency Pipeline"),
      p("Shiny keeps track of which transformations depend on which inputs, and which outputs depend on which transformations."),
      p("This is exactly like a DAG (directed acyclic graph) in data engineering."),
      p("In the kitchen analogy, it’s the chef knowing which dishes use which ingredients.")
    ),
    
    
    dark_card(
      h3("Invalidation = When Upstream Data Changes"),
      p("When an input changes, Shiny marks all downstream steps as 'invalid' — meaning they need to be recomputed."),
      
      p(strong("Example:")),
      tags$ul(
        tags$li("User changes ", code("input$threshold")),
        tags$li("Shiny invalidates ", code("filtered_data()")),
        tags$li("Shiny invalidates ", code("output$plot")),
        tags$li("Next time the plot is needed, Shiny recomputes everything downstream")
      ),
      
      p("This is identical to how a data pipeline re-runs downstream steps when upstream data changes."),
      p("And in the kitchen analogy, it’s like changing an ingredient — any dish using it must be remade.")
    ),
    
    
    # ---------------------------------------------------------
    # NEW: Readable Definition-List Style Section
    # ---------------------------------------------------------
    dark_card(
      h3("Why This Matters"),
      p("Once you understand the reactive graph as a data pipeline with automatic updates, Shiny’s behaviour becomes predictable. Here’s why each part works the way it does:"),
      
      div(class = "explain-list",
          
          div(
            strong("Why some things re-run and others don’t"),
            p("Shiny only recomputes steps that depend on the changed input — just like a pipeline only recomputes downstream steps, or a kitchen only remakes dishes using the changed ingredient.")
          ),
          
          div(
            strong("Why isolate() stops automatic updates"),
            p("It tells Shiny not to register a dependency — like freezing a step in a pipeline or telling a chef not to remake a dish even if an ingredient changes.")
          ),
          
          div(
            strong("Why eventReactive() exists"),
            p("Some steps should run only when a specific trigger happens — like cooking a dish only when someone orders it, not every time an ingredient changes.")
          ),
          
          div(
            strong("Why observers don’t return values"),
            p("They perform side effects, not data transformations — like sending a message, logging something, or ringing a bell in the kitchen.")
          ),
          
          div(
            strong("How to optimise performance"),
            p("Reduce unnecessary dependencies so fewer steps need to re-run — just like simplifying a data workflow or avoiding extra prep work.")
          )
      ),
      
      p("The hybrid analogy gives you both the intuition and the structure behind every Shiny app you will ever build.")
    )
  )
}


reactiveGraphServer <- function(id) {
  moduleServer(id, function(input, output, session) {
    # No server logic needed for this conceptual page
  })
}
