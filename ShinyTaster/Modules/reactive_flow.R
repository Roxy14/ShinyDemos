reactiveFlowUI <- function(id) {
  ns <- NS(id)
  
  tagList(
    section_banner("How the UI and Server Talk to Each Other", type = "reactive"),
    
    dark_card(
      h5("Why This Matters"),
      p("Every Shiny app—simple or complex—runs on the same communication loop. 
         Once you understand how the UI and server talk to each other, you can 
         reason about any Shiny app with confidence.")
    ),
    
    dark_card(
      h5("The Shiny Communication Loop"),
      p("Here’s the full cycle at a glance:"),
      div(class = "diagram-wrapper",
          pre(class = "diagram-block",
"User → Input → Server → Output → UI → User"
          )
      )
    ),
    
    dark_card(
      h5("1. The user interacts with the UI"),
      p("The loop begins when the user moves a slider, types text, clicks a button, 
         or selects an option. These actions change input values, and every input 
         in Shiny is reactive—Shiny instantly knows something changed.")
    ),
    
    dark_card(
      h5("2. Inputs send updated values to the server"),
      p("Inside the server, you access inputs like ", code("input$slider"), ", ",
        code("input$text"), ", and ", code("input$select"), ". These values update 
        automatically. Inputs push updates to the server whenever they change.")
    ),
    
    dark_card(
      h5("3. The server reacts and recalculates"),
      p("Reactive tools respond to these changes:"),
      tags$ul(
        tags$li(code("reactive()"), " creates new reactive values"),
        tags$li(code("observe()"), " performs side‑effects"),
        tags$li(code("observeEvent()"), " responds to specific triggers")
      ),
      p("Anything that depends on a changed input re-runs automatically. This is 
         the core of Shiny’s reactive engine.")
    ),
    
    dark_card(
      h5("4. Outputs update and return results to the UI"),
      p("Output functions like ", code("renderPlot()"), ", ", code("renderText()"), 
        ", and ", code("renderTable()"), " re-run when their reactive dependencies 
        change. They send updated results back to the UI.")
    ),
    
    dark_card(
      h5("5. The UI updates, and the user sees the change"),
      p("The UI instantly reflects the new output—the plot redraws, the text updates, 
         the table refreshes. The user sees the change and interacts again, restarting 
         the loop.")
    )
  )
}


reactiveFlowServer <- function(id) {
  moduleServer(id, function(input, output, session) {
    # This module is currently informational only.
    # No reactive logic is required yet.
    # Keeping the server empty is perfectly valid.
  })
}
