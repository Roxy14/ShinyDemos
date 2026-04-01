observeEventUI <- function(id) {
  ns <- NS(id)

  div(
    class = "main-container",

	    # --- Section Banner ---
    section_banner(
      text = "Buttons & Triggers: observeEvent()",
      type = "basics"
    ),


dark_card(
  h5("When the User Triggers an Action"),
  p("observeEvent() runs code only when a specific event happens — 
     usually a button click. It ignores other reactive changes."),
  p("This makes it perfect for actions the user triggers deliberately, 
     like submitting a form, resetting a plot, or starting a calculation.")
)
,

    dark_card(
      h5("How observeEvent() Works"),
      p("This is perfect for buttons, toggles, and controlled updates."),
	  	        div(
        class = "three-quarter",
      code_block(
        ns("observeevent_example"),
        paste(
"observeEvent(input$go, {
  output$result <- renderText('Button clicked!')
})", collapse = "\n"
        ),
        height = "140px"
      )
	  )
    ),

    dark_card(
      h5("Live Demo"),
      p("Click the button to trigger observeEvent()."),
      actionButton(ns("go"), "Click Me"),
      h4(textOutput(ns("result")))
    )
  )
}


observeEventServer <- function(id) {
  moduleServer(id, function(input, output, session) {

    observeEvent(input$go, {
      output$result <- renderText({
        paste("Button clicked at", Sys.time())
      })
    })

  })
}
