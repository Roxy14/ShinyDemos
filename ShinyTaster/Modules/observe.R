observeUI <- function(id) {
  ns <- NS(id)

  div(
    class = "main-container",


	    # --- Section Banner ---
    section_banner(
      text = "Watching for Changes: observe()",
      type = "basics"
    ),


dark_card(
  h5("Responding to the User"),
  p("Unlike reactive expressions, observe() does not return a value. 
     It runs code for its side‑effects whenever its reactive dependencies change."),
  p("Think of observe() as a way to tell Shiny: 'When this changes, do something.' 
     It’s how your app performs actions, updates UI elements, writes logs, 
     or triggers other behaviour in response to user input.")
)
,

    dark_card(
      h5("How observe() Works"),
      p("observe() is like a reactive listener. Whenever an input it depends on changes, 
         it re-runs the code inside it."),
		 
		 	  	        div(
        class = "three-quarter",
      code_block(
        ns("observe_example"),
        paste(
"observe({
  print(input$name)
})", collapse = "\n"
        ),
        height = "120px"
      )
	  )
    ),

    dark_card(
      h5("Live Demo"),
      p("Type something — observe() will print to the console every time you change the input."),
      textInput(ns("txt"), "Type here:"),
      verbatimTextOutput(ns("console"))
    )
  )
}


observeServer <- function(id) {
  moduleServer(id, function(input, output, session) {

    observe({
      output$console <- renderText({
        paste("observe() detected change:", input$txt)
      })
    })

  })
}
