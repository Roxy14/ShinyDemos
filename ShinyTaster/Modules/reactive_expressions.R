reactiveExpressionsUI <- function(id) {
  ns <- NS(id)

  div(
    class = "main-container",

	
	    # --- Section Banner ---
    section_banner(
      text = "Reactivity in Shiny",
      type = "basics"
    ),

	
	dark_card(
  h5("Understanding Reactive Thinking"),
  p("Reactive expressions are Shiny’s way of caching work. They let you compute a value 
     once and reuse it many times without repeating the same calculation. Whenever the 
     inputs they depend on change, they automatically re-run — but only when needed."),
  p("Think of them as smart variables: they remember their results, update themselves 
     when required, and keep your app efficient and responsive.")
),


    p("Reactivity is the core of Shiny. It allows outputs to automatically update 
       when inputs change."),
	

    dark_card(
      h5("Reactive Expressions"),
      p("Reactive expressions let you reuse computed values efficiently."),
	        div(
        class = "three-quarter",
      code_block(
        ns("reactive_example"),
        paste(
"server <- function(input, output, session) {

  data_filtered <- reactive({
    mtcars[mtcars$cyl == input$cyl, ]
  })

  output$table <- renderTable({
    data_filtered()
  })
}", collapse = "\n"
        ),
        height = "250px"
      )
	  )
    ),

    dark_card(
      h5("Live Demo"),
      p("Choose a number of cylinders to filter the dataset."),
      selectInput(ns("cyl"), "Cylinders:", choices = c(4, 6, 8)),
      DTOutput(ns("table"))
    )
  )
}


reactiveExpressionsServer <- function(id) {
  moduleServer(id, function(input, output, session) {

    data_filtered <- reactive({
      mtcars[mtcars$cyl == input$cyl, ]
    })

    output$table <- DT::renderDT({
      df <- data_filtered()

      dt <- DT::datatable(
        df,
        options = list(
          pageLength = 5,
          dom = "t",
          ordering = FALSE
        )
      )

      DT::formatStyle(
        dt,
        "cyl",
        backgroundColor = "#4db8ff",
        color = "black",
        fontWeight = "bold"
      )
    })
  })
}

