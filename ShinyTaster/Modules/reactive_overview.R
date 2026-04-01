# ============================================================
# UI
# ============================================================

reactiveOverviewUI <- function(id) {
  ns <- NS(id)

  tagList(

    section_banner(
      text = "They All React… So What’s the Difference?",
      type = "basics"
    ),

    p(
      style = "margin-top: -5px; margin-bottom: 20px;",
      "Three tools. Similar jobs. Different purposes. 
       This page shows how they fit together — and when to use each one."
    ),

    fluidRow(
      column(
        4,
        dark_card(
          h5("reactive()"),
          tags$ul(
            tags$li("Returns a value"),
            tags$li("Auto-updates"),
            tags$li("Feeds into outputs"),
            tags$li("Use for tables, plots, summaries")
          )
        )
      ),
      column(
        4,
        dark_card(
          h5("observe()"),
          tags$ul(
            tags$li("Side-effects only"),
            tags$li("Auto-updates"),
            tags$li("Does not return a value"),
            tags$li("Use for printing, logging, UI updates")
          )
        )
      ),
      column(
        4,
        dark_card(
          h5("observeEvent()"),
          tags$ul(
            tags$li("Event-driven"),
            tags$li("No auto-update"),
            tags$li("Runs only when triggered"),
            tags$li("Use for buttons & manual control")
          )
        )
      )
    ),

    dark_card(
      h5("Same Job, Different Tool"),
      p(
        "All three tools can update a plot or table — but each one is designed for a different purpose. ",
        strong("reactive()"), " is for values, ",
        strong("observe()"), " is for side-effects, and ",
        strong("observeEvent()"), " is for manual control."
      )
    ),

    # --- QUIZ ---
    dark_card(
      h5("Quick Quiz (5 Questions)"),
      uiOutput(ns("quiz_ui"))
    )
  )
}


# ============================================================
# SERVER
# ============================================================

reactiveOverviewServer <- function(id) {
  moduleServer(id, function(input, output, session) {

    ns <- session$ns

    # Quiz questions
    questions <- list(
      list(q = "Which tool returns a value?", a = "reactive()"),
      list(q = "Which tool is for side-effects?", a = "observe()"),
      list(q = "Which tool is event-driven?", a = "observeEvent()"),
      list(q = "Which tool should update a plot automatically?", a = "reactive()"),
      list(q = "Which tool should run only when a button is clicked?", a = "observeEvent()")
    )

    # Common choices
    choice_labels <- c("reactive()", "observe()", "observeEvent()")

    # Render quiz
    output$quiz_ui <- renderUI({
      tagList(
        lapply(seq_along(questions), function(i) {
          q <- questions[[i]]

          dark_card(
            h6(paste("Question", i)),
            p(q$q),

            # radioButtons with explicit light text for labels
            radioButtons(
              inputId = ns(paste0("q", i)),
              label = NULL,
              choiceNames = lapply(choice_labels, function(lbl) {
                tags$span(style = "color:#eee;", lbl)
              }),
              choiceValues = choice_labels,
              inline = TRUE,
              selected = character(0)
            ),

            uiOutput(ns(paste0("feedback", i)))
          )
        }),

        actionButton(ns("submit_quiz"), "Check Answers", class = "btn-primary")
      )
    })

    # Feedback logic
    observeEvent(input$submit_quiz, {
      lapply(seq_along(questions), function(i) {
        correct <- questions[[i]]$a
        user_ans <- input[[paste0("q", i)]]

        output[[paste0("feedback", i)]] <- renderUI({
          if (is.null(user_ans) || identical(user_ans, "")) return(NULL)

          if (user_ans == correct) {
            tags$p(style = "color:#4dff88; font-weight:bold;", "Correct!")
          } else {
            tags$p(style = "color:#ff6666; font-weight:bold;",
                   paste("Incorrect — correct answer is", correct))
          }
        })
      })
    })
  })
}
