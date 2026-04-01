fileStructureUI <- function(id) {
  ns <- NS(id)

  div(
    class = "main-container",

    section_banner("How This App Is Organised", type = "basics"),

    # ------------------------------------------------------------
    # INTRO CARD
    # ------------------------------------------------------------
    dark_card(
      h5("A Quick Look Behind the Scenes"),
      p("Before we wrap up, here’s a simple overview of how this learning app is organised. 
         You don’t need to memorise anything — this is just to give you a sense of how 
         real Shiny projects keep their code tidy as they grow.")
    ),

    # ------------------------------------------------------------
    # FILE STRUCTURE DIAGRAM
    # ------------------------------------------------------------
    dark_card(
      h5("The Structure of This App"),
      code_block(
        ns("structure"),
"
shiny-learn/
│
├── app.R
│
├── modules/               # Each page of the training app
│   ├── home.R
│   ├── basics.R
│   ├── reactive_expressions.R
│   ├── observe.R
│   ├── observe_event.R
│   ├── reactive_flow.R
│   ├── reactive_overview.R
│   ├── mini_app.R
│   ├── file_structure.R
│   └── full_app_example.R
│
├── components/            # Reusable UI pieces
│   ├── dark_card.R
│   ├── code_block.R
│   ├── section_banner.R
│   └── diagram_box.R
│
└── www/                   # Static assets
    ├── styles.css
    └── images/
        └── ui_server_diagram.png
",
        height = "600px"
      )
    ),

    # ------------------------------------------------------------
    # EXPLANATION OF EACH PART
    # ------------------------------------------------------------
    dark_card(
      h5("What Each Part Does"),
      tags$ul(
        tags$li(
          strong("app.R"), 
          " — the main entry point. It loads the theme, sidebar, and all modules."
        ),
        tags$li(
          strong("modules/"), 
          " — each page of the learning app lives in its own file. 
           This keeps the app organised and makes it easy to add or remove pages."
        ),
        tags$li(
          strong("components/"), 
          " — small reusable UI helpers (cards, banners, code blocks, diagrams). 
           These keep the design consistent across the whole app."
        ),
        tags$li(
          strong("www/"), 
          " — static files such as CSS, images, and JavaScript. 
           Shiny automatically serves everything in this folder."
        )
      ),
      p("Inside the images folder, you’ll find small assets used throughout the app, 
         such as ", strong("ui_server_diagram.png"), ", which appears on the 
         'How UI and Server Connect' page.")
    ),

    # ------------------------------------------------------------
    # FINAL CARD — directs learners to official training
    # ------------------------------------------------------------
    dark_card(
      h5("Where to Learn More"),
      p("This concludes the overview. To continue learning and build more advanced apps, 
         visit the official Shiny training site for full lessons, examples, and tutorials."),
      tags$a(
        href = 'https://shiny.posit.co/r/getstarted/shiny-basics/lesson1/',
        'Visit the official Shiny training site',
        target = '_blank'
      )
    )
  )
}

fileStructureServer <- function(id) {
  moduleServer(id, function(input, output, session) {})
}
