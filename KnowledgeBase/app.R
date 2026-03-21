library(shiny)
library(dplyr)
library(stringr)

# -----------------------------
# Initial Knowledge Base
# -----------------------------
knowledge_items <- tibble::tibble(
  title = c(
    "R Shiny Documentation",
    "SQL Server Performance Tuning",
    "GitHub Actions Guide",
    "Data Engineering Best Practices",
    "Clinical Study Data Standards",
    "Regression Modelling in R"
  ),
  link = c(
    "https://shiny.posit.co/r/",
    "https://learn.microsoft.com/sql/relational-databases/performance/",
    "https://docs.github.com/actions",
    "https://databricks.com/glossary/data-engineering",
    "https://www.cdisc.org/standards",
    "https://r4ds.hadley.nz/model-basics"
  ),
  category = c(
    "R / Shiny",
    "Databases",
    "DevOps",
    "Data Engineering",
    "Clinical",
    "Machine Learning"
  ),
  description = c(
    "Official documentation for building apps with R Shiny.",
    "Microsoft SQL Server performance tuning and optimisation resources.",
    "Guide to CI/CD automation using GitHub Actions.",
    "Best practices for scalable data engineering pipelines.",
    "Standards for clinical trial data management and submission.",
    "Introduction to regression modelling concepts and how to implement them in R."
  )
)

categories <- unique(knowledge_items$category)

# -----------------------------
# UI
# -----------------------------
ui <- fluidPage(
  
  tags$head(
    tags$style(HTML("
      body {
        background: linear-gradient(135deg, #1e3c72, #2a5298);
        font-family: 'Inter', 'Segoe UI', sans-serif;
        color: #f5f5f5;
        margin: 0;
      }

      h1 {
        font-weight: 700;
        margin-bottom: 5px;
      }

      .hero {
        padding: 40px 30px;
        text-align: left;
        background: rgba(255,255,255,0.08);
        backdrop-filter: blur(10px);
        border-bottom: 1px solid rgba(255,255,255,0.15);
        box-shadow: 0 4px 20px rgba(0,0,0,0.2);
      }

      .hero-sub {
        font-size: 16px;
        opacity: 0.85;
      }

      .sidebar {
        background: rgba(255,255,255,0.12);
        backdrop-filter: blur(12px);
        height: 100vh;
        padding: 25px;
        border-right: 1px solid rgba(255,255,255,0.2);
      }

      #search {
        border-radius: 25px;
        padding: 10px 15px;
        border: none;
        background: rgba(255,255,255,0.2);
        color: white;
      }

      #search::placeholder {
        color: #ddd;
      }

      .category-pill {
        display: inline-block;
        padding: 8px 16px;
        margin: 6px 6px 12px 0;
        border-radius: 20px;
        background: rgba(255,255,255,0.15);
        cursor: pointer;
        transition: 0.25s;
        font-size: 13px;
        font-weight: 500;
        border: 1px solid rgba(255,255,255,0.25);
      }

      .category-pill:hover {
        background: #4a90e2;
        color: white;
        transform: translateY(-2px);
      }

      .category-pill.active {
        background: #1e90ff;
        color: white;
        border-color: #1e90ff;
      }

      .card {
        background: rgba(255,255,255,0.15);
        backdrop-filter: blur(12px);
        padding: 20px;
        margin-bottom: 18px;
        border-radius: 14px;
        border: 1px solid rgba(255,255,255,0.25);
        box-shadow: 0 4px 12px rgba(0,0,0,0.25);
        transition: 0.25s;
      }

      .card:hover {
        transform: translateY(-4px);
        box-shadow: 0 8px 20px rgba(0,0,0,0.35);
      }

      .card-title {
        font-size: 20px;
        font-weight: 600;
        margin-bottom: 6px;
      }

      .card-category {
        font-size: 12px;
        color: #dcdcdc;
        margin-bottom: 10px;
        font-style: italic;
      }

      .upload-box {
        background: rgba(255,255,255,0.15);
        padding: 15px;
        border-radius: 12px;
        border: 1px solid rgba(255,255,255,0.25);
        margin-top: 20px;
      }

      .btn-primary {
        width: 100%;
        border-radius: 25px;
        padding: 10px;
        font-weight: 600;
        background: #1e90ff;
        border: none;
      }

      .btn-primary:hover {
        background: #187bcd;
      }
    "))
  ),
  
  div(class = "hero",
      h1("📚 Knowledge Base"),
      div(class = "hero-sub", "A modern, searchable hub for your technical references")
  ),
  
  fluidRow(
    column(
      width = 3,
      div(class = "sidebar",
          
          textInput("search", NULL, placeholder = "🔍 Search..."),
          
          h4("Categories"),
          uiOutput("category_pills"),
          
          h4("Submit New Idea"),
          div(class = "upload-box",
              textInput("new_title", "Title"),
              textAreaInput("new_desc", "Description", height = "80px"),
              selectInput("new_cat", "Category", choices = categories),
              textInput("new_link", "Link (optional)"),
              fileInput("new_file", "Attach file (optional)"),
              actionButton("submit_idea", "Add Idea", class = "btn btn-primary")
          )
      )
    ),
    
    column(
      width = 9,
      uiOutput("results_ui")
    )
  )
)

# -----------------------------
# Server
# -----------------------------
server <- function(input, output, session) {
  
  kb <- reactiveVal(knowledge_items)
  
  # ---- Category pills ----
  output$category_pills <- renderUI({
    current <- if (is.null(input$category)) "All" else input$category
    
    tagList(
      div(
        lapply(categories, function(cat) {
          div(
            class = paste("category-pill", if (current == cat) "active"),
            cat,
            onclick = sprintf(
              "Shiny.setInputValue('category', '%s', {priority: 'event'})",
              cat
            )
          )
        }),
        div(
          class = paste("category-pill", if (current == "All") "active"),
          "All",
          onclick = "Shiny.setInputValue('category', 'All', {priority: 'event'})"
        )
      )
    )
  })
  
  # ---- Add new idea ----
  observeEvent(input$submit_idea, {
    req(input$new_title, input$new_desc)
    
    new_row <- tibble(
      title = input$new_title,
      link = ifelse(input$new_link == "", NA, input$new_link),
      category = input$new_cat,
      description = input$new_desc
    )
    
    kb(bind_rows(kb(), new_row))
    
    updateTextInput(session, "new_title", value = "")
    updateTextAreaInput(session, "new_desc", value = "")
    updateTextInput(session, "new_link", value = "")
  })
  
  # ---- Filtering ----
  filtered_data <- reactive({
    df <- kb()
    
    if (!is.null(input$category) && input$category != "All") {
      df <- df %>% filter(category == input$category)
    }
    
    if (!is.null(input$search) && input$search != "") {
      term <- tolower(input$search)
      df <- df %>%
        filter(
          str_detect(tolower(title), term) |
            str_detect(tolower(description), term) |
            str_detect(tolower(category), term)
        )
    }
    
    df
  })
  
  # ---- Results ----
  output$results_ui <- renderUI({
    df <- filtered_data()
    
    if (nrow(df) == 0) {
      return(tags$p("No results found.", style = "color: #ffdddd; font-size: 18px;"))
    }
    
    lapply(seq_len(nrow(df)), function(i) {
      item <- df[i, ]
      
      div(class = "card",
          div(class = "card-title", item$title),
          div(class = "card-category", paste("Category:", item$category)),
          p(item$description),
          if (!is.na(item$link)) tags$a(href = item$link, target = "_blank", style="color:#aee1ff;", "Open link")
      )
    })
  })
}

shinyApp(ui, server)
