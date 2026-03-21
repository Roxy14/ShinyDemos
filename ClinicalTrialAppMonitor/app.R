library(shiny)
library(shinydashboard)
library(dplyr)
library(lubridate)
library(plotly)
library(DT)
library(ggplot2)
library(tibble)
library(tidyr)

# -----------------------------
# Dummy clinical query data
# -----------------------------
simulate_queries <- function(n = 1200, today = Sys.Date()) {
  set.seed(123)
  
  sites <- paste0("Site ", sprintf("%02d", 1:8))
  
  created_dt <- sample(
    seq(today - 60, today, by = "day"),
    n, replace = TRUE
  )
  
  is_closed <- rbinom(n, 1, 0.7) == 1
  
  resp_time <- rpois(n, lambda = 5)
  
  resolved_dt <- ifelse(
    is_closed,
    as.character(as.Date(created_dt) + resp_time),
    NA_character_
  )
  
  tibble(
    query_id    = paste0("Q", sprintf("%04d", 1:n)),
    site        = sample(sites, n, replace = TRUE),
    created_dt  = as.Date(created_dt),
    resolved_dt = as.Date(resolved_dt),
    status      = if_else(is.na(resolved_dt), "Open", "Closed"),
    description = sample(
      c("Missing value", "Out-of-range lab", "Protocol deviation",
        "Visit date mismatch", "Concomitant med issue"),
      n, replace = TRUE
    )
  )
}

query_data <- simulate_queries()

# -----------------------------
# UI
# -----------------------------
ui <- dashboardPage(
  dashboardHeader(title = "Clinical Query Site Helper"),
  
  dashboardSidebar(
    selectInput(
      "site", "Select site:",
      choices = sort(unique(query_data$site)),
      selected = sort(unique(query_data$site))[1]
    ),
    
    sliderInput(
      "target_days", "Target response time (days):",
      min = 1, max = 14, value = 5
    ),
    
    dateRangeInput(
      "date_range", "Query creation date range:",
      start = Sys.Date() - 30,
      end   = Sys.Date()
    ),
    
    dateInput(
      "lock_date", "Planned database lock:",
      value = Sys.Date() + 21
    ),
    
    actionButton("refresh", "Recalculate", class = "btn-primary")
  ),
  
  dashboardBody(
    
    # -----------------------------
    # Banner + Gauge Row
    # -----------------------------
    fluidRow(
      column(
        width = 8,
        uiOutput("banner")
      ),
      column(
        width = 4,
        tags$div(
          style="
            background:white;
            border-radius:10px;
            box-shadow:0 4px 12px rgba(0,0,0,0.15);
            padding:10px;
            text-align:center;
            margin-top:10px;
          ",
          tags$div(
            style="font-size:15px; font-weight:600; margin-bottom:0px;",
            "Lock Readiness Score"
          ),
          plotlyOutput("lock_gauge", height="120px"),
          uiOutput("gauge_labels")
        )
      )
    ),
    
    br(),
    
    fluidRow(
      column(
        width = 6,
        h3("Open queries"),
        DTOutput("open_table")
      ),
      column(
        width = 6,
        h3("Daily query volume (opened vs closed)"),
        plotlyOutput("daily_plot", height = "260px")
      )
    ),
    
    hr(),
    
    fluidRow(
      column(
        width = 12,
        h3("Site performance summary"),
        actionButton("email_btn", "Email this summary", icon = icon("envelope"), class = "btn-info"),
        br(), br(),
        DTOutput("metrics_table")
      )
    )
  )
)

# -----------------------------
# SERVER
# -----------------------------
server <- function(input, output, session) {
  
  # -----------------------------
  # Filtered data + overdue logic
  # -----------------------------
  filtered_data <- reactive({
    query_data %>%
      filter(site == input$site) %>%
      filter(created_dt >= input$date_range[1],
             created_dt <= input$date_range[2]) %>%
      mutate(
        due_date      = created_dt + input$target_days,
        response_time = as.numeric(resolved_dt - created_dt),
        is_overdue    = status == "Open" & Sys.Date() > due_date,
        days_late     = if_else(is_overdue,
                                as.numeric(Sys.Date() - due_date),
                                0L)
      )
  })
  
  # -----------------------------
  # Forecast + performance
  # -----------------------------
  forecast_data <- reactive({
    req(input$refresh)
    
    df <- filtered_data()
    today <- Sys.Date()
    days_rem <- max(as.numeric(input$lock_date - today), 1)
    
    opened_daily <- df %>%
      count(created_dt, name = "opened") %>%
      rename(day = created_dt)
    
    closed_daily <- df %>%
      filter(!is.na(resolved_dt)) %>%
      count(resolved_dt, name = "closed") %>%
      rename(day = resolved_dt)
    
    daily <- full_join(opened_daily, closed_daily, by = "day") %>%
      mutate(
        opened = replace_na(opened, 0),
        closed = replace_na(closed, 0),
        net    = opened - closed
      )
    
    current_overdue <- sum(df$is_overdue)
    net_rate <- if (nrow(daily) > 1) mean(daily$net) else 0
    projected_overdue <- max(0, current_overdue + net_rate * days_rem)
    
    median_resp <- suppressWarnings(median(df$response_time, na.rm = TRUE))
    on_time_rate <- suppressWarnings(
      mean(df$response_time <= input$target_days, na.rm = TRUE)
    )
    
    tibble(
      site              = unique(df$site),
      current_overdue   = current_overdue,
      projected_overdue = round(projected_overdue, 1),
      median_response   = round(median_resp, 1),
      on_time_rate      = round(on_time_rate * 100, 1),
      days_remaining    = days_rem,
      required_rate     = round(current_overdue / days_rem, 2),
      open_queries      = sum(df$status == "Open")
    )
  })
  
  # -----------------------------
  # Lock Readiness Score (corrected)
  # -----------------------------
  lock_readiness <- reactive({
    fd <- forecast_data()
    
    overdue <- fd$current_overdue
    open_q  <- fd$open_queries
    rate    <- fd$required_rate
    ontime  <- fd$on_time_rate
    
    overdue_penalty <- min(60, overdue * 5)
    rate_penalty    <- min(25, rate * 10)
    open_penalty    <- min(15, open_q * 0.5)
    on_time_bonus   <- ontime * 0.2
    
    score <- 100 - overdue_penalty - rate_penalty - open_penalty + on_time_bonus
    score <- max(0, min(100, round(score, 1)))
    
    score
  })
  
  # -----------------------------
  # Gauge Labels (clean)
  # -----------------------------
  output$gauge_labels <- renderUI({
    score <- lock_readiness()
    
    status <- if (score < 60) {
      "🔴 Not Ready"
    } else if (score < 80) {
      "🟡 At Risk"
    } else {
      "🟢 Ready"
    }
    
    tags$div(style="font-size:13px; margin-top:-4px;", paste0(score, " — ", status))
  })
  
  # -----------------------------
  # Gauge
  # -----------------------------
  output$lock_gauge <- renderPlotly({
    score <- lock_readiness()
    
    plot_ly(
      type = "indicator",
      mode = "gauge+number",
      value = score,
      number = list(font = list(size = 20)),
      gauge = list(
        shape = "semi",
        axis = list(range = list(0, 100)),
        bar = list(color = "darkblue"),
        steps = list(
          list(range = c(0, 59),  color = "#ff6666"),
          list(range = c(59, 79), color = "#ffcc66"),
          list(range = c(79, 100), color = "#66cc66")
        )
      )
    )
  })
  
  # -----------------------------
  # Banner
  # -----------------------------
  output$banner <- renderUI({
    fd <- forecast_data()
    
    div(
      style = "
        background-color:#f0f8ff;
        border-left:6px solid #007acc;
        padding:12px;
        margin-top:10px;
        font-size:16px;
        font-weight:500;
      ",
      HTML(
        paste0(
          "For <b>", fd$site, "</b>: you currently have <b>", fd$current_overdue,
          "</b> overdue queries. With <b>", fd$days_remaining,
          "</b> days until lock, you need to close <b>", fd$required_rate,
          "</b> queries per day to be on time."
        )
      )
    )
  })
  
  # -----------------------------
  # Open queries table
  # -----------------------------
  output$open_table <- renderDT({
    df <- filtered_data() %>% filter(status == "Open")
    
    datatable(
      df %>% select(query_id, created_dt, due_date, days_late, description),
      options = list(
        pageLength = 8,
        rowCallback = JS("
          function(row, data) {
            var daysLate = Number(data[3]);
            if (!isNaN(daysLate) && daysLate > 0) {
              $('td', row).css('background-color', '#ffdddd');
            }
          }
        ")
      )
    )
  })
  
  # -----------------------------
  # Daily opened vs closed with query IDs in hover
  # -----------------------------
  output$daily_plot <- renderPlotly({
    df <- filtered_data()
    
    opened_daily <- df %>%
      group_by(created_dt) %>%
      summarise(
        opened = n(),
        opened_ids = paste(query_id, collapse = ", "),
        .groups = "drop"
      ) %>%
      rename(day = created_dt)
    
    closed_daily <- df %>%
      filter(!is.na(resolved_dt)) %>%
      group_by(resolved_dt) %>%
      summarise(
        closed = n(),
        closed_ids = paste(query_id, collapse = ", "),
        .groups = "drop"
      ) %>%
      rename(day = resolved_dt)
    
    daily <- full_join(opened_daily, closed_daily, by = "day") %>%
      mutate(
        opened = replace_na(opened, 0),
        closed = replace_na(closed, 0),
        opened_ids = replace_na(opened_ids, "None"),
        closed_ids = replace_na(closed_ids, "None")
      ) %>%
      pivot_longer(
        cols = c(opened, closed),
        names_to = "type",
        values_to = "n"
      ) %>%
      mutate(
        hover_text = ifelse(
          type == "opened",
          paste0(
            "Date: ", day, "<br>",
            "Opened: ", n, "<br>",
            "Query IDs: ", opened_ids
          ),
          paste0(
            "Date: ", day, "<br>",
            "Closed: ", n, "<br>",
            "Query IDs: ", closed_ids
          )
        )
      )
    
    plot_ly(
      daily,
      x = ~day,
      y = ~n,
      color = ~type,
      colors = c("opened" = "#3498db", "closed" = "#2ecc71"),
      type = "bar",
      text = ~hover_text,
      hoverinfo = "text"
    ) %>%
      layout(
        barmode = "group",
        xaxis = list(title = "Date"),
        yaxis = list(title = "Count")
      )
  })
  
  # -----------------------------
  # Metrics table with export buttons + tooltip
  # -----------------------------
  output$metrics_table <- renderDT({
    fd <- forecast_data()
    
    tooltip_text <- paste(
      "Projected overdue is calculated using the average net daily query change.",
      "",
      "R code (using dplyr):",
      "daily <- df %>%",
      "  group_by(created_dt) %>%",
      "  summarise(opened = n(),",
      "            closed = sum(status == \"Closed\")) %>%",
      "  mutate(net = opened - closed)",
      "",
      "net_rate <- mean(daily$net)",
      "projected_overdue <- current_overdue + net_rate * days_remaining",
      sep = "\n"
    )
    
    tooltip_text <- gsub("\"", "&quot;", tooltip_text)
    
    metrics <- tibble(
      Metric = c(
        "Median response time (days)",
        "On-time rate (%)",
        "Current overdue",
        paste0(
          "Projected overdue ",
          "<span title=\"", tooltip_text, "\" style='cursor:help;'>ℹ️</span>"
        ),
        "Days until lock",
        "Required closures per day",
        "Target response time (days)"
      ),
      Value = c(
        fd$median_response,
        fd$on_time_rate,
        fd$current_overdue,
        fd$projected_overdue,
        fd$days_remaining,
        fd$required_rate,
        input$target_days
      )
    )
    
    datatable(
      metrics,
      escape = FALSE,
      extensions = "Buttons",
      options = list(
        dom = "Bft",
        paging = FALSE,
        buttons = c("copy", "csv", "excel", "pdf", "print")
      ),
      rownames = FALSE
    )
  })
  
  # -----------------------------
  # Email Summary Modal
  # -----------------------------
  observeEvent(input$email_btn, {
    fd <- forecast_data()
    
    email_body <- paste0(
      "Site: ", fd$site, "\n",
      "Current overdue: ", fd$current_overdue, "\n",
      "Projected overdue: ", fd$projected_overdue, "\n",
      "Median response time: ", fd$median_response, " days\n",
      "On-time rate: ", fd$on_time_rate, "%\n",
      "Days until lock: ", fd$days_remaining, "\n",
      "Required closures per day: ", fd$required_rate, "\n",
      "Target response time: ", input$target_days, " days\n"
    )
    
    showModal(modalDialog(
      title = "Email Site Summary",
      
      textInput("email_to", "Send to:", placeholder = "manager@example.com"),
      
      textAreaInput("email_text", "Email body:", value = email_body, width = "100%", height = "250px"),
      
      footer = tagList(
        modalButton("Cancel"),
        actionButton("send_email", "Open Email Client", class = "btn-primary")
      ),
      
      easyClose = TRUE
    ))
  })
  
  # -----------------------------
  # Open Email Client
  # -----------------------------
  observeEvent(input$send_email, {
    req(input$email_to)
    
    body <- URLencode(input$email_text)
    to   <- URLencode(input$email_to)
    
    browseURL(paste0("mailto:", to, "?subject=Site Query Summary&body=", body))
    
    removeModal()
  })
}

shinyApp(ui, server)
