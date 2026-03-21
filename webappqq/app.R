library(shiny)
library(shinydashboard)
library(dplyr)
library(plotly)
library(lubridate)
library(DT)

# ---- simulate one run ----
simulate_run <- function(run_id, study, base_runtime, datetime) {
  stages  <- c("Extract", "Transform", "Load", "QC")
  weights <- c(0.3, 0.4, 0.2, 0.1)
  jitter  <- runif(4, 0.8, 1.3)
  
  tibble(
    run_id       = run_id,
    study        = study,
    run_datetime = datetime,
    stage        = stages,
    duration     = round(base_runtime * weights * jitter, 1)
  )
}

# ---- UI ----
ui <- dashboardPage(
  dashboardHeader(title = "Runtime Monitor Dashboard"),
  
  dashboardSidebar(
    sidebarMenu(
      menuItem("Dashboard", tabName = "dashboard", icon = icon("tachometer-alt")),
      
      sliderInput("base_runtime", "Base Runtime (sec):",
                  min = 10, max = 300, value = 120, step = 10),
      
      numericInput("fail_threshold", "Failure Threshold (sec):",
                   value = 180, min = 30, max = 600, step = 10),
      
      dateRangeInput("date_range", "Run Date Range:",
                     start = Sys.Date() - 7, end = Sys.Date()),
      
      selectInput("study", "Study:",
                  choices = c("All", paste0("Study_", 1:5))),
      
      numericInput("n_runs", "Number of Runs:", value = 20, min = 5, max = 200),
      
      actionButton("run", "Update", class = "btn-primary")
    )
  ),
  
  dashboardBody(
    
    # JS for printing + email compose
    tags$script(HTML("
      Shiny.addCustomMessageHandler('printSection', function(message) {
        var section = document.getElementById(message.id);
        var win = window.open('', '', 'width=900,height=700');
        win.document.write(section.innerHTML);
        win.document.close();
        win.focus();
        win.print();
        win.close();
      });

      Shiny.addCustomMessageHandler('emailSection', function(message) {
        var subject = encodeURIComponent(message.subject);
        var body = encodeURIComponent(message.body);
        window.location.href = 'mailto:?subject=' + subject + '&body=' + body;
      });
    ")),
    
    tabItems(
      tabItem(tabName = "dashboard",
              
              # ---------------- KPI ROW ----------------
              fluidRow(
                valueBoxOutput("avgBox",  width = 4),
                valueBoxOutput("maxBox",  width = 4),
                valueBoxOutput("runsBox", width = 4)
              ),
              
              # ---------------- PLOT + TABLE ROW ----------------
              fluidRow(
                box(
                  title = "Runtime Breakdown by Run",
                  width = 8, solidHeader = TRUE, status = "primary",
                  
                  div(
                    style = "margin-bottom:10px;",
                    actionButton("print_plot", "Print", icon = icon("print")),
                    actionButton("email_plot", "Email", icon = icon("envelope"))
                  ),
                  
                  div(id = "plot_section",
                      plotlyOutput("runtime_plot", height = "350px")
                  )
                ),
                
                box(
                  title = "Latest Runs Table",
                  width = 4, solidHeader = TRUE, status = "info",
                  
                  div(
                    style = "margin-bottom:10px;",
                    actionButton("print_table", "Print", icon = icon("print")),
                    actionButton("email_table", "Email", icon = icon("envelope"))
                  ),
                  
                  div(id = "table_section",
                      DTOutput("runs_table")
                  )
                )
              ),
              
              # ---------------- FORECAST SECTION (BOTTOM) ----------------
              fluidRow(
                box(
                  title = div(
                    "Studies Expected to Exceed Runtime Threshold Next Week",
                    tags$i(
                      class = "fa fa-info-circle",
                      style = "margin-left: 8px; cursor: help;",
                      title = "Risk is calculated by measuring how often a study’s total runtime exceeds the threshold. We compute each study’s failure rate, estimate how many runs it produces per day, and project how many failures are expected next week."
                    )
                  ),
                  width = 12, solidHeader = TRUE, status = "warning",
                  
                  div(
                    style = "margin-bottom:10px;",
                    actionButton("print_forecast", "Print", icon = icon("print")),
                    actionButton("email_forecast", "Email", icon = icon("envelope"))
                  ),
                  
                  div(id = "forecast_section",
                      DTOutput("forecast_table")
                  )
                )
              )
      )
    )
  )
)

# ---- SERVER ----
server <- function(input, output, session) {
  
  # simulate data dynamically whenever Update is clicked
  log_data <- reactive({
    req(input$run)
    
    start_date <- input$date_range[1]
    end_date   <- input$date_range[2]
    
    run_ids <- seq_len(input$n_runs)
    
    run_dates <- sample(seq(start_date, end_date, by = "day"),
                        input$n_runs, replace = TRUE)
    
    run_datetimes <- run_dates + seconds(sample(0:(24*3600), input$n_runs, replace = TRUE))
    
    studies <- sample(paste0("Study_", 1:5), input$n_runs, replace = TRUE)
    
    bind_rows(lapply(seq_along(run_ids), function(i) {
      simulate_run(
        run_id       = run_ids[i],
        study        = studies[i],
        base_runtime = input$base_runtime * runif(1, 0.7, 1.3),
        datetime     = run_datetimes[i]
      )
    }))
  })
  
  # filtered data for main visuals
  filtered <- reactive({
    df <- log_data()
    
    df <- df %>%
      filter(run_datetime >= input$date_range[1],
             run_datetime <= input$date_range[2] + days(1))
    
    if (input$study != "All") {
      df <- df %>% filter(study == input$study)
    }
    
    df
  })
  
  # KPI boxes
  output$avgBox <- renderValueBox({
    df <- filtered()
    if (nrow(df) == 0) {
      return(valueBox("—", "Average Stage Duration", icon = icon("clock"), color = "yellow"))
    }
    avg <- round(mean(df$duration), 1)
    valueBox(paste0(avg, " sec"), "Average Stage Duration", icon = icon("clock"), color = "blue")
  })
  
  output$maxBox <- renderValueBox({
    df <- filtered()
    if (nrow(df) == 0) {
      return(valueBox("—", "Max Stage Duration", icon = icon("exclamation-triangle"), color = "yellow"))
    }
    mx <- round(max(df$duration), 1)
    valueBox(paste0(mx, " sec"), "Max Stage Duration", icon = icon("exclamation-triangle"), color = "red")
  })
  
  output$runsBox <- renderValueBox({
    df <- filtered()
    valueBox(n_distinct(df$run_id), "Total Runs", icon = icon("database"), color = "green")
  })
  
  # stacked bar runtime plot
  output$runtime_plot <- renderPlotly({
    df <- filtered()
    if (nrow(df) == 0) return(NULL)
    
    df <- df %>%
      mutate(
        run_datetime = as.POSIXct(run_datetime),
        stage        = as.character(stage),
        study        = as.character(study),
        duration     = as.numeric(duration),
        run_label    = format(run_datetime, "%Y-%m-%d %H:%M")
      ) %>%
      arrange(run_datetime)
    
    df$run_label <- factor(df$run_label, levels = unique(df$run_label))
    
    plot_ly(
      df,
      x     = ~run_label,
      y     = ~duration,
      color = ~stage,
      type  = "bar",
      colors = "Set2"
    ) %>%
      layout(
        barmode = "stack",
        xaxis = list(title = "Run Datetime", tickangle = -45, automargin = TRUE),
        yaxis = list(title = "Duration (sec)")
      )
  })
  
  # runs table
  output$runs_table <- renderDT({
    df <- filtered() %>%
      group_by(run_id, study, run_datetime) %>%
      summarise(total_runtime = sum(duration), .groups = "drop") %>%
      arrange(desc(run_datetime))
    
    datatable(df, extensions = "Buttons",
              options = list(dom = "Bfrtip", buttons = c("copy", "csv", "excel", "print")))
  })
  
  # ---- NEXT WEEK FAILURE FORECAST ----
  forecast_data <- reactive({
    df <- log_data()
    req(nrow(df) > 0)
    
    # collapse to total runtime per run
    run_summary <- df %>%
      group_by(run_id, study, run_datetime) %>%
      summarise(total_runtime = sum(duration), .groups = "drop") %>%
      mutate(failed = total_runtime > input$fail_threshold)
    
    # per-study stats
    study_stats <- run_summary %>%
      mutate(run_date = as.Date(run_datetime)) %>%
      group_by(study) %>%
      summarise(
        runs             = n(),
        failures         = sum(failed),
        failure_rate     = ifelse(runs > 0, failures / runs, 0),
        runs_per_day     = runs / n_distinct(run_date),
        expected_next_week = round(runs_per_day * 7 * failure_rate, 1),
        .groups = "drop"
      ) %>%
      mutate(
        `Failure Rate (%)`          = round(failure_rate * 100, 1),
        `Runs per Day`              = round(runs_per_day, 2),
        `Expected Failures Next Week` = expected_next_week,
        Risk = case_when(
          expected_next_week >= 3 ~ "High",
          expected_next_week >= 1 ~ "Medium",
          TRUE                    ~ "Low"
        )
      ) %>%
      select(
        Study = study,
        `Failure Rate (%)`,
        `Runs per Day`,
        `Expected Failures Next Week`,
        Risk
      )
    
    study_stats
  })
  
  output$forecast_table <- renderDT({
    df <- forecast_data()
    datatable(
      df,
      options = list(pageLength = 10),
      rownames = FALSE
    )
  })
  
  # PRINT HANDLERS
  observeEvent(input$print_plot, {
    session$sendCustomMessage("printSection", list(id = "plot_section"))
  })
  
  observeEvent(input$print_table, {
    session$sendCustomMessage("printSection", list(id = "table_section"))
  })
  
  observeEvent(input$print_forecast, {
    session$sendCustomMessage("printSection", list(id = "forecast_section"))
  })
  
  # EMAIL HANDLERS
  observeEvent(input$email_plot, {
    session$sendCustomMessage("emailSection", list(
      subject = "Runtime Breakdown Plot",
      body    = "Please see the runtime breakdown plot from the Runtime Monitor Dashboard."
    ))
  })
  
  observeEvent(input$email_table, {
    session$sendCustomMessage("emailSection", list(
      subject = "Runtime Summary Table",
      body    = "Please see the runtime summary table from the Runtime Monitor Dashboard."
    ))
  })
  
  observeEvent(input$email_forecast, {
    session$sendCustomMessage("emailSection", list(
      subject = "Next Week Runtime Failure Forecast",
      body    = "Please see the forecast of studies expected to exceed the runtime threshold next week."
    ))
  })
}

shinyApp(ui, server)
