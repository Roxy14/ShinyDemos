options(stringsAsFactors = FALSE)

library(shiny)
library(plotly)
library(DT)
library(dplyr)
library(tidyr)
library(purrr)
library(lubridate)

# ============================================================
# 1. Generate 4 independent source datasets (10 records/run)
# ============================================================

generate_source_dataset <- function(prefix, n_per_run = 10, runs = 3) {
  run_dates <- Sys.Date() - rev(seq_len(runs) - 1)
  
  map_dfr(seq_len(runs), function(r) {
    tibble(
      RUNDATE = run_dates[r],
      ID = sprintf("%sR%d_%03d", prefix, r, 1:n_per_run),
      Source = prefix,
      Email = paste0("user", 1:n_per_run, "@example.com"),
      Amount = sprintf("%.2f", runif(n_per_run, 50, 500)),
      Status = sample(c("Active", "Inactive", "Pending"), n_per_run, TRUE),
      Notes = paste("Notes for", sprintf("%sR%d_%03d", prefix, r, 1:n_per_run)),
      StartDate = sample(seq(as.Date("2024-01-01"), as.Date("2024-12-31"), by="day"), n_per_run)
    )
  })
}

xml_df   <- generate_source_dataset("XML")
json_df  <- generate_source_dataset("JSON")
csv_df   <- generate_source_dataset("CSV")
excel_df <- generate_source_dataset("EXCEL")

all_sources <- bind_rows(xml_df, json_df, csv_df, excel_df)

# ============================================================
# 2. Cloud dataset: field errors + row diffs + duplicates
# ============================================================

inject_field_errors <- function(df, error_rate = 0.08) {
  df_cloud <- df
  
  fields <- c("Email", "Amount", "Status", "Notes", "StartDate")
  total_cells <- nrow(df) * length(fields)
  num_errors <- round(total_cells * error_rate)
  
  error_positions <- tibble(
    row = sample(1:nrow(df), num_errors, replace = TRUE),
    field = sample(fields, num_errors, replace = TRUE)
  )
  
  for (i in 1:nrow(error_positions)) {
    r <- error_positions$row[i]
    f <- error_positions$field[i]
    
    if (f == "StartDate") {
      df_cloud[[f]][r] <- df_cloud[[f]][r] + sample(c(-30, -15, 15, 30), 1)
    } else {
      df_cloud[[f]][r] <- paste0("ERR_", sample(1000:9999, 1))
    }
  }
  
  df_cloud
}

cloud_df <- inject_field_errors(all_sources)

set.seed(123)

# Remove 2 random rows per source from Cloud
rows_to_remove <- cloud_df %>%
  group_by(Source) %>%
  slice_sample(n = 2, replace = FALSE) %>%
  ungroup()

cloud_df <- anti_join(cloud_df, rows_to_remove, by = c("RUNDATE", "ID", "Source"))

# Add some full-row duplicates in Cloud (2 per source)
dupe_rows_cloud <- all_sources %>%
  group_by(Source) %>%
  slice_sample(n = 2, replace = TRUE) %>%
  ungroup()

cloud_df <- bind_rows(cloud_df, dupe_rows_cloud)

# Add some full-row duplicates in Source (1 per source)
dupe_rows_source <- all_sources %>%
  group_by(Source) %>%
  slice_sample(n = 1, replace = TRUE) %>%
  ungroup()

all_sources <- bind_rows(all_sources, dupe_rows_source)

# ============================================================
# 3. Field-level discrepancies
# ============================================================

build_discrepancies <- function(source_df, cloud_df) {
  joined <- left_join(
    cloud_df, source_df,
    by = c("ID", "RUNDATE", "Source"),
    suffix = c("_Cloud", "_Source")
  )
  
  fields <- c("Email", "Amount", "Status", "Notes", "StartDate")
  
  purrr::map_dfr(fields, function(f) {
    cloud_col  <- paste0(f, "_Cloud")
    source_col <- paste0(f, "_Source")
    
    joined %>%
      filter(!is.na(.data[[source_col]]),
             .data[[cloud_col]] != .data[[source_col]]) %>%
      transmute(
        RUNDATE,
        ID,
        Source,
        Field = f,
        Cloud_Value  = as.character(.data[[cloud_col]]),
        Source_Value = as.character(.data[[source_col]])
      )
  })
}

discrepancies <- build_discrepancies(all_sources, cloud_df)

# ============================================================
# 4. Full-row duplicate detection
# ============================================================

find_full_row_duplicates <- function(df) {
  df %>%
    group_by(across(everything())) %>%
    filter(n() > 1) %>%
    ungroup()
}

source_dupes <- find_full_row_duplicates(all_sources)
cloud_dupes  <- find_full_row_duplicates(cloud_df)

source_dupe_examples <- source_dupes %>% group_by(Source) %>% slice_head(n = 5)
cloud_dupe_examples  <- cloud_dupes %>% group_by(Source) %>% slice_head(n = 5)

# ============================================================
# 5. Row count differences
# ============================================================

missing_in_cloud <- anti_join(all_sources, cloud_df, by = c("ID", "RUNDATE", "Source")) %>%
  slice_head(n = 5)

extra_in_cloud <- anti_join(cloud_df, all_sources, by = c("ID", "RUNDATE", "Source")) %>%
  slice_head(n = 5)

# ============================================================
# 6. KPI calculations
# ============================================================

row_count_summary <- all_sources %>%
  group_by(Source) %>%
  summarise(
    Source_Rows = n_distinct(ID),
    .groups = "drop"
  ) %>%
  left_join(
    cloud_df %>%
      group_by(Source) %>%
      summarise(Cloud_Rows = n_distinct(ID), .groups = "drop"),
    by = "Source"
  ) %>%
  mutate(
    Row_Match_Pct = round((pmin(Source_Rows, Cloud_Rows) / pmax(Source_Rows, Cloud_Rows)) * 100, 2)
  )

overall_row_match <- round(mean(row_count_summary$Row_Match_Pct), 2)

total_duplicate_issues <- nrow(source_dupes) + nrow(cloud_dupes)

field_mismatch_pct <- round(
  n_distinct(discrepancies$ID) / n_distinct(all_sources$ID) * 100, 2
)

worst_source <- row_count_summary %>%
  arrange(Row_Match_Pct) %>%
  slice(1) %>%
  pull(Source)

# ============================================================
# 7. Validation matrix + combined match score
# ============================================================

validation_matrix <- row_count_summary %>%
  left_join(
    tibble(
      Source = unique(all_sources$Source),
      Duplicates_Source = sapply(unique(all_sources$Source), function(s) nrow(source_dupes %>% filter(Source == s))),
      Duplicates_Cloud  = sapply(unique(all_sources$Source), function(s) nrow(cloud_dupes %>% filter(Source == s))),
      Field_Mismatches  = sapply(unique(all_sources$Source), function(s) nrow(discrepancies %>% filter(Source == s)))
    ),
    by = "Source"
  ) %>%
  mutate(
    Cloud_Match_Pct = round(
      (1 - Field_Mismatches / Source_Rows) * 100, 2
    ),
    Combined_Match_Score = round((Row_Match_Pct + Cloud_Match_Pct) / 2, 2)
  ) %>%
  rename(
    `Source System` = Source,
    `Rows in Source` = Source_Rows,
    `Rows in Cloud` = Cloud_Rows
  )

# ============================================================
# 8. Trend over time
# ============================================================

row_match_trend <- all_sources %>%
  group_by(RUNDATE, Source) %>%
  summarise(Source_Records = n_distinct(ID), .groups = "drop") %>%
  left_join(
    cloud_df %>%
      group_by(RUNDATE, Source) %>%
      summarise(Cloud_Records = n_distinct(ID), .groups = "drop"),
    by = c("RUNDATE", "Source")
  ) %>%
  mutate(
    MatchPct = round((pmin(Source_Records, Cloud_Records) / pmax(Source_Records, Cloud_Records)) * 100, 2)
  )

# ============================================================
# 9. UI (KPI tiles + combined match chart + matrix + tabs + trend)
# ============================================================

ui <- fluidPage(
  
  tags$head(tags$style(HTML("
    .kpi-tile {
      background: #f5f5f5;
      padding: 20px;
      border-radius: 12px;
      text-align: center;
      box-shadow: 0 2px 6px rgba(0,0,0,0.1);
      margin-bottom: 20px;
    }
    .kpi-value {
      font-size: 32px;
      font-weight: bold;
      margin-top: 10px;
    }
    .kpi-label {
      font-size: 14px;
      color: #555;
    }
    .card { border: 1px solid #ccc; border-radius: 8px; margin-bottom: 20px; }
    .card-header { background: #f7f7f7; padding: 12px 16px; cursor: pointer; font-size: 16px; font-weight: bold; }
    .card-body { padding: 16px; }
  "))),
  
  titlePanel("📊 Cloud vs Source Migration Validation Dashboard"),
  
  fluidRow(
    column(3, div(class="kpi-tile",
                  div(class="kpi-label", "Row Count Match %"),
                  div(class="kpi-value", overall_row_match)
    )),
    column(3, div(class="kpi-tile",
                  div(class="kpi-label", "Duplicate Issues (rows)"),
                  div(class="kpi-value", total_duplicate_issues)
    )),
    column(3, div(class="kpi-tile",
                  div(class="kpi-label", "Field Mismatch %"),
                  div(class="kpi-value", field_mismatch_pct)
    )),
    column(3, div(class="kpi-tile",
                  div(class="kpi-label", "Worst Source (Row Match)"),
                  div(class="kpi-value", worst_source)
    ))
  ),
  
  div(class="card",
      div(class="card-header", `data-toggle`="collapse", `data-target`="#combinedMatchCard",
          "Combined Match Score by Source"),
      div(id="combinedMatchCard", class="collapse in card-body",
          plotlyOutput("combined_match_plot")
      )
  ),
  
  div(class="card",
      div(class="card-header", `data-toggle`="collapse", `data-target`="#matrixCard",
          "Source-Level Validation Matrix"),
      div(id="matrixCard", class="collapse in card-body",
          DTOutput("validation_matrix_table")
      )
  ),
  
  div(class="card",
      div(class="card-header", `data-toggle`="collapse", `data-target`="#issuesCard",
          "Issue Examples"),
      div(id="issuesCard", class="collapse card-body",
          tabsetPanel(
            tabPanel("Duplicate Examples",
                     h4("Source Full-Row Duplicates"),
                     DTOutput("source_dupe_table"),
                     h4("Cloud Full-Row Duplicates"),
                     DTOutput("cloud_dupe_table")
            ),
            tabPanel("Row Count Differences",
                     h4("Missing in Cloud"),
                     DTOutput("missing_table"),
                     h4("Extra in Cloud"),
                     DTOutput("extra_table")
            ),
            tabPanel("Field-Level Discrepancies",
                     DTOutput("discrepancy_table")
            )
          )
      )
  ),
  
  div(class="card",
      div(class="card-header", `data-toggle`="collapse", `data-target`="#trendCard",
          "Row Count Match % Trend Over Time"),
      div(id="trendCard", class="collapse card-body",
          plotlyOutput("row_match_trend_plot")
      )
  )
)

# ============================================================
# SERVER
# ============================================================

server <- function(input, output, session) {
  
  source_colors <- c(
    "XML" = "#1f77b4",
    "JSON" = "#ff7f0e",
    "CSV" = "#2ca02c",
    "EXCEL" = "#d62728"
  )
  
  # Combined Match Score chart
  output$combined_match_plot <- renderPlotly({
    plot_ly(
      validation_matrix,
      x = ~`Source System`,
      y = ~Combined_Match_Score,
      type = "bar",
      marker = list(color = source_colors[validation_matrix$`Source System`])
    ) %>%
      layout(
        title = "Combined Match Score (Row Count & Field Match)",
        yaxis = list(title = "Combined Match Score", range = c(0, 100)),
        xaxis = list(title = "Source System")
      )
  })
  
  # Validation matrix
  output$validation_matrix_table <- renderDT({
    datatable(validation_matrix, options = list(pageLength = 10))
  })
  
  # Duplicate examples
  output$source_dupe_table <- renderDT({
    if (nrow(source_dupe_examples) == 0)
      return(datatable(data.frame(Message = "No full-row duplicates found in Source")))
    datatable(source_dupe_examples)
  })
  
  output$cloud_dupe_table <- renderDT({
    if (nrow(cloud_dupe_examples) == 0)
      return(datatable(data.frame(Message = "No full-row duplicates found in Cloud")))
    datatable(cloud_dupe_examples)
  })
  
  # Row count differences
  output$missing_table <- renderDT({
    if (nrow(missing_in_cloud) == 0)
      return(datatable(data.frame(Message = "No missing rows in Cloud")))
    datatable(missing_in_cloud)
  })
  
  output$extra_table <- renderDT({
    if (nrow(extra_in_cloud) == 0)
      return(datatable(data.frame(Message = "No extra rows in Cloud")))
    datatable(extra_in_cloud)
  })
  
  # Field-level discrepancies
  output$discrepancy_table <- renderDT({
    if (nrow(discrepancies) == 0)
      return(datatable(data.frame(Message = "No field-level mismatches found")))
    datatable(discrepancies)
  })
  
  # Trend over time
  output$row_match_trend_plot <- renderPlotly({
    plot_ly(
      row_match_trend,
      x = ~RUNDATE,
      y = ~MatchPct,
      color = ~Source,
      colors = source_colors,
      type = "scatter",
      mode = "lines+markers"
    ) %>%
      layout(
        title = "Row Count Match % Trend Over Time",
        yaxis = list(title = "Row Count Match %", range = c(0, 100)),
        xaxis = list(title = "Run Date")
      )
  })
}

shinyApp(ui = ui, server = server)

