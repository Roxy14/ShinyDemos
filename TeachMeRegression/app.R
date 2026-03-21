library(shiny)
library(ggplot2)
library(dplyr)
library(splines)
library(mgcv)
library(randomForest)

ui <- fluidPage(
  
  # ---- GLOBAL GREEN THEME ----
  tags$head(tags$style(HTML("
    body { background-color: #f4f9f4; }
    h3 { color: #2e7d32; font-weight: bold; }
    h4 { color: #1b5e20; }
    .panel { background: #e8f5e9; padding: 12px; border-radius: 6px; }
    .green-box { background:#c8e6c9; padding:10px; border-radius:6px; }
    .warning-box { background:#ffcc80; padding:10px; border-radius:6px; font-weight:bold; }
    .error-box { background:#ef9a9a; padding:10px; border-radius:6px; font-weight:bold; color:white; }
  "))),
  
  titlePanel("Regression Teaching Dashboard: Blood Pressure Examples"),
  
  sidebarLayout(
    sidebarPanel(
      h3("Choose a Scenario"),
      selectInput(
        "scenario",
        "Regression Example:",
        choices = c(
          "Good fit (BP increases linearly with age)",
          "Too much noise (weak relationship)",
          "Non-linear pattern (curved relationship)",
          "Missing predictors (age alone is not enough)",
          "Outliers present (distorts regression)",
          "Small sample size (unstable model)"
        )
      ),
      hr(),
      
      h3("Beginner Cheat Sheet"),
      checkboxInput("show_cheatsheet", "Show Cheat Sheet", FALSE),
      
      # ---- UPDATED CHEAT SHEET ----
      conditionalPanel(
        condition = "input.show_cheatsheet == true",
        tags$div(class="panel",
                 
                 h4("Regression Basics"),
                 p("Regression models how one variable changes with another."),
                 
                 h4("Predictors"),
                 p("Predictors are variables that help explain the outcome (e.g., Age, Weight)."),
                 
                 h4("Residuals"),
                 p("Residual = actual value – predicted value."),
                 
                 h4("R-squared"),
                 p("How much of the variation the model explains. Higher is usually better, but it can increase even when the model is wrong."),
                 
                 h4("p-value"),
                 p("Tells you whether the slope is likely real or random noise. A significant p-value does not guarantee the model shape is correct."),
                 
                 h4("Scatterplots"),
                 p("Tight points = good fit. Wide spread = weak fit. Curves = non-linear."),
                 
                 h4("Residual Plots"),
                 p("Residuals should look like random noise. Patterns mean the model is missing something or the shape is wrong."),
                 
                 tags$hr(),
                 h3("Are There 'Good' R-squared or p-values?"),
                 
                 h4("Is there a good R-squared?"),
                 p("There is no universal cutoff. What counts as 'good' depends on the field:"),
                 tags$ul(
                   tags$li("In medicine or social science, R² of 0.2–0.4 can be perfectly acceptable."),
                   tags$li("In engineering or physics, you might expect R² above 0.9."),
                   tags$li("A high R² does NOT guarantee the model is correct — it only means it explains more variation.")
                 ),
                 
                 h4("Is there a good p-value?"),
                 p("The usual threshold is p < 0.05, but:"),
                 tags$ul(
                   tags$li("A small p-value does NOT mean the model is good."),
                   tags$li("A small p-value does NOT mean the relationship is strong."),
                   tags$li("A small p-value does NOT mean the model shape is correct.")
                 ),
                 
                 h4("How does a beginner know what is appropriate?"),
                 p("Use these simple rules:"),
                 tags$ul(
                   tags$li("If your field expects high precision (engineering, physics), expect higher R²."),
                   tags$li("If your field involves human behaviour or biology, lower R² is normal."),
                   tags$li("If residuals show a curve → your model is too simple, even if R² is high."),
                   tags$li("If residuals show a pattern → you are missing predictors."),
                   tags$li("If residuals look random → the model shape is appropriate.")
                 ),
                 
                 tags$hr(),
                 h3("Understanding Non-Linear Methods"),
                 
                 h4("What is Age² (Quadratic Term)?"),
                 p("Age² lets the model bend once, creating a gentle curve. It is ideal when the relationship is smoothly curved in one direction."),
                 p("It does NOT mean 'more curve' if you increase the exponent. Higher powers (Age³, Age⁴...) create unstable, wiggly models and are rarely appropriate."),
                 tags$pre("lm(BP ~ Age + I(Age^2))"),
                 
                 h4("What are Splines?"),
                 p("Splines allow the model to bend at several points, but in a controlled, smooth way. They are better than high‑order polynomials for more complex curves."),
                 tags$pre("lm(BP ~ bs(Age, df = 4))"),
                 
                 h4("What is a GAM (Generalized Additive Model)?"),
                 p("A GAM automatically learns the shape of the curve from the data. You don't need to guess the formula — the model finds the smooth pattern."),
                 tags$pre("gam(BP ~ s(Age))"),
                 
                 h4("What are Non-linear Models (like Random Forests)?"),
                 p("Random forests do not try to draw a line at all. They split the data into groups and can capture complex shapes, jumps, and interactions."),
                 tags$pre("randomForest(BP ~ Age)"),
                 
                 h4("When to Use Each Method"),
                 tags$ul(
                   tags$li("Use Age² when the curve is simple and smooth."),
                   tags$li("Use Splines when the curve bends in several places."),
                   tags$li("Use GAMs when you want the model to learn the curve automatically."),
                   tags$li("Use Random Forests when the pattern is complex or irregular.")
                 ),
                 
                 tags$hr(),
                 h3("Understanding Bootstrapping"),
                 
                 h4("What is Bootstrapping?"),
                 p("Bootstrapping checks how stable your model is when you have limited data."),
                 p("You repeatedly resample your dataset WITH replacement, refit the model, and see how much the results change."),
                 p("If the model changes a lot, your sample is unstable. If it stays similar, your model is reliable."),
                 tags$pre("
boot_samples <- replicate(1000, {
  idx <- sample(1:nrow(dataset), replace = TRUE)
  coef(lm(BP ~ Age, data = dataset[idx, ]))[2]
})
"),
                 p("Bootstrapping is especially useful for small sample sizes or when you want to estimate uncertainty."),
                 
                 tags$hr(),
                 h3("Which Model Should I Use? (Beginner Decision Tree)"),
                 
                 h4("1. Look at the Scatterplot"),
                 tags$ul(
                   tags$li("Straight-ish line → Use Linear Regression"),
                   tags$li("Gentle curve → Use Age²"),
                   tags$li("Curve bends more than once → Use Splines"),
                   tags$li("Not sure what the shape is → Use a GAM"),
                   tags$li("Messy or irregular pattern → Use a Random Forest")
                 ),
                 
                 h4("2. Look at the Residual Plot"),
                 tags$ul(
                   tags$li("Random cloud → Model shape is appropriate"),
                   tags$li("Curved pattern → Model is too simple (try Age², Splines, or GAM)"),
                   tags$li("Funnel shape → Variance changes with Age (try transformations)"),
                   tags$li("Pattern or stripes → Missing predictors"),
                   tags$li("Big spikes → Outliers")
                 ),
                 
                 h4("3. Think About Your Goal"),
                 tags$ul(
                   tags$li("Want a simple, explainable model? → Linear or Age²"),
                   tags$li("Want flexibility but still smooth? → Splines or GAM"),
                   tags$li("Want accuracy over interpretability? → Random Forest")
                 ),
                 
                 h4("4. Think About Your Data Size"),
                 tags$ul(
                   tags$li("Small dataset → Linear, Age², or GAM"),
                   tags$li("Medium dataset → Splines or GAM"),
                   tags$li("Large dataset → Random Forest works well")
                 ),
                 
                 h4("5. Think About Interpretability"),
                 tags$ul(
                   tags$li("Need to explain the model to others? → Linear or Age²"),
                   tags$li("Need the best prediction? → Random Forest"),
                   tags$li("Need a smooth curve without guessing? → GAM")
                 )
        )
      )
    ),
    
    # ---- MAIN PANEL ----
    mainPanel(
      
      # ---- SIDE-BY-SIDE PLOTS ----
      fluidRow(
        column(
          width = 6,
          h3("Scatterplot + Regression Line"),
          uiOutput("scatter_banner"),
          plotOutput("scatter", height = "350px"),
          uiOutput("scatter_explain")
        ),
        column(
          width = 6,
          h3("Residual Plot"),
          uiOutput("residual_banner"),
          plotOutput("residuals", height = "350px"),
          uiOutput("residual_explain")
        )
      ),
      
      hr(),
      h3("Model Diagnosis"),
      uiOutput("model_quality"),
      hr(),
      
      # ---- SHOW CODE TOGGLE ----
      h3("Show R Code"),
      checkboxInput("show_code", "Show me the R code", FALSE),
      conditionalPanel(
        condition = "input.show_code == true",
        tags$div(class="green-box",
                 tags$pre("
# Fit the model
model <- lm(BP ~ Age, data = dataset)

# Predictions
predicted_bp <- predict(model, dataset)

# Residuals
residuals <- resid(model)

# Fitted values
fitted_values <- fitted(model)
")
        )
      ),
      
      hr(),
      h3("How to Fix This Model"),
      uiOutput("model_improve")
    )
  )
)

server <- function(input, output, session) {
  
  dataset <- reactive({
    set.seed(123)
    
    if (input$scenario == "Good fit (BP increases linearly with age)") {
      age <- runif(200, 20, 80)
      bp  <- 90 + 0.8 * age + rnorm(200, 0, 8)
    }
    
    if (input$scenario == "Too much noise (weak relationship)") {
      age <- runif(200, 20, 80)
      bp  <- 90 + 0.8 * age + rnorm(200, 0, 30)
    }
    
    if (input$scenario == "Non-linear pattern (curved relationship)") {
      age <- runif(200, 20, 80)
      bp  <- 90 + 0.1 * age^2 + rnorm(200, 0, 10)
    }
    
    if (input$scenario == "Missing predictors (age alone is not enough)") {
      age <- runif(200, 20, 80)
      weight <- runif(200, 50, 120)
      bp  <- 70 + 0.5 * age + 0.8 * weight + rnorm(200, 0, 10)
    }
    
    if (input$scenario == "Outliers present (distorts regression)") {
      age <- runif(200, 20, 80)
      bp  <- 90 + 0.8 * age + rnorm(200, 0, 10)
      bp[sample(1:200, 5)] <- bp[sample(1:200, 5)] + 80
    }
    
    if (input$scenario == "Small sample size (unstable model)") {
      age <- runif(15, 20, 80)
      bp  <- 90 + 0.8 * age + rnorm(15, 0, 10)
    }
    
    data.frame(Age = age, BP = bp)
  })
  
  model <- reactive({
    lm(BP ~ Age, data = dataset())
  })
  
  # ---- BANNERS ----
  output$scatter_banner <- renderUI({
    if (input$scenario == "Missing predictors (age alone is not enough)") {
      tags$div(class="warning-box", "⚠ WIDE SPREAD = MISSING PREDICTOR (e.g., Weight)")
    }
  })
  
  output$residual_banner <- renderUI({
    if (input$scenario == "Missing predictors (age alone is not enough)") {
      tags$div(class="error-box", "⚠ PATTERN IN RESIDUALS = MODEL IS MISSING SOMETHING")
    }
    if (input$scenario == "Non-linear pattern (curved relationship)") {
      tags$div(class="warning-box", "⚠ CURVED RESIDUALS = MODEL TOO SIMPLE")
    }
  })
  
  # ---- SCATTERPLOT ----
  output$scatter <- renderPlot({
    ggplot(dataset(), aes(Age, BP)) +
      geom_point(color = "#2e7d32", size = 2.5) +
      geom_smooth(method = "lm", se = TRUE, color = "#1b5e20") +
      theme_minimal(base_size = 14)
  })
  
  output$scatter_explain <- renderUI({
    scenario <- input$scenario
    explanations <- list(
      "Good fit (BP increases linearly with age)" = "Scatterplot: Tight line = good fit.",
      "Too much noise (weak relationship)" = "Scatterplot: Points are everywhere — noise overwhelms the signal.",
      "Non-linear pattern (curved relationship)" = "Scatterplot: The data curves, but the model is straight — the model is too simple.",
      "Missing predictors (age alone is not enough)" = "Scatterplot: Points are VERY spread out. Age matters, but weight also affects BP — and weight is missing.",
      "Outliers present (distorts regression)" = "Scatterplot: A few extreme points pull the line away from the trend.",
      "Small sample size (unstable model)" = "Scatterplot: Too few points — the line is unstable."
    )
    tags$p(explanations[[scenario]])
  })
  
  # ---- RESIDUAL PLOT ----
  output$residuals <- renderPlot({
    m <- model()
    res <- resid(m)
    fit <- fitted(m)
    
    ggplot(data.frame(fit, res), aes(fit, res)) +
      geom_point(color = "#c62828", size = 2.8) +
      geom_smooth(se = FALSE, color = "darkorange", linetype = "solid", size = 1.2) +
      geom_hline(yintercept = 0, linetype = "dashed") +
      theme_minimal(base_size = 14)
  })
  
  output$residual_explain <- renderUI({
    scenario <- input$scenario
    explanations <- list(
      "Good fit (BP increases linearly with age)" = "Residuals: Random scatter = good model.",
      "Too much noise (weak relationship)" = "Residuals: Huge spread = model can't predict well.",
      "Non-linear pattern (curved relationship)" = "Residuals: Clear curve = model is too simple.",
      "Missing predictors (age alone is not enough)" = "Residuals: Pattern = model is missing weight.",
      "Outliers present (distorts regression)" = "Residuals: Extreme spikes = outliers.",
      "Small sample size (unstable model)" = "Residuals: Too few points to judge."
    )
    tags$p(explanations[[scenario]])
  })
  
  # ---- MODEL QUALITY ----
  output$model_quality <- renderUI({
    m <- summary(model())
    r2 <- round(m$r.squared, 3)
    pval <- signif(m$coefficients[2,4], 3)
    
    tags$div(class="panel",
             h4("Model Summary"),
             p(paste("R-squared:", r2)),
             p(paste("p-value:", pval)),
             p("Higher R-squared = better fit. Low p-value = slope is likely real.")
    )
  })
  
  # ---- FIX SUGGESTIONS ----
  output$model_improve <- renderUI({
    scenario <- input$scenario
    
    suggestions <- list(
      "Missing predictors (age alone is not enough)" = tags$ul(
        tags$li("Add Weight."),
        tags$li("Add Sex/Gender."),
        tags$li("Add Smoking Status."),
        tags$li("Add Medication Use."),
        tags$li("Add Exercise Level."),
        tags$li("Add Diet Factors."),
        tags$li("Add Stress or Sleep Quality.")
      ),
      
      "Too much noise (weak relationship)" = tags$ul(
        tags$li("Increase sample size."),
        tags$li("Improve measurement accuracy."),
        tags$li("Add more predictors.")
      ),
      
      "Non-linear pattern (curved relationship)" = tags$ul(
        tags$li("Add Age²: lm(BP ~ Age + I(Age^2))"),
        tags$li("Try Splines: lm(BP ~ bs(Age, df = 4))"),
        tags$li("Try a GAM: gam(BP ~ s(Age))"),
        tags$li("Use Non-linear Models: randomForest(BP ~ Age)")
      ),
      
      "Outliers present (distorts regression)" = tags$ul(
        tags$li("Remove outliers."),
        tags$li("Winsorize outliers."),
        tags$li("Use robust regression.")
      ),
      
      "Small sample size (unstable model)" = tags$ul(
        tags$li("Collect more data."),
        tags$li("Use bootstrapping.")
      ),
      
      "Good fit (BP increases linearly with age)" = tags$ul(
        tags$li("Model is strong."),
        tags$li("Add predictors for more accuracy.")
      )
    )
    
    tags$div(class="panel", suggestions[[scenario]])
  })
}

shinyApp(ui, server)
