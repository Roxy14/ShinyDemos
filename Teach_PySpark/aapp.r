library(shiny)
library(shinyAce)
library(DT)
library(bslib)

ui <- page_sidebar(
  
  # ---------------------------
  # THEME (SUPERHERO)
  # ---------------------------
  theme = bs_theme(
    version = 5,
    bootswatch = "superhero",
    base_font = font_google("Inter"),
    code_font = font_google("Fira Code")
  ),
  
  # ---------------------------
  # GLOBAL STYLES
  # ---------------------------
  tags$head(
    tags$style(HTML("
      /* Code editor improvements */
      .ace_editor {
        min-height: 120px !important;
        max-height: 300px !important;
        font-size: 14px !important;
        border-radius: 6px !important;
      }
      .ace_content {
        color: #f8f8f2 !important;
      }
      .ace-monokai {
        background-color: #1e1e1e !important;
      }
      .ace_scroller {
        padding: 6px !important;
      }

      /* Typography + spacing */
      h2, h3 {
        margin-top: 20px !important;
        margin-bottom: 10px !important;
      }
      p, ul, li {
        margin-bottom: 6px !important;
      }

      /* Card spacing */
      .card {
        margin-bottom: 20px !important;
      }

      /* Center main content */
      .main-container {
        max-width: 900px;
        margin: auto;
      }
    "))
  ),
  
  # ---------------------------
  # SIDEBAR (Minimal + Clean)
  # ---------------------------
  sidebar = sidebar(
    width = 260,
    
    h3("📘 Topics"),
    selectInput(
      "topic",
      NULL,
      choices = c(
        "What is Spark?",
        "Spark Architecture",
        "PySpark Basics",
        "DataFrames",
        "Transformations vs Actions",
        "Joins",
        "Window Functions",
        "String Operations",
        "Numeric Operations",
        "PySpark Functions",
        "Intermediate: Catalyst Optimizer",
        "Intermediate: Tungsten Engine",
        "Intermediate: Partitioning & Shuffling",
        "Intermediate: Repartitioning Strategy",
        "Intermediate: Wide vs Narrow Transformations",
        "Intermediate: Caching & Persistence",
        "Intermediate: Broadcast Joins",
        "Spark DAG Visualizer",
        "Quiz"
      )
    ),
    
    hr(),
    h4("💡 Tip of the Moment"),
    textOutput("tip")
  ),
  
  # ---------------------------
  # MAIN CONTENT
  # ---------------------------
  layout_columns(
    col_widths = c(12),
    
    div(
      class = "main-container",
      card(
        id = "content_card",
        card_body(
          uiOutput("content")
        )
      )
    )
  )
)





server <- function(input, output, session) {
  
  # ----------------------------------------------------------
  # Tips
  # ----------------------------------------------------------
  tips <- c(
    "Spark DataFrames are immutable — every transformation creates a new DataFrame.",
    "Spark is lazy — nothing runs until you trigger an action like show(), count(), or collect().",
    "Avoid mixing Pandas and PySpark — converting large DataFrames can crash the driver.",
    "Avoid Python UDFs when possible — built‑in Spark SQL functions are faster and optimised.",
    "Repartition only when necessary — shuffles are expensive and slow.",
    "Broadcast joins prevent shuffles when one side of the join is small.",
    "Use .explain() to inspect the execution plan and spot shuffles or scans.",
    "Cache or persist DataFrames that you reuse multiple times to avoid recomputation.",
    "Wide transformations (joins, groupBy, distinct) cause shuffles — narrow ones don’t.",
    "Predicate pushdown reduces I/O by filtering data at the storage layer.",
    "Column pruning avoids reading unused columns from Parquet/Delta files.",
    "Skewed keys can cause huge partitions — watch out for uneven join keys.",
    "collect() brings all data to the driver — avoid it unless the data is tiny.",
    "CSV is slow and untyped — prefer Parquet or Delta for real workloads.",
    "Think in columns, not rows — PySpark is vectorised and distributed."
  )
  
  
  observeEvent(input$topic, {
    output$tip <- renderText({
      sample(tips, 1)
    })
  }, ignoreInit = FALSE)
  
  # ----------------------------------------------------------
  # Dynamic content loader
  # ----------------------------------------------------------
  output$content <- renderUI({
    switch(input$topic,
           
           # --------------------------------------------------
           # BASIC SECTIONS
           # --------------------------------------------------
           "What is Spark?" = {
             tagList(
               h2("✨ Welcome to the Spark Learning App"),
               
               p("Fast‑tracking Python and SQL developers into Spark with familiar, comparable examples:"),
               
               tags$ul(
                 tags$li("🔹 PySpark — distributed DataFrames"),
                 tags$li("🔹 Spark SQL — SQL on big data"),
                 tags$li("🔹 Pandas — local Python DataFrames"),
                 tags$li("🔹 Polars — fast, parallel DataFrames")
               ),
               
               h3("🔥 What is Apache Spark?"),
               p("Spark helps you work with big datasets by spreading work across many machines. It's fast, scalable, and friendly to both Python and SQL users."),
               
               
               # -------------------------
               # PySpark Example (highlighted key line)
               # -------------------------
               h3("PySpark"),
               aceEditor(
                 outputId = "code_pyspark",
                 value =
                   "from pyspark.sql import SparkSession  # DataFrame API lives in pyspark.sql

spark = SparkSession.builder.getOrCreate()

df = spark.read.csv('file.csv', header=True)

# PySpark: DataFrame API filter
df.filter(df.age > 30).show()",
                 mode = "python",
                 theme = "monokai",
                 height = "180px"
               ),
               
               
               # -------------------------
               # Spark SQL Example (highlighted key line)
               # -------------------------
               h3("Spark SQL"),
               aceEditor(
                 outputId = "code_sql",
                 value =
                   "from pyspark.sql import SparkSession  # Spark SQL is built into Spark

spark = SparkSession.builder.getOrCreate()

df = spark.read.csv('file.csv', header=True)

df.createOrReplaceTempView('people')  # SQL requires a temp view

# Spark SQL: SQL query
spark.sql('SELECT name, age FROM people WHERE age > 30').show()",
                 mode = "python",
                 theme = "monokai",
                 height = "220px"
               ),
               
               
               # -------------------------
               # Pandas Example (highlighted key line)
               # -------------------------
               h3("Pandas"),
               aceEditor(
                 outputId = "code_pandas",
                 value =
                   "import pandas as pd  # Pandas loads data into local memory

df = pd.read_csv('file.csv')

# Pandas: boolean filtering
df[df['age'] > 30]",
                 mode = "python",
                 theme = "monokai",
                 height = "140px"
               ),
               
               
               # -------------------------
               # Polars Example (highlighted key line)
               # -------------------------
               h3("Polars"),
               aceEditor(
                 outputId = "code_polars",
                 value =
                   "import polars as pl  # Polars uses fast, parallel DataFrames

df = pl.read_csv('file.csv')

# Polars: expression-based filtering
df.filter(pl.col('age') > 30)",
                 mode = "python",
                 theme = "monokai",
                 height = "140px"
               )
             )
           }
           
           
           
           ,
           
           
           
           
           
           
           "Spark Architecture" = {
             tagList(
               h2("🏗 Spark Architecture"),
               
               h3("Key Components"),
               DTOutput("architecture_table"),
               
               tags$hr(),
               
               p("Spark is designed for big data, so it spreads work across many machines. The driver plans the work, executors run tasks in parallel, and the cluster manager assigns resources."),
               
               p("How this compares to other tools:"),
               tags$ul(
                 tags$li("🔹 Pandas — runs on one machine; all data must fit in memory."),
                 tags$li("🔹 Polars — faster and parallel, but still single‑machine."),
                 tags$li("🔹 PySpark / Spark SQL — distribute data and computation across many executors.")
               ),
               
               p("Analogy: Pandas and Polars are like cooking in your own kitchen. Spark is like running a restaurant kitchen — the driver is the head chef, executors are the line cooks, and the cluster manager assigns stations."),
               
               tags$hr(),
               
               h3("Architecture Diagram"),
               HTML('
<div style="width:100%;overflow-x:auto;text-align:center;">
<svg width="760" height="330" viewBox="0 0 760 330" preserveAspectRatio="xMidYMid meet" style="background:#fafafa;border:1px solid #ccc;border-radius:8px;padding:10px;">
  
  <!-- Driver Program -->
  <rect x="230" y="20" width="300" height="60" rx="10" ry="10" fill="#d9eaff" stroke="#5a8fd8" stroke-width="2"></rect>
  <text x="380" y="55" font-size="18" text-anchor="middle" fill="#003366">Driver Program</text>

  <!-- Arrow to Cluster Manager -->
  <line x1="380" y1="80" x2="380" y2="130" stroke="#333" stroke-width="2"></line>
  <polygon points="375,130 385,130 380,140" fill="#333"></polygon>

  <!-- Cluster Manager -->
  <rect x="180" y="140" width="400" height="60" rx="10" ry="10" fill="#ffeccc" stroke="#d89a3c" stroke-width="2"></rect>
  <text x="380" y="175" font-size="18" text-anchor="middle" fill="#663300">Cluster Manager</text>

  <!-- Arrows to Executors -->
  <line x1="380" y1="200" x2="200" y2="250" stroke="#333" stroke-width="2"></line>
  <polygon points="195,250 205,250 200,260" fill="#333"></polygon>

  <line x1="380" y1="200" x2="380" y2="250" stroke="#333" stroke-width="2"></line>
  <polygon points="375,250 385,250 380,260" fill="#333"></polygon>

  <line x1="380" y1="200" x2="560" y2="250" stroke="#333" stroke-width="2"></line>
  <polygon points="555,250 565,250 560,260" fill="#333"></polygon>

  <!-- Executors -->
  <rect x="120" y="260" width="160" height="60" rx="10" ry="10" fill="#e8ffe8" stroke="#4caf50" stroke-width="2"></rect>
  <text x="200" y="295" font-size="16" text-anchor="middle" fill="#1b5e20">Executor 1</text>

  <rect x="300" y="260" width="160" height="60" rx="10" ry="10" fill="#e8ffe8" stroke="#4caf50" stroke-width="2"></rect>
  <text x="380" y="295" font-size="16" text-anchor="middle" fill="#1b5e20">Executor 2</text>

  <rect x="480" y="260" width="160" height="60" rx="10" ry="10" fill="#e8ffe8" stroke="#4caf50" stroke-width="2"></rect>
  <text x="560" y="295" font-size="16" text-anchor="middle" fill="#1b5e20">Executor N</text>

</svg>
</div>
'),
               
               tags$hr(),
               
               h3("Why This Matters"),
               p("Spark can scale far beyond what Pandas or Polars can handle because it runs work across many executors instead of a single machine.")
             )
           }
           
           
           
           
           
           
           ,
           "PySpark Basics" = {
             tagList(
               h2("🐍 PySpark Basics"),
               
               h3("Core Concepts"),
               DTOutput("pyspark_basics_table"),
               
               tags$hr(),
               
               ###############################################
               # THINK IN COLUMNS, NOT ROWS
               ###############################################
               h3("Think in Columns, Not Rows"),
               p("PySpark does not loop over rows like Pandas. It transforms entire columns at once, across many machines. If you catch yourself thinking 'for each row…', you're in Pandas mode. Spark wants you to think: 'take this column and apply a transformation to it'."),
               HTML('
<div style="background:#222;padding:10px;border-radius:6px;margin-bottom:10px;">
<b>Pandas (row thinking)</b><br>
<pre style="color:#ddd;">df["label"] = df.apply(lambda r: r["age"] > 30, axis=1)</pre>
<b>PySpark (column thinking)</b><br>
<pre style="color:#4da6ff;">df = df.withColumn("label", F.col("age") > 30)</pre>
</div>
'),
               p("Rule of thumb: if you are touching rows, you're doing it wrong. If you're transforming columns, you're doing it right."),
               
               tags$hr(),
               
               h3("DataFrames Are Immutable"),
               p("PySpark DataFrames never change in place. Every transformation creates a new DataFrame. This makes Spark predictable and easy to optimise."),
               HTML("<pre style='background:#222;padding:10px;border-radius:6px;'>df2 = df.withColumn('x2', F.col('x') * 2)</pre>"),
               p("Rule of thumb: Spark never updates a DataFrame — it always returns a new one."),
               tags$hr(),
               
               
               ###############################################
               # EXECUTION DIAGRAM
               ###############################################
               h3("How PySpark Executes Your Code"),
               HTML('
<div style="width:100%;overflow-x:auto;text-align:center;">
<svg width="100%" height="260" viewBox="0 0 800 260" preserveAspectRatio="xMidYMid meet" style="max-width:800px;background:#fafafa;border:1px solid #ccc;border-radius:8px;padding:10px;">

  <!-- Python Code -->
  <rect x="40" y="40" width="200" height="60" rx="10" ry="10" fill="#d9eaff" stroke="#5a8fd8" stroke-width="2"></rect>
  <text x="140" y="75" font-size="18" text-anchor="middle" fill="#003366">Your Python Code</text>

  <!-- Arrow -->
  <line x1="240" y1="70" x2="330" y2="70" stroke="#333" stroke-width="2"></line>
  <polygon points="330,65 330,75 340,70" fill="#333"></polygon>

  <!-- PySpark API -->
  <rect x="340" y="40" width="200" height="60" rx="10" ry="10" fill="#ffeccc" stroke="#d89a3c" stroke-width="2"></rect>
  <text x="440" y="75" font-size="18" text-anchor="middle" fill="#663300">PySpark API</text>

  <!-- Arrow -->
  <line x1="440" y1="100" x2="440" y2="150" stroke="#333" stroke-width="2"></line>
  <polygon points="435,150 445,150 440,160" fill="#333"></polygon>

  <!-- Spark Engine -->
  <rect x="300" y="160" width="280" height="60" rx="10" ry="10" fill="#e8ffe8" stroke="#4caf50" stroke-width="2"></rect>
  <text x="440" y="195" font-size="18" text-anchor="middle" fill="#1b5e20">Spark Engine</text>

</svg>
</div>
'),
               
               tags$hr(),
               
               ###############################################
               # SPARK SESSION
               ###############################################
               h3("Creating a SparkSession"),
               p("The SparkSession is your entry point to PySpark. You can optionally give your job a name — helpful when viewing it in the Spark UI."),
               aceEditor(
                 "code_pyspark_session",
                 value = paste(
                   "from pyspark.sql import SparkSession",
                   "",
                   "# Version with a job name (shows up in Spark UI)",
                   "spark = (",
                   "    SparkSession.builder",
                   "        .appName('MyApp')  # Optional: a friendly name for your Spark job",
                   "        .getOrCreate()",
                   ")",
                   "",
                   "# Simple version — Spark picks a default name like 'pyspark-shell'",
                   "# spark = SparkSession.builder.getOrCreate()",
                   "",
                   "spark",
                   sep = "\n"
                 ),
                 mode = "python",
                 theme = "monokai",
                 height = "260px"
               ),
               
               tags$hr(),
               
               ###############################################
               # READING DATA
               ###############################################
               h3("Reading Data"),
               p("PySpark can read many formats. Here are the most common ones:"),
               aceEditor(
                 "code_pyspark_read",
                 value = paste(
                   "# CSV",
                   "df = spark.read.csv('data.csv', header=True, inferSchema=True)",
                   "",
                   "# Parquet",
                   "df_parquet = spark.read.parquet('data.parquet')",
                   "",
                   "# JSON",
                   "df_json = spark.read.json('data.json')",
                   sep = "\n"
                 ),
                 mode = "python",
                 theme = "monokai",
                 height = "200px"
               ),
               
               tags$hr(),
               
               ###############################################
               # PYSPARK EXAMPLE
               ###############################################
               h3("PySpark"),
               p("A tiny example showing the typical PySpark workflow: load → transform → show."),
               aceEditor(
                 "code_pyspark_expected",
                 value = paste(
                   "from pyspark.sql import SparkSession",
                   "",
                   "spark = SparkSession.builder.getOrCreate()",
                   "",
                   "# Load a CSV as a DataFrame",
                   "df = spark.read.csv('people.csv', header=True, inferSchema=True)",
                   "",
                   "# PySpark: DataFrame API filter",
                   "df.filter(df.age > 30).select('name', 'age').show()",
                   sep = "\n"
                 ),
                 mode = "python",
                 theme = "monokai",
                 height = "200px"
               ),
               
               tags$hr(),
               
               ###############################################
               # SPARK SQL
               ###############################################
               h3("Spark SQL"),
               p("You can also run SQL by creating a temporary view."),
               aceEditor(
                 "sql_pyspark_basics",
                 value = paste(
                   "df.createOrReplaceTempView('people')",
                   "",
                   "spark.sql(\"\"\"",
                   "SELECT name, age",
                   "FROM people",
                   "WHERE age > 30",
                   "\"\"\").show()",
                   sep = "\n"
                 ),
                 mode = "python",
                 theme = "monokai",
                 height = "200px"
               ),
               
               tags$hr(),
               
               ###############################################
               # PANDAS + POLARS COMPARISON
               ###############################################
               h3("How This Looks in Pandas and Polars"),
               p("Same operation, three different tools — so you can instantly relate PySpark to what you already know."),
               aceEditor(
                 "code_compare_pyspark_pandas_polars",
                 value = paste(
                   "# PySpark (distributed)",
                   "df.filter(df.age > 30).select('name', 'age').show()",
                   "",
                   "# Pandas (single machine)",
                   "df[df['age'] > 30][['name', 'age']]",
                   "",
                   "# Polars (single machine, fast)",
                   "df.filter(pl.col('age') > 30).select(['name', 'age'])",
                   sep = "\n"
                 ),
                 mode = "python",
                 theme = "monokai",
                 height = "200px"
               )
             )
           }
           
           
           
           
           
           
           ,
           
           
           
           
           "DataFrames" = {
             tagList(
               h2("📊 Spark DataFrames"),
               
               p("A Spark DataFrame is like a table with rows and columns — similar to Pandas, Polars, or a SQL table — but it is distributed across many machines. This means you can work with datasets far bigger than your laptop’s memory."),
               
               p("If you already know Pandas or Polars, think of a Spark DataFrame as:"),
               tags$ul(
                 tags$li("🔹 Pandas DataFrame → runs on your laptop"),
                 tags$li("🔹 Polars DataFrame → runs on your laptop but faster and parallel"),
                 tags$li("🔹 Spark DataFrame → same idea, but spread across a cluster"),
                 tags$li("🔹 SQL table → Spark DataFrames behave almost the same")
               ),
               
               tags$hr(),
               
               h3("DataFrame Operations Table"),
               DTOutput("df_table"),
               
               tags$hr(),
               
               h3("Common PySpark DataFrame Operations"),
               p("These are the most common transformations you’ll use. They look similar to Pandas and Polars, but run in parallel across a cluster."),
               aceEditor(
                 "code_dataframes",
                 value = paste(
                   "# Select columns",
                   "df.select('col1', 'col2')",
                   "",
                   "# Filter rows",
                   "df.filter(df.col1 > 10)",
                   "",
                   "# Add a new column",
                   "df.withColumn('new_col', df.col1 * 2)",
                   "",
                   "# Group and aggregate",
                   "df.groupBy('category').count()",
                   sep = '\n'
                 ),
                 mode = "python",
                 theme = "monokai",
                 height = "220px"
               ),
               
               tags$hr(),
               
               h3("How This Looks in Pandas and Polars"),
               p("Same operations, written in the tools you already know:"),
               aceEditor(
                 "code_compare_df",
                 value = paste(
                   "# Pandas",
                   "df[['col1', 'col2']]",
                   "df[df['col1'] > 10]",
                   "df.assign(new_col=df['col1'] * 2)",
                   "df.groupby('category').size()",
                   "",
                   "# Polars",
                   "df.select(['col1', 'col2'])",
                   "df.filter(pl.col('col1') > 10)",
                   "df.with_columns((pl.col('col1') * 2).alias('new_col'))",
                   "df.groupby('category').count()",
                   sep = '\n'
                 ),
                 mode = "python",
                 theme = "monokai",
                 height = "220px"
               ),
               
               tags$hr(),
               
               h3("SQL Equivalent"),
               p("Spark DataFrame operations map almost 1‑to‑1 with SQL. If you know SQL, Spark will feel familiar."),
               aceEditor(
                 "sql_dataframes",
                 value = paste(
                   "SELECT col1, col2 FROM table;",
                   "SELECT * FROM table WHERE col1 > 10;",
                   "SELECT col1, col1 * 2 AS new_col FROM table;",
                   "SELECT category, COUNT(*) FROM table GROUP BY category;",
                   sep = '\n'
                 ),
                 mode = "sql",
                 theme = "monokai",
                 height = "180px"
               ),
               
               tags$hr(),
               
               h3("Using spark.sql()"),
               p("You can run the same SQL directly inside PySpark:"),
               aceEditor(
                 "sql_api_dataframes",
                 value = paste(
                   "spark.sql(\"SELECT col1, col2 FROM table\")",
                   "spark.sql(\"SELECT * FROM table WHERE col1 > 10\")",
                   "spark.sql(\"SELECT col1, col1 * 2 AS new_col FROM table\")",
                   "spark.sql(\"SELECT category, COUNT(*) FROM table GROUP BY category\")",
                   sep = '\n'
                 ),
                 mode = "python",
                 theme = "monokai",
                 height = "180px"
               )
             )
           }
           
           
           ,
           
           "Transformations vs Actions" = {
             tagList(
               h2("⚙ Transformations vs ⚡ Actions"),
               
               p("Spark DataFrames behave differently from Pandas and Polars because Spark is distributed. 
       Instead of running each step immediately, Spark builds a plan first (transformations) 
       and only runs it when you ask for a result (actions)."),
               
               p("If you're used to Pandas or Polars:"),
               tags$ul(
                 tags$li("🔹 Pandas & Polars → every operation runs immediately"),
                 tags$li("🔹 Spark → most operations are lazy (they wait)"),
                 tags$li("🔹 Spark only runs work when you request a final answer")
               ),
               
               p("A simple analogy:"),
               p("Transformations are like writing down cooking steps. Actions are when you actually start cooking."),
               
               tags$hr(),
               
               h3("Comparison Table"),
               DTOutput("transform_table"),
               
               tags$hr(),
               
               h3("Transformations (Lazy)"),
               p("Transformations describe what you want to do, but Spark does not run them yet. 
       It builds a logical plan — similar to SQL query planning."),
               tags$ul(
                 tags$li("select"),
                 tags$li("filter"),
                 tags$li("withColumn"),
                 tags$li("join")
               ),
               
               h3("Actions (Trigger Execution)"),
               p("Actions make Spark actually run the computation across the cluster and return results."),
               tags$ul(
                 tags$li("show"),
                 tags$li("count"),
                 tags$li("collect"),
                 tags$li("write")
               ),
               
               tags$hr(),
               
               h3("PySpark Example"),
               p("Here, the filter is lazy — Spark waits. The count() is an action, so Spark finally runs the job."),
               aceEditor(
                 "code_transformations",
                 value = paste(
                   "df2 = df.filter(df.age > 30)   # transformation (lazy)",
                   "count = df2.count()            # action (triggers execution)",
                   sep = '\n'
                 ),
                 mode = "python",
                 theme = "monokai",
                 height = "140px"
               ),
               
               tags$hr(),
               
               h3("How This Looks in Pandas and Polars"),
               p("In Pandas and Polars, everything runs immediately — there is no lazy plan."),
               aceEditor(
                 "code_compare_transform",
                 value = paste(
                   "# Pandas (runs immediately)",
                   "df2 = df[df['age'] > 30]",
                   "count = len(df2)",
                   "",
                   "# Polars (runs immediately unless using lazy mode)",
                   "df2 = df.filter(pl.col('age') > 30)",
                   "count = df2.height()",
                   sep = '\n'
                 ),
                 mode = "python",
                 theme = "monokai",
                 height = "180px"
               ),
               
               tags$hr(),
               
               h3("SQL Equivalent"),
               p("SQL also separates the idea of planning vs executing. A SELECT builds a plan; returning rows is the action."),
               aceEditor(
                 "sql_transformations",
                 value = paste(
                   "-- Transformation (logical only)",
                   "SELECT * FROM table WHERE age > 30;",
                   "",
                   "-- Action (returns results)",
                   "SELECT COUNT(*) FROM table WHERE age > 30;",
                   sep = '\n'
                 ),
                 mode = "sql",
                 theme = "monokai",
                 height = "160px"
               ),
               
               h3("Using spark.sql()"),
               aceEditor(
                 "sql_api_transformations",
                 value = paste(
                   "spark.sql(\"SELECT * FROM table WHERE age > 30\")        # transformation-like",
                   "spark.sql(\"SELECT COUNT(*) FROM table WHERE age > 30\")  # action",
                   sep = '\n'
                 ),
                 mode = "python",
                 theme = "monokai",
                 height = "140px"
               )
             )
           }
           
           ,
           
           
           
           
           "Joins" = {
             tagList(
               h2("🔗 Joins in PySpark"),
               
               p("A join combines two DataFrames based on a matching column. 
       If you’ve used Pandas, Polars, or SQL, Spark joins work almost the same — 
       the only difference is that Spark performs the join across a cluster."),
               
               p("Think of a join like matching two lists:"),
               tags$ul(
                 tags$li("🔹 Pandas → merge() on your laptop"),
                 tags$li("🔹 Polars → join() on your laptop, fast and parallel"),
                 tags$li("🔹 SQL → JOIN keyword"),
                 tags$li("🔹 Spark → same idea, but distributed across many machines")
               ),
               
               tags$hr(),
               
               h3("Join Types Overview"),
               DTOutput("joins_table"),
               
               tags$hr(),
               
               h3("Join Type Visuals (Green = Returned Rows)"),
               HTML('
<div style="display:grid;grid-template-columns:repeat(2,1fr);gap:30px;justify-items:center;">

  <!-- INNER JOIN -->
  <div>
    <h4 style="text-align:center;">Inner Join</h4>
    <svg width="300" height="180" viewBox="0 0 300 180">
      <circle cx="110" cy="90" r="55" fill="#d9eaff" stroke="#5a8fd8" stroke-width="2"></circle>
      <circle cx="180" cy="90" r="55" fill="#ffeccc" stroke="#d89a3c" stroke-width="2"></circle>
      <circle cx="145" cy="90" r="30" fill="#4caf50"></circle>
    </svg>
    <p style="text-align:center;">Matching rows from both tables</p>
  </div>

  <!-- LEFT JOIN -->
  <div>
    <h4 style="text-align:center;">Left Join</h4>
    <svg width="300" height="180" viewBox="0 0 300 180">
      <circle cx="110" cy="90" r="55" fill="#d9eaff" stroke="#5a8fd8" stroke-width="2"></circle>
      <circle cx="180" cy="90" r="55" fill="#ffeccc" stroke="#d89a3c" stroke-width="2"></circle>
      <circle cx="110" cy="90" r="55" fill="#4caf50" opacity="0.5"></circle>
    </svg>
    <p style="text-align:center;">All left rows + matches</p>
  </div>

  <!-- RIGHT JOIN -->
  <div>
    <h4 style="text-align:center;">Right Join</h4>
    <svg width="300" height="180" viewBox="0 0 300 180">
      <circle cx="110" cy="90" r="55" fill="#d9eaff" stroke="#5a8fd8" stroke-width="2"></circle>
      <circle cx="180" cy="90" r="55" fill="#ffeccc" stroke="#d89a3c" stroke-width="2"></circle>
      <circle cx="180" cy="90" r="55" fill="#4caf50" opacity="0.5"></circle>
    </svg>
    <p style="text-align:center;">All right rows + matches</p>
  </div>

  <!-- FULL OUTER JOIN -->
  <div>
    <h4 style="text-align:center;">Full Outer Join</h4>
    <svg width="300" height="180" viewBox="0 0 300 180">
      <circle cx="110" cy="90" r="55" fill="#d9eaff" stroke="#5a8fd8" stroke-width="2"></circle>
      <circle cx="180" cy="90" r="55" fill="#ffeccc" stroke="#d89a3c" stroke-width="2"></circle>
      <circle cx="110" cy="90" r="55" fill="#4caf50" opacity="0.3"></circle>
      <circle cx="180" cy="90" r="55" fill="#4caf50" opacity="0.3"></circle>
    </svg>
    <p style="text-align:center;">All rows from both tables</p>
  </div>

  <!-- LEFT SEMI JOIN -->
  <div>
    <h4 style="text-align:center;">Left Semi Join</h4>
    <svg width="300" height="180" viewBox="0 0 300 180">
      <circle cx="110" cy="90" r="55" fill="#d9eaff" stroke="#5a8fd8" stroke-width="2"></circle>
      <circle cx="180" cy="90" r="55" fill="#ffeccc" stroke="#d89a3c" stroke-width="2"></circle>
      <circle cx="140" cy="90" r="30" fill="#4caf50"></circle>
    </svg>
    <p style="text-align:center;">Left rows that have a match</p>
  </div>

  <!-- LEFT ANTI JOIN -->
  <div>
    <h4 style="text-align:center;">Left Anti Join</h4>
    <svg width="300" height="180" viewBox="0 0 300 180">
      <circle cx="110" cy="90" r="55" fill="#d9eaff" stroke="#5a8fd8" stroke-width="2"></circle>
      <circle cx="180" cy="90" r="55" fill="#ffeccc" stroke="#d89a3c" stroke-width="2"></circle>
      <circle cx="110" cy="90" r="55" fill="#4caf50" opacity="0.5"></circle>
      <circle cx="145" cy="90" r="30" fill="#fafafa"></circle>
    </svg>
    <p style="text-align:center;">Left rows with NO match</p>
  </div>

  <!-- CROSS JOIN -->
  <div>
    <h4 style="text-align:center;">Cross Join</h4>
    <svg width="300" height="180" viewBox="0 0 300 180">
      <rect x="70" y="60" width="70" height="50" fill="#d9eaff" stroke="#5a8fd8" stroke-width="2"></rect>
      <rect x="160" y="60" width="70" height="50" fill="#ffeccc" stroke="#d89a3c" stroke-width="2"></rect>
      <text x="150" y="150" text-anchor="middle" fill="#4caf50">All combinations</text>
    </svg>
    <p style="text-align:center;">Cartesian product</p>
  </div>

</div>
'),
               
               tags$hr(),
               
               h3("PySpark Examples"),
               p("Spark uses the same join names as SQL. Here are the most common ones:"),
               aceEditor(
                 "code_joins",
                 value = paste(
                   "df.join(df2, on='id', how='inner')",
                   "df.join(df2, on='id', how='left')",
                   "df.join(df2, on='id', how='right')",
                   "df.join(df2, on='id', how='outer')",
                   "df.join(df2, on='id', how='left_semi')",
                   "df.join(df2, on='id', how='left_anti')",
                   "df.crossJoin(df2)",
                   sep = '\n'
                 ),
                 mode = "python",
                 theme = "monokai",
                 height = "220px"
               ),
               
               tags$hr(),
               
               h3("How This Looks in Pandas and Polars"),
               p("Same joins, written in the tools you already know:"),
               aceEditor(
                 "code_compare_joins",
                 value = paste(
                   "# Pandas",
                   "df.merge(df2, on='id', how='inner')",
                   "df.merge(df2, on='id', how='left')",
                   "df.merge(df2, on='id', how='right')",
                   "df.merge(df2, on='id', how='outer')",
                   "",
                   "# Polars",
                   "df.join(df2, on='id', how='inner')",
                   "df.join(df2, on='id', how='left')",
                   "df.join(df2, on='id', how='right')",
                   "df.join(df2, on='id', how='outer')",
                   sep = '\n'
                 ),
                 mode = "python",
                 theme = "monokai",
                 height = "220px"
               ),
               
               tags$hr(),
               
               h3("SQL Equivalent"),
               aceEditor(
                 "sql_joins",
                 value = paste(
                   "SELECT * FROM A INNER JOIN B USING (id);",
                   "SELECT * FROM A LEFT JOIN B USING (id);",
                   "SELECT * FROM A RIGHT JOIN B USING (id);",
                   "SELECT * FROM A FULL OUTER JOIN B USING (id);",
                   "SELECT A.* FROM A LEFT SEMI JOIN B USING (id);",
                   "SELECT A.* FROM A LEFT ANTI JOIN B USING (id);",
                   "SELECT * FROM A CROSS JOIN B;",
                   sep = '\n'
                 ),
                 mode = "sql",
                 theme = "monokai",
                 height = "220px"
               ),
               
               tags$hr(),
               
               h3("Using spark.sql()"),
               aceEditor(
                 "sql_api_joins",
                 value = paste(
                   "spark.sql(\"SELECT * FROM A INNER JOIN B USING (id)\")",
                   "spark.sql(\"SELECT * FROM A LEFT JOIN B USING (id)\")",
                   "spark.sql(\"SELECT * FROM A RIGHT JOIN B USING (id)\")",
                   "spark.sql(\"SELECT * FROM A FULL OUTER JOIN B USING (id)\")",
                   "spark.sql(\"SELECT A.* FROM A LEFT SEMI JOIN B USING (id)\")",
                   "spark.sql(\"SELECT A.* FROM A LEFT ANTI JOIN B USING (id)\")",
                   "spark.sql(\"SELECT * FROM A CROSS JOIN B\")",
                   sep = '\n'
                 ),
                 mode = "python",
                 theme = "monokai",
                 height = "220px"
               )
             )
           }
           
           
           
           
           
           
           
           ,
           "Window Functions" = {
             tagList(
               
               h2("🪟 Window Functions in PySpark"),
               
               p("Window functions let you compute values across rows — like rankings, running totals, or comparing a row to the previous one — without collapsing the DataFrame like GROUP BY would."),
               
               tags$hr(),
               
               h3("Window Definition"),
               p("All window functions below use this window:"),
               
               HTML('
      <pre style="background:#f0f0f0;color:#222;padding:12px;border:1px solid #ccc;border-radius:6px;font-size:14px;">
PARTITION BY category
ORDER BY amount ASC
      </pre>
    '),
               
               p("Rows are grouped by category (orange for A, green for B)."),
               p("Within each category, rows are ordered by amount (blue)."),
               p("Every window function below uses these two columns."),
               
               tags$hr(),
               
               h3("Window Function Outputs (All Columns Together)"),
               p("Each column shows the result of a different window function:"),
               
               tags$ul(
                 tags$li(HTML("📌 <strong>row_number</strong> → unique row index within category")),
                 tags$li(HTML("📌 <strong>rank</strong> → ties share rank, gaps appear")),
                 tags$li(HTML("📌 <strong>dense_rank</strong> → ties share rank, no gaps")),
                 tags$li(HTML("📌 <strong>lag_amount</strong> → previous row’s amount")),
                 tags$li(HTML("📌 <strong>lead_amount</strong> → next row’s amount")),
                 tags$li(HTML("📌 <strong>moving_avg</strong> → average of current + previous 2 rows"))
               ),
               
               HTML('
  <table style="margin:auto;border-collapse:collapse;table-layout:fixed;width:100%;font-size:13px;">

    <tr>
      <th>id</th>
      <th>category</th>
      <th>date</th>
      <th>amount</th>
      <th>row_number</th>
      <th>rank</th>
      <th>dense_rank</th>
      <th>lag_amount</th>
      <th>lead_amount</th>
      <th>moving_avg</th>
    </tr>

    <!-- CATEGORY A -->
    <tr>
      <td>1</td>
      <td style="color:#ffb347;">A</td>
      <td>2024-01-01</td>
      <td style="color:#4da6ff;">100</td>
      <td>1</td><td>1</td><td>1</td><td>null</td><td>150</td><td>100</td>
    </tr>

    <tr>
      <td>2</td>
      <td style="color:#ffb347;">A</td>
      <td>2024-01-02</td>
      <td style="color:#4da6ff;">150</td>
      <td>2</td><td>2</td><td>2</td><td>100</td><td>150</td><td>125</td>
    </tr>

    <tr>
      <td>3</td>
      <td style="color:#ffb347;">A</td>
      <td>2024-01-03</td>
      <td style="color:#4da6ff;">150</td>
      <td>3</td><td>2</td><td>2</td><td>150</td><td>200</td><td>133.3</td>
    </tr>

    <tr>
      <td>4</td>
      <td style="color:#ffb347;">A</td>
      <td>2024-01-04</td>
      <td style="color:#4da6ff;">200</td>
      <td>4</td><td>4</td><td>3</td><td>150</td><td>null</td><td>150</td>
    </tr>

    <!-- CATEGORY B -->
    <tr>
      <td>5</td>
      <td style="color:#7CFC00;">B</td>
      <td>2024-01-01</td>
      <td style="color:#4da6ff;">150</td>
      <td>1</td><td>1</td><td>1</td><td>null</td><td>150</td><td>150</td>
    </tr>

    <tr>
      <td>6</td>
      <td style="color:#7CFC00;">B</td>
      <td>2024-01-02</td>
      <td style="color:#4da6ff;">150</td>
      <td>2</td><td>1</td><td>1</td><td>150</td><td>180</td><td>150</td>
    </tr>

    <tr>
      <td>7</td>
      <td style="color:#7CFC00;">B</td>
      <td>2024-01-03</td>
      <td style="color:#4da6ff;">180</td>
      <td>3</td><td>3</td><td>2</td><td>150</td><td>220</td><td>160</td>
    </tr>

    <tr>
      <td>8</td>
      <td style="color:#7CFC00;">B</td>
      <td>2024-01-04</td>
      <td style="color:#4da6ff;">220</td>
      <td>4</td><td>4</td><td>3</td><td>180</td><td>null</td><td>183.3</td>
    </tr>

  </table>
')
               
               ,
               tags$hr(),
               
               h3("How This Looks in PySpark, SQL, Spark SQL, Pandas, and Polars"),
               p("Below are the same window functions expressed in different tools. All of them use the same window: partition by category, order by amount."),
               
               # -------------------------
               # PySpark
               # -------------------------
               h4("PySpark (DataFrame API)"),
               aceEditor(
                 "code_windows_pyspark_short",
                 value = paste(
                   "from pyspark.sql.window import Window",
                   "import pyspark.sql.functions as F",
                   "",
                   "w = Window.partitionBy('category').orderBy('amount')",
                   "",
                   "df = df \\",
                   "  .withColumn('row_number', F.row_number().over(w)) \\",
                   "  .withColumn('rank', F.rank().over(w)) \\",
                   "  .withColumn('dense_rank', F.dense_rank().over(w)) \\",
                   "  .withColumn('lag_amount', F.lag('amount').over(w)) \\",
                   "  .withColumn('lead_amount', F.lead('amount').over(w)) \\",
                   "  .withColumn('moving_avg', F.avg('amount').over(w.rowsBetween(-2, 0)))",
                   sep = "\n"
                 ),
                 mode = "python",
                 theme = "monokai",
                 height = "230px"
               ),
               
               # -------------------------
               # SQL (expressions only)
               # -------------------------
               h4("SQL (Window Expressions Only)"),
               aceEditor(
                 "code_windows_sql_short",
                 value = paste(
                   "ROW_NUMBER() OVER (PARTITION BY category ORDER BY amount) AS row_number,",
                   "RANK() OVER (PARTITION BY category ORDER BY amount) AS rank,",
                   "DENSE_RANK() OVER (PARTITION BY category ORDER BY amount) AS dense_rank,",
                   "LAG(amount) OVER (PARTITION BY category ORDER BY amount) AS lag_amount,",
                   "LEAD(amount) OVER (PARTITION BY category ORDER BY amount) AS lead_amount,",
                   "AVG(amount) OVER (",
                   "  PARTITION BY category",
                   "  ORDER BY amount",
                   "  ROWS BETWEEN 2 PRECEDING AND CURRENT ROW",
                   ") AS moving_avg",
                   sep = "\n"
                 ),
                 mode = "sql",
                 theme = "monokai",
                 height = "220px"
               ),
               
               # -------------------------
               # Spark SQL (full query with spark.sql)
               # -------------------------
               h4("Spark SQL (Full Query Using spark.sql)"),
               aceEditor(
                 "code_windows_sparksql",
                 value = paste(
                   'spark.sql("""',
                   'SELECT *,',
                   '  ROW_NUMBER() OVER (PARTITION BY category ORDER BY amount) AS row_number,',
                   '  RANK() OVER (PARTITION BY category ORDER BY amount) AS rank,',
                   '  DENSE_RANK() OVER (PARTITION BY category ORDER BY amount) AS dense_rank,',
                   '  LAG(amount) OVER (PARTITION BY category ORDER BY amount) AS lag_amount,',
                   '  LEAD(amount) OVER (PARTITION BY category ORDER BY amount) AS lead_amount,',
                   '  AVG(amount) OVER (',
                   '      PARTITION BY category',
                   '      ORDER BY amount',
                   '      ROWS BETWEEN 2 PRECEDING AND CURRENT ROW',
                   '  ) AS moving_avg',
                   'FROM sales',
                   '""")',
                   sep = "\n"
                 ),
                 mode = "python",
                 theme = "monokai",
                 height = "260px"
               ),
               
               # -------------------------
               # Pandas
               # -------------------------
               h4("Pandas"),
               aceEditor(
                 "code_windows_pandas",
                 value = paste(
                   "# Ranking",
                   "df['row_number'] = df.sort_values('amount').groupby('category').cumcount() + 1",
                   "df['rank'] = df.groupby('category')['amount'].rank(method='min')",
                   "df['dense_rank'] = df.groupby('category')['amount'].rank(method='dense')",
                   "",
                   "# Lag / Lead",
                   "df['lag_amount'] = df.groupby('category')['amount'].shift(1)",
                   "df['lead_amount'] = df.groupby('category')['amount'].shift(-1)",
                   "",
                   "# Moving average (previous 2 rows + current)",
                   "df['moving_avg'] = (",
                   "    df.groupby('category')['amount']",
                   "      .rolling(3).mean().reset_index(0, drop=True)",
                   ")",
                   sep = "\n"
                 ),
                 mode = "python",
                 theme = "monokai",
                 height = "260px"
               ),
               
               # -------------------------
               # Polars
               # -------------------------
               h4("Polars"),
               aceEditor(
                 "code_windows_polars",
                 value = paste(
                   "df = df.with_columns([",
                   "    pl.col('amount').rank('ordinal').over('category').alias('row_number'),",
                   "    pl.col('amount').rank('min').over('category').alias('rank'),",
                   "    pl.col('amount').rank('dense').over('category').alias('dense_rank'),",
                   "",
                   "    pl.col('amount').shift(1).over('category').alias('lag_amount'),",
                   "    pl.col('amount').shift(-1).over('category').alias('lead_amount'),",
                   "",
                   "    pl.col('amount').rolling_mean(3).over('category').alias('moving_avg')",
                   "])",
                   sep = "\n"
                 ),
                 mode = "python",
                 theme = "monokai",
                 height = "260px"
               )
             )
           }
           
           
           
           
           
           
           
           ,
           
           
           
           
           "String Operations" = {
             tagList(
               
               h2("🔤 String Operations in PySpark"),
               
               p("These examples show the most common string transformations used in PySpark. Each table shows a simple before → after view so beginners can clearly see what each function does."),
               
               tags$hr(),
               
               
               
               
               fluidRow(
                 
                 ###############################################
                 # LEFT COLUMN
                 ###############################################
                 column(
                   width = 6,
                   style = "font-size:12px;",
                   
                   div(style="margin-bottom:12px;",
                       h4("1. trim(name)"),
                       div(style="background:#222;padding:6px;border-radius:6px;",
                           HTML("<b>␣␣Alice␣␣</b> → <span style='color:#4da6ff;'>Alice</span><br>
              <b>bob</b> → <span style='color:#4da6ff;'>bob</span><br>
              <b>CHARLIE</b> → <span style='color:#4da6ff;'>CHARLIE</span>")
                       )
                   ),
                   
                   div(style="margin-bottom:12px;",
                       h4("2. lower(email)"),
                       div(style="background:#222;padding:6px;border-radius:6px;",
                           HTML("<b>ALICE@example.COM</b> → <span style='color:#4da6ff;'>alice@example.com</span><br>
              <b>bob_smith@Example.com</b> → <span style='color:#4da6ff;'>bob_smith@example.com</span><br>
              <b>charlie99@domain.org</b> → <span style='color:#4da6ff;'>charlie99@domain.org</span>")
                       )
                   ),
                   
                   div(style="margin-bottom:12px;",
                       h4("3. upper(name)"),
                       div(style="background:#222;padding:6px;border-radius:6px;",
                           HTML("<b>Alice</b> → <span style='color:#4da6ff;'>ALICE</span><br>
              <b>bob</b> → <span style='color:#4da6ff;'>BOB</span><br>
              <b>CHARLIE</b> → <span style='color:#4da6ff;'>CHARLIE</span>")
                       )
                   ),
                   
                   div(style="margin-bottom:12px;",
                       h4("4. initcap(name)"),
                       div(style="background:#222;padding:6px;border-radius:6px;",
                           HTML("<b>Alice</b> → <span style='color:#4da6ff;'>Alice</span><br>
              <b>bob</b> → <span style='color:#4da6ff;'>Bob</span><br>
              <b>CHARLIE</b> → <span style='color:#4da6ff;'>Charlie</span>")
                       )
                   )
                 ),
                 
                 ###############################################
                 # RIGHT COLUMN
                 ###############################################
                 column(
                   width = 6,
                   style = "font-size:12px;",
                   
                   div(style="margin-bottom:12px;",
                       h4("5. trim(comment)"),
                       div(style="background:#222;padding:6px;border-radius:6px;",
                           HTML("<b>␣␣␣Great product!␣␣</b> → <span style='color:#4da6ff;'>Great product!</span><br>
              <b>Needs improvement...</b> → <span style='color:#4da6ff;'>Needs improvement...</span><br>
              <b>␣␣AMAZING support team!␣␣␣</b> → <span style='color:#4da6ff;'>AMAZING support team!</span>")
                       )
                   ),
                   
                   div(style="margin-bottom:12px;",
                       h4("6. length(clean)"),
                       div(style="background:#222;padding:6px;border-radius:6px;",
                           HTML("<b>Great product!</b> → <span style='color:#4da6ff;'>14</span><br>
              <b>Needs improvement...</b> → <span style='color:#4da6ff;'>18</span><br>
              <b>AMAZING support team!</b> → <span style='color:#4da6ff;'>21</span>")
                       )
                   ),
                   
                   div(style="margin-bottom:12px;",
                       h4("7. domain(email)"),
                       div(style="background:#222;padding:6px;border-radius:6px;",
                           HTML("<b>alice@example.com</b> → <span style='color:#4da6ff;'>example.com</span><br>
              <b>bob_smith@example.com</b> → <span style='color:#4da6ff;'>example.com</span><br>
              <b>charlie99@domain.org</b> → <span style='color:#4da6ff;'>domain.org</span>")
                       )
                   ),
                   
                   div(style="margin-bottom:12px;",
                       h4("8. masked email"),
                       div(style="background:#222;padding:6px;border-radius:6px;",
                           HTML("<b>Alice</b> → <span style='color:#4da6ff;'>a***e@example.com</span><br>
              <b>bob</b> → <span style='color:#4da6ff;'>b******h@example.com</span><br>
              <b>CHARLIE</b> → <span style='color:#4da6ff;'>c******e@domain.org</span>")
                       )
                   )
                 )
               )
               
               
               
               ,
               
               tags$hr(),
               
               ###############################################
               # PYSPARK CODE
               ###############################################
               h3("PySpark Code (DataFrame API)"),
               aceEditor(
                 "code_string_ops",
                 value = paste(
                   "from pyspark.sql import functions as F",
                   "",
                   "df = df \\",
                   "  .withColumn('trim_name', F.trim('name')) \\",
                   "  .withColumn('lower_email', F.lower('email')) \\",
                   "  .withColumn('upper_name', F.upper('name')) \\",
                   "  .withColumn('initcap_name', F.initcap('name')) \\",
                   "  .withColumn('clean_comment', F.trim('comment')) \\",
                   "  .withColumn('comment_length', F.length(F.trim('comment'))) \\",
                   "  .withColumn('domain', F.split(F.lower('email'), '@')[1]) \\",
                   "  .withColumn('masked_email',",
                   "      F.concat(",
                   "          F.substring(F.trim('name'), 1, 1),",
                   "          F.lit('***'),",
                   "          F.substring(F.trim('name'), -1, 1),",
                   "          F.lit('@'),",
                   "          F.split(F.lower('email'), '@')[1]",
                   "      )",
                   "  )",
                   sep = "\n"
                 ),
                 mode = "python",
                 theme = "monokai",
                 height = "550px"
               ),
               
               tags$hr(),
               
               ###############################################
               # SQL
               ###############################################
               h3("SQL String Functions"),
               aceEditor(
                 "sql_string_ops",
                 value = paste(
                   "SELECT",
                   "  TRIM(name) AS trim_name,",
                   "  LOWER(email) AS lower_email,",
                   "  UPPER(name) AS upper_name,",
                   "  INITCAP(name) AS initcap_name,",
                   "  TRIM(comment) AS clean_comment,",
                   "  LENGTH(TRIM(comment)) AS comment_length,",
                   "  SPLIT(LOWER(email), '@')[1] AS domain,",
                   "  CONCAT(",
                   "      SUBSTRING(TRIM(name), 1, 1),",
                   "      '***',",
                   "      SUBSTRING(TRIM(name), LENGTH(TRIM(name)), 1),",
                   "      '@',",
                   "      SPLIT(LOWER(email), '@')[1]",
                   "  ) AS masked_email",
                   "FROM customers;",
                   sep = "\n"
                 ),
                 mode = "sql",
                 theme = "monokai",
                 height = "450px"
               ),
               
               tags$hr(),
               
               ###############################################
               # spark.sql()
               ###############################################
               h3("Spark SQL Example (spark.sql)"),
               aceEditor(
                 "spark_sql_string_ops",
                 value = paste(
                   'spark.sql("""',
                   "SELECT *,",
                   "  TRIM(name) AS trim_name,",
                   "  LOWER(email) AS lower_email,",
                   "  UPPER(name) AS upper_name,",
                   "  INITCAP(name) AS initcap_name,",
                   "  TRIM(comment) AS clean_comment,",
                   "  LENGTH(TRIM(comment)) AS comment_length,",
                   "  SPLIT(LOWER(email), '@')[1] AS domain,",
                   "  CONCAT(",
                   "      SUBSTRING(TRIM(name), 1, 1),",
                   "      '***',",
                   "      SUBSTRING(TRIM(name), LENGTH(TRIM(name)), 1),",
                   "      '@',",
                   "      SPLIT(LOWER(email), '@')[1]",
                   "  ) AS masked_email",
                   "FROM customers",
                   '""")',
                   sep = "\n"
                 ),
                 mode = "python",
                 theme = "monokai",
                 height = "450px"
               )
               
               ,
               tags$hr(),
               
               ###############################################
               # Pandas
               ###############################################
               h3("Pandas Example"),
               aceEditor(
                 "pandas_string_ops",
                 value = paste(
                   "import pandas as pd",
                   "",
                   "# Trim name",
                   "df['trim_name'] = df['name'].str.strip()",
                   "",
                   "# Lowercase email",
                   "df['lower_email'] = df['email'].str.lower()",
                   "",
                   "# Uppercase name",
                   "df['upper_name'] = df['name'].str.upper()",
                   "",
                   "# Initcap (title case)",
                   "df['initcap_name'] = df['name'].str.title()",
                   "",
                   "# Clean comment",
                   "df['clean_comment'] = df['comment'].str.strip()",
                   "",
                   "# Length after cleaning",
                   "df['comment_length'] = df['clean_comment'].str.len()",
                   "",
                   "# Extract domain",
                   "df['domain'] = df['lower_email'].str.split('@').str[1]",
                   "",
                   "# Masked email",
                   "df['masked_email'] = (",
                   "    df['trim_name'].str[0] +",
                   "    '***' +",
                   "    df['trim_name'].str[-1] +",
                   "    '@' +",
                   "    df['domain']",
                   ")",
                   sep = "\n"
                 ),
                 mode = "python",
                 theme = "monokai",
                 height = "450px"
               ),
               
               tags$hr(),
               
               ###############################################
               # Polars
               ###############################################
               h3("Polars Example"),
               aceEditor(
                 "polars_string_ops",
                 value = paste(
                   "import polars as pl",
                   "",
                   "df = df.with_columns([",
                   "    # Trim name",
                   "    pl.col('name').str.strip().alias('trim_name'),",
                   "",
                   "    # Lowercase email",
                   "    pl.col('email').str.to_lowercase().alias('lower_email'),",
                   "",
                   "    # Uppercase name",
                   "    pl.col('name').str.to_uppercase().alias('upper_name'),",
                   "",
                   "    # Initcap",
                   "    pl.col('name').str.to_titlecase().alias('initcap_name'),",
                   "",
                   "    # Clean comment",
                   "    pl.col('comment').str.strip().alias('clean_comment'),",
                   "",
                   "    # Length after cleaning",
                   "    pl.col('comment').str.strip().str.len().alias('comment_length'),",
                   "",
                   "    # Extract domain",
                   "    pl.col('email').str.to_lowercase().str.split('@').list.get(1).alias('domain'),",
                   "",
                   "    # Masked email",
                   "    (",
                   "        pl.col('name').str.strip().str.slice(0, 1) +",
                   "        '***' +",
                   "        pl.col('name').str.strip().str.slice(-1, 1) +",
                   "        '@' +",
                   "        pl.col('email').str.to_lowercase().str.split('@').list.get(1)",
                   "    ).alias('masked_email')",
                   "])",
                   sep = "\n"
                 ),
                 mode = "python",
                 theme = "monokai",
                 height = "450px"
               )
             )
           }
           
           
           
           ,
           
           
           
           "Numeric Operations" = {
             tagList(
               
               h2("🔢 Numeric Operations in PySpark"),
               
               p("These are the most common numeric transformations used in PySpark. Each card shows a simple before → after example so beginners can quickly understand what each function does."),
               
               tags$hr(),
               
               fluidRow(
                 
                 ###############################################
                 # LEFT COLUMN
                 ###############################################
                 column(
                   width = 6,
                   style = "font-size:12px;",
                   
                   # ABS
                   div(style="margin-bottom:12px;",
                       h4("1. abs(x) — Absolute value"),
                       div(style="background:#222;padding:6px;border-radius:6px;",
                           HTML("<b>-12</b> → <span style='color:#4da6ff;'>12</span><br>
                  <b>7</b> → <span style='color:#4da6ff;'>7</span>")
                       )
                   ),
                   
                   # ROUND
                   div(style="margin-bottom:12px;",
                       h4("2. round(x, n) — Round to n decimals"),
                       div(style="background:#222;padding:6px;border-radius:6px;",
                           HTML("<b>12.3456</b> → <span style='color:#4da6ff;'>12.35</span><br>
                  <b>9.876</b> → <span style='color:#4da6ff;'>9.88</span>")
                       )
                   ),
                   
                   # FLOOR
                   div(style="margin-bottom:12px;",
                       h4("3. floor(x) — Round down"),
                       div(style="background:#222;padding:6px;border-radius:6px;",
                           HTML("<b>7.9</b> → <span style='color:#4da6ff;'>7</span><br>
                  <b>3.1</b> → <span style='color:#4da6ff;'>3</span>")
                       )
                   ),
                   
                   # CEIL
                   div(style="margin-bottom:12px;",
                       h4("4. ceil(x) — Round up"),
                       div(style="background:#222;padding:6px;border-radius:6px;",
                           HTML("<b>7.1</b> → <span style='color:#4da6ff;'>8</span><br>
                  <b>3.9</b> → <span style='color:#4da6ff;'>4</span>")
                       )
                   )
                 ),
                 
                 ###############################################
                 # RIGHT COLUMN
                 ###############################################
                 column(
                   width = 6,
                   style = "font-size:12px;",
                   
                   # POW
                   div(style="margin-bottom:12px;",
                       h4("5. pow(x, y) — x to the power of y"),
                       div(style="background:#222;padding:6px;border-radius:6px;",
                           HTML("<b>2^3</b> → <span style='color:#4da6ff;'>8</span><br>
                  <b>5^2</b> → <span style='color:#4da6ff;'>25</span>")
                       )
                   ),
                   
                   # SQRT
                   div(style="margin-bottom:12px;",
                       h4("6. sqrt(x) — Square root"),
                       div(style="background:#222;padding:6px;border-radius:6px;",
                           HTML("<b>25</b> → <span style='color:#4da6ff;'>5</span><br>
                  <b>9</b> → <span style='color:#4da6ff;'>3</span>")
                       )
                   ),
                   
                   # LOG
                   div(style="margin-bottom:12px;",
                       h4("7. log(x) — Natural log"),
                       div(style="background:#222;padding:6px;border-radius:6px;",
                           HTML("<b>1</b> → <span style='color:#4da6ff;'>0</span><br>
                  <b>e</b> → <span style='color:#4da6ff;'>1</span>")
                       )
                   ),
                   
                   # COALESCE
                   div(style="margin-bottom:12px;",
                       h4("8. coalesce(x, y) — First non-null"),
                       div(style="background:#222;padding:6px;border-radius:6px;",
                           HTML("<b>null, 5</b> → <span style='color:#4da6ff;'>5</span><br>
                  <b>null, null, 9</b> → <span style='color:#4da6ff;'>9</span>")
                       )
                   )
                 )
               ),
               
               tags$hr(),
               
               ###############################################
               # PYSPARK CODE
               ###############################################
               h3("PySpark Code (DataFrame API)"),
               aceEditor(
                 "code_numeric_ops",
                 value = paste(
                   "from pyspark.sql import functions as F",
                   "",
                   "df = df \\",
                   "  .withColumn('abs_val', F.abs('x')) \\",
                   "  .withColumn('rounded', F.round('x', 2)) \\",
                   "  .withColumn('floor_val', F.floor('x')) \\",
                   "  .withColumn('ceil_val', F.ceil('x')) \\",
                   "  .withColumn('power_val', F.pow('x', 2)) \\",
                   "  .withColumn('sqrt_val', F.sqrt('x')) \\",
                   "  .withColumn('log_val', F.log('x')) \\",
                   "  .withColumn('first_non_null', F.coalesce('x', 'y'))",
                   sep = "\n"
                 ),
                 mode = "python",
                 theme = "monokai",
                 height = "350px"
               ),
               
               tags$hr(),
               
               ###############################################
               # SQL
               ###############################################
               h3("SQL Numeric Functions"),
               aceEditor(
                 "sql_numeric_ops",
                 value = paste(
                   "SELECT",
                   "  ABS(x) AS abs_val,",
                   "  ROUND(x, 2) AS rounded,",
                   "  FLOOR(x) AS floor_val,",
                   "  CEIL(x) AS ceil_val,",
                   "  POWER(x, 2) AS power_val,",
                   "  SQRT(x) AS sqrt_val,",
                   "  LOG(x) AS log_val,",
                   "  COALESCE(x, y) AS first_non_null",
                   "FROM numbers;",
                   sep = "\n"
                 ),
                 mode = "sql",
                 theme = "monokai",
                 height = "350px"
               ),
               h3("Spark SQL Example (spark.sql)"),
               aceEditor(
                 "spark_sql_numeric_ops",
                 value = paste(
                   'spark.sql("""',
                   "SELECT",
                   "  ABS(x) AS abs_val,",
                   "  ROUND(x, 2) AS rounded,",
                   "  FLOOR(x) AS floor_val,",
                   "  CEIL(x) AS ceil_val,",
                   "  POWER(x, 2) AS power_val,",
                   "  SQRT(x) AS sqrt_val,",
                   "  LOG(x) AS log_val,",
                   "  COALESCE(x, y) AS first_non_null",
                   "FROM numbers",
                   '""")',
                   sep = "\n"
                 ),
                 mode = "python",
                 theme = "monokai",
                 height = "350px"
               )
               ,
               h3("Pandas Example"),
               aceEditor(
                 "pandas_numeric_ops",
                 value = paste(
                   "import pandas as pd",
                   "import numpy as np",
                   "",
                   "# Absolute value",
                   "df['abs_val'] = df['x'].abs()",
                   "",
                   "# Round to 2 decimals",
                   "df['rounded'] = df['x'].round(2)",
                   "",
                   "# Floor",
                   "df['floor_val'] = np.floor(df['x'])",
                   "",
                   "# Ceil",
                   "df['ceil_val'] = np.ceil(df['x'])",
                   "",
                   "# Power",
                   "df['power_val'] = df['x'] ** 2",
                   "",
                   "# Square root",
                   "df['sqrt_val'] = np.sqrt(df['x'])",
                   "",
                   "# Natural log",
                   "df['log_val'] = np.log(df['x'])",
                   "",
                   "# First non-null",
                   "df['first_non_null'] = df['x'].fillna(df['y'])",
                   sep = "\n"
                 ),
                 mode = "python",
                 theme = "monokai",
                 height = "350px"
               )
               ,
               h3("Polars Example"),
               aceEditor(
                 "polars_numeric_ops",
                 value = paste(
                   "import polars as pl",
                   "",
                   "df = df.with_columns([",
                   "    pl.col('x').abs().alias('abs_val'),",
                   "    pl.col('x').round(2).alias('rounded'),",
                   "    pl.col('x').floor().alias('floor_val'),",
                   "    pl.col('x').ceil().alias('ceil_val'),",
                   "    (pl.col('x') ** 2).alias('power_val'),",
                   "    pl.col('x').sqrt().alias('sqrt_val'),",
                   "    pl.col('x').log().alias('log_val'),",
                   "    pl.coalesce([pl.col('x'), pl.col('y')]).alias('first_non_null')",
                   "])",
                   sep = "\n"
                 ),
                 mode = "python",
                 theme = "monokai",
                 height = "380px"
               )
               
             )
           }
           ,
           
           
           
           
           
           "PySpark Functions" = {
             tagList(
               
               h2("🧠 Essential PySpark Functions"),
               
               p("These are the core functions used in almost every PySpark pipeline. Each card shows a tiny before → after example so beginners can clearly see what the function does, followed by equivalent code in PySpark, Spark SQL, SQL, Pandas, and Polars."),
               
               tags$hr(),
               
               fluidRow(
                 
                 ###############################################
                 # LEFT COLUMN
                 ###############################################
                 column(
                   width = 6,
                   style = "font-size:12px;",
                   
                   #################################################
                   # 1. col()
                   #################################################
                   h4("1. col('x') — Reference a column"),
                   div(style="background:#222;padding:6px;border-radius:6px;",
                       HTML("
            <b>Before</b><br>
            x<br>
            10<br><br>

            <b>After</b><br>
            col('x') simply refers to the column — no change.
          ")
                   ),
                   
                   #################################################
                   # 2. lit()
                   #################################################
                   h4("2. lit(value) — Create a constant column"),
                   div(style="background:#222;padding:6px;border-radius:6px;",
                       HTML("
            <b>Before</b><br>
            x<br>
            10<br><br>

            <b>After</b><br>
            x | constant<br>
            10 | <span style='color:#4da6ff;'>5</span>
          ")
                   ),
                   
                   #################################################
                   # 3. when() / otherwise()
                   #################################################
                   h4("3. when() / otherwise() — IF logic"),
                   div(style="background:#222;padding:6px;border-radius:6px;",
                       HTML("
            <b>Before</b><br>
            x<br>
            5<br>
            20<br><br>

            <b>After</b><br>
            x | size_label<br>
            5 | <span style='color:#4da6ff;'>small</span><br>
            20 | <span style='color:#4da6ff;'>large</span>
          ")
                   ),
                   
                   #################################################
                   # 4. expr()
                   #################################################
                   h4("4. expr() — SQL inside PySpark"),
                   div(style="background:#222;padding:6px;border-radius:6px;",
                       HTML("
            <b>Before</b><br>
            x<br>
            10<br><br>

            <b>After (expr('x + 5'))</b><br>
            x | x_plus_5<br>
            10 | <span style='color:#4da6ff;'>15</span>
          ")
                   ),
                   
                   #################################################
                   # 5. withColumn()
                   #################################################
                   h4("5. withColumn() — Add or replace a column"),
                   div(style="background:#222;padding:6px;border-radius:6px;",
                       HTML("
            <b>Before</b><br>
            x<br>
            10<br><br>

            <b>After</b><br>
            x | x_plus_5<br>
            10 | <span style='color:#4da6ff;'>15</span>
          ")
                   )
                 ),
                 
                 ###############################################
                 # RIGHT COLUMN
                 ###############################################
                 column(
                   width = 6,
                   style = "font-size:12px;",
                   
                   #################################################
                   # 6. select()
                   #################################################
                   h4("6. select() — Choose columns"),
                   div(style="background:#222;padding:6px;border-radius:6px;",
                       HTML("
            <b>Before</b><br>
            x | y | z<br>
            1 | 2 | 3<br><br>

            <b>After (select x,y)</b><br>
            x | y<br>
            1 | 2
          ")
                   ),
                   
                   #################################################
                   # 7. filter()
                   #################################################
                   h4("7. filter() — Keep only matching rows"),
                   div(style="background:#222;padding:6px;border-radius:6px;",
                       HTML("
            <b>Before</b><br>
            x<br>
            5<br>
            12<br><br>

            <b>After (x > 10)</b><br>
            x<br>
            <span style='color:#4da6ff;'>12</span>
          ")
                   ),
                   
                   #################################################
                   # 8. groupBy().agg()
                   #################################################
                   h4("8. groupBy().agg() — Aggregations"),
                   div(style="background:#222;padding:6px;border-radius:6px;",
                       HTML("
            <b>Before</b><br>
            category | x<br>
            A | 5<br>
            A | 7<br><br>

            <b>After (sum)</b><br>
            category | total_x<br>
            A | <span style='color:#4da6ff;'>12</span>
          ")
                   ),
                   
                   #################################################
                   # 9. explode()
                   #################################################
                   h4("9. explode() — Turn array elements into rows"),
                   div(style="background:#222;padding:6px;border-radius:6px;",
                       HTML("
            <b>Before</b><br>
            items<br>
            [1,2,3]<br><br>

            <b>After (explode)</b><br>
            items<br>
            <span style='color:#4da6ff;'>1</span><br>
            <span style='color:#4da6ff;'>2</span><br>
            <span style='color:#4da6ff;'>3</span>
          ")
                   ),
                   
                   #################################################
                   # 10. array() / struct()
                   #################################################
                   h4("10. array() / struct() — Build complex types"),
                   div(style="background:#222;padding:6px;border-radius:6px;",
                       HTML("
            <b>Before</b><br>
            x | y<br>
            1 | 2<br><br>

            <b>After</b><br>
            array → <span style='color:#4da6ff;'>[1,2]</span><br>
            struct → <span style='color:#4da6ff;'>{x:1, y:2}</span>
          ")
                   )
                 )
               ),
               
               tags$hr(),
               
               ###############################################
               # PYSPARK CODE
               ###############################################
               h3("PySpark Code (DataFrame API)"),
               aceEditor(
                 "code_pyspark_funcs",
                 value = paste(
                   "from pyspark.sql import functions as F",
                   "",
                   "df = df \\",
                   "  .withColumn('x_plus_5', F.col('x') + F.lit(5)) \\",
                   "  .withColumn('size_label', F.when(F.col('x') > 10, 'large').otherwise('small')) \\",
                   "  .select('x', 'x_plus_5', 'size_label') \\",
                   "  .filter(F.col('x') > 0) \\",
                   "  .groupBy('category').agg(F.sum('x').alias('total_x')) \\",
                   "  .withColumn('exploded', F.explode('items')) \\",
                   "  .withColumn('as_array', F.array('x', 'y')) \\",
                   "  .withColumn('as_struct', F.struct('x', 'y'))",
                   sep = "\n"
                 ),
                 mode = "python",
                 theme = "monokai",
                 height = "350px"
               ),
               
               tags$hr(),
               
               ###############################################
               # SPARK SQL
               ###############################################
               h3("Spark SQL Example"),
               aceEditor(
                 "spark_sql_funcs",
                 value = paste(
                   'spark.sql("""',
                   "SELECT",
                   "  x + 5 AS x_plus_5,",
                   "  CASE WHEN x > 10 THEN 'large' ELSE 'small' END AS size_label,",
                   "  ARRAY(x, y) AS as_array,",
                   "  STRUCT(x, y) AS as_struct",
                   "FROM table",
                   '""")',
                   sep = "\n"
                 ),
                 mode = "python",
                 theme = "monokai",
                 height = "350px"
               ),
               
               tags$hr(),
               
               ###############################################
               # PLAIN SQL
               ###############################################
               h3("Plain SQL Example"),
               aceEditor(
                 "plain_sql_funcs",
                 value = paste(
                   "-- SQL equivalents where possible",
                   "SELECT",
                   "  x + 5 AS x_plus_5,",
                   "  CASE WHEN x > 10 THEN 'large' ELSE 'small' END AS size_label,",
                   "  -- explode() has NO SQL equivalent",
                   "  -- array() and struct() depend on the SQL dialect",
                   "FROM table;",
                   sep = "\n"
                 ),
                 mode = "sql",
                 theme = "monokai",
                 height = "350px"
               ),
               
               tags$hr(),
               
               ###############################################
               # PANDAS
               ###############################################
               h3("Pandas Example"),
               aceEditor(
                 "pandas_funcs",
                 value = paste(
                   "import pandas as pd",
                   "",
                   "df['x_plus_5'] = df['x'] + 5",
                   "df['size_label'] = df['x'].apply(lambda v: 'large' if v > 10 else 'small')",
                   "",
                   "# explode",
                   "df_exploded = df.explode('items')",
                   "",
                   "# array + struct equivalents",
                   "df['as_array'] = df[['x','y']].values.tolist()",
                   "df['as_struct'] = df.apply(lambda r: {'x': r['x'], 'y': r['y']}, axis=1)",
                   sep = "\n"
                 ),
                 mode = "python",
                 theme = "monokai",
                 height = "350px"
               ),
               
               tags$hr(),
               
               ###############################################
               # POLARS
               ###############################################
               h3("Polars Example"),
               aceEditor(
                 "polars_funcs",
                 value = paste(
                   "import polars as pl",
                   "",
                   "df = df.with_columns([",
                   "    (pl.col('x') + 5).alias('x_plus_5'),",
                   "    pl.when(pl.col('x') > 10).then('large').otherwise('small').alias('size_label'),",
                   "    pl.col('items').explode().alias('exploded'),",
                   "    pl.concat_list(['x','y']).alias('as_array'),",
                   "    pl.struct(['x','y']).alias('as_struct')",
                   "])",
                   sep = "\n"
                 ),
                 mode = "python",
                 theme = "monokai",
                 height = "380px"
               )
             )
           }
           
           ,
           
           
           
           # --------------------------------------------------
           # INTERMEDIATE: Catalyst Optimizer
           # --------------------------------------------------
           "Intermediate: Catalyst Optimizer" = {
             tagList(
               
               h2("🧠 Catalyst Optimizer"),
               
               p("Catalyst is Spark’s query optimizer. It transforms your logical plan into an optimized logical plan, 
       and finally into a physical plan. Catalyst applies rule‑based and cost‑based optimizations such as 
       predicate pushdown, constant folding, projection pruning, and join reordering."),
               
               h3("Catalyst Optimization Techniques"),
               DTOutput("catalyst_table"),
               
               tags$hr(),
               
               h3("How Catalyst Works"),
               HTML("
      <div style='border:1px solid #ccc;padding:15px;width:80%;'>
        Logical Plan ➜ <b>Optimized Logical Plan</b> ➜ Physical Plan<br><br>
        Catalyst performs:<br>
        • Constant folding<br>
        • Predicate pushdown<br>
        • Projection pruning<br>
        • Filter simplification<br>
        • Join reordering<br>
      </div>
    "),
               
               tags$hr(),
               
               h3("PySpark Example (Shows Catalyst in Action)"),
               aceEditor(
                 "code_catalyst",
                 value = paste(
                   "df = spark.read.parquet('people.parquet')",
                   "",
                   "# Catalyst will push down the filter and prune unused columns",
                   "df_filtered = df.filter('age > 30').select('name')",
                   "",
                   "# Show logical + optimized + physical plan",
                   "df_filtered.explain('formatted')",
                   sep = "\n"
                 ),
                 mode = "python",
                 theme = "monokai",
                 height = "200px"
               ),
               
               tags$hr(),
               
               h3("Sample Output of explain('formatted')"),
               HTML("
<pre style='background:#222;color:#eee;padding:12px;border:1px solid #444;font-size:13px;border-radius:6px;overflow-x:auto;'>
== Physical Plan ==
*(1) Project [name#12]
+- *(1) Filter (age#10 > 30)
   +- FileScan parquet [name#12,age#10]
      PushedFilters: [GreaterThan(age,30)]
      ReadSchema: struct<name:string,age:int>

== Optimized Logical Plan ==
Project [name#12]
+- Filter (age#10 > 30)
   +- Relation [name#12,age#10] parquet

== Logical Plan ==
Project [name#12]
+- Filter (age#10 > 30)
   +- Relation [name#12,age#10] parquet
</pre>
")
               ,
               
               tags$hr(),
               
               h3("SQL Equivalent (Pure SQL)"),
               aceEditor(
                 "sql_catalyst",
                 value = paste(
                   "EXPLAIN FORMATTED",
                   "SELECT name",
                   "FROM people",
                   "WHERE age > 30;",
                   sep = "\n"
                 ),
                 mode = "sql",
                 theme = "monokai",
                 height = "160px"
               ),
               
               tags$hr(),
               
               h3("Using spark.sql()"),
               aceEditor(
                 "sql_api_catalyst",
                 value = paste(
                   "spark.sql(\"\"\"",
                   "  SELECT name",
                   "  FROM people",
                   "  WHERE age > 30",
                   "\"\"\").explain('formatted')",
                   sep = "\n"
                 ),
                 mode = "python",
                 theme = "monokai",
                 height = "180px"
               )
             )
           }
           
           ,
           
           # --------------------------------------------------
           # INTERMEDIATE: Tungsten Engine
           # --------------------------------------------------
           "Intermediate: Tungsten Engine" = {
             tagList(
               
               h2("⚡ Tungsten Engine"),
               
               p("Tungsten is Spark’s physical execution engine. It focuses on CPU and memory efficiency through 
       off‑heap memory, binary row format, and whole‑stage code generation. Tungsten is always active — 
       you do not enable it manually."),
               
               h3("Tungsten Optimization Techniques"),
               DTOutput("tungsten_table"),
               
               tags$hr(),
               
               h3("How Tungsten Works"),
               HTML("
      <div style='border:1px solid #ccc;padding:15px;width:80%;'>
        Tungsten provides:<br>
        • Off‑heap memory management<br>
        • Cache‑aware execution<br>
        • Whole‑stage code generation (WSCG)<br>
        • Binary row format<br><br>

        <b>Why off‑heap memory matters:</b><br>
        Spark normally stores data inside the JVM heap, which is managed by Java’s Garbage Collector (GC). 
        Large datasets create many Java objects, causing frequent GC pauses and slowdowns.<br><br>

        Tungsten avoids this by storing data <b>outside</b> the JVM heap in a compact binary format. 
        This reduces GC pressure because the GC no longer needs to scan or clean up that memory. 
        The result is faster, more predictable performance.
      </div>
    "),
               
               tags$hr(),
               
               h3("PySpark Example (Shows Tungsten Physical Plan)"),
               aceEditor(
                 "code_tungsten",
                 value = paste(
                   "df = spark.read.parquet('people.parquet')",
                   "",
                   "# Tungsten executes the physical plan using whole-stage codegen",
                   "df_selected = df.select('name', 'age')",
                   "",
                   "# Show physical plan with Tungsten operators",
                   "df_selected.explain('formatted')",
                   sep = "\n"
                 ),
                 mode = "python",
                 theme = "monokai",
                 height = "200px"
               ),
               
               tags$hr(),
               
               h3("Sample Output of explain('formatted')"),
               HTML("
<pre style='background:#222;color:#eee;padding:12px;border:1px solid #444;font-size:13px;border-radius:6px;overflow-x:auto;'>
== Physical Plan ==
*(1) Project [name#12, age#10]
+- *(1) FileScan parquet [name#12,age#10]
      ReadSchema: struct<name:string,age:int>
      BatchScan: true
      WholeStageCodegen: true

== Optimized Logical Plan ==
Project [name#12, age#10]
+- Relation [name#12,age#10] parquet

== Logical Plan ==
Project [name#12, age#10]
+- Relation [name#12,age#10] parquet
</pre>
"),
               
               
               tags$hr(),
               
               h3("SQL Equivalent"),
               aceEditor(
                 "sql_tungsten",
                 value = paste(
                   "EXPLAIN FORMATTED",
                   "SELECT name, age",
                   "FROM people;",
                   sep = "\n"
                 ),
                 mode = "sql",
                 theme = "monokai",
                 height = "160px"
               ),
               
               tags$hr(),
               
               h3("Using spark.sql()"),
               aceEditor(
                 "sql_api_tungsten",
                 value = paste(
                   "spark.sql(\"\"\"",
                   "  SELECT name, age",
                   "  FROM people",
                   "\"\"\").explain('formatted')",
                   sep = "\n"
                 ),
                 mode = "python",
                 theme = "monokai",
                 height = "180px"
               )
             )
           }
           
           
           
           ,
           
           
           
           # --------------------------------------------------
           # INTERMEDIATE: Partitioning & Shuffling
           # --------------------------------------------------
           "Intermediate: Partitioning & Shuffling" = {
             tagList(
               
               h2("📦 Partitioning & Shuffling"),
               
               p("Partitioning determines how Spark distributes data across the cluster. 
       Shuffling occurs when Spark must move data between partitions — usually during joins, aggregations, 
       or repartitioning. Understanding how data moves is essential for performance."),
               
               h3("Partitioning Concepts"),
               DTOutput("partition_table"),
               
               tags$hr(),
               
               ###############################################
               # DARK MODE EXAMPLE DATASET
               ###############################################
               h3("Example Dataset"),
               HTML("
      <div style='background:#222;border:1px solid #444;border-radius:8px;
                  padding:15px;width:70%;margin:auto;'>
        <table style='border-collapse:collapse;width:100%;color:#eee;'>
          <tr style='background:#333;'>
            <th style='padding:6px;border:1px solid #555;'>id</th>
            <th style='padding:6px;border:1px solid #555;'>country</th>
            <th style='padding:6px;border:1px solid #555;'>value</th>
          </tr>
          <tr><td style='padding:6px;border:1px solid #555;'>1</td><td style='padding:6px;border:1px solid #555;'>US</td><td style='padding:6px;border:1px solid #555;'>10</td></tr>
          <tr><td style='padding:6px;border:1px solid #555;'>2</td><td style='padding:6px;border:1px solid #555;'>US</td><td style='padding:6px;border:1px solid #555;'>20</td></tr>
          <tr><td style='padding:6px;border:1px solid #555;'>3</td><td style='padding:6px;border:1px solid #555;'>UK</td><td style='padding:6px;border:1px solid #555;'>30</td></tr>
          <tr><td style='padding:6px;border:1px solid #555;'>4</td><td style='padding:6px;border:1px solid #555;'>UK</td><td style='padding:6px;border:1px solid #555;'>40</td></tr>
          <tr><td style='padding:6px;border:1px solid #555;'>5</td><td style='padding:6px;border:1px solid #555;'>IN</td><td style='padding:6px;border:1px solid #555;'>50</td></tr>
          <tr><td style='padding:6px;border:1px solid #555;'>6</td><td style='padding:6px;border:1px solid #555;'>IN</td><td style='padding:6px;border:1px solid #555;'>60</td></tr>
        </table>
      </div>
    "),
               
               tags$hr(),
               
               ###############################################
               # INITIAL PARTITIONING
               ###############################################
               h3("Initial Partitioning (2 partitions — default/random distribution)"),
               p("Spark does NOT group by key unless instructed. Default partitioning is based on file splits or round‑robin distribution."),
               
               HTML("
      <div style='background:#222;border:1px solid #444;border-radius:8px;
                  padding:15px;width:80%;margin:auto;'>
        <table style='border-collapse:collapse;width:100%;color:#eee;'>
          <tr style='background:#2d3b55;'>
            <th style='padding:6px;border:1px solid #555;'>Partition 0</th>
            <th style='padding:6px;border:1px solid #555;'>Partition 1</th>
          </tr>
          <tr>
            <td style='padding:8px;border:1px solid #555;'>
              1 US 10<br>
              3 UK 30<br>
              5 IN 50
            </td>
            <td style='padding:8px;border:1px solid #555;'>
              2 US 20<br>
              4 UK 40<br>
              6 IN 60
            </td>
          </tr>
        </table>
      </div>
    "),
               
               tags$hr(),
               
               ###############################################
               # REPARTITION
               ###############################################
               h3("Repartition: Increases Partitions (Shuffle Happens)"),
               p("Repartition redistributes data across the cluster. This always triggers a shuffle. Here we repartition by country."),
               
               HTML("
      <div style='background:#222;border:1px solid #444;border-radius:8px;
                  padding:15px;width:80%;margin:auto;'>
        <table style='border-collapse:collapse;width:100%;color:#eee;'>
          <tr style='background:#5a3d1e;'>
            <th style='padding:6px;border:1px solid #555;'>Partition 0 (US)</th>
            <th style='padding:6px;border:1px solid #555;'>Partition 1 (UK)</th>
            <th style='padding:6px;border:1px solid #555;'>Partition 2 (IN)</th>
          </tr>
          <tr>
            <td style='padding:8px;border:1px solid #555;'>1 US 10<br>2 US 20</td>
            <td style='padding:8px;border:1px solid #555;'>3 UK 30<br>4 UK 40</td>
            <td style='padding:8px;border:1px solid #555;'>5 IN 50<br>6 IN 60</td>
          </tr>
        </table>
      </div>
    "),
               
               tags$hr(),
               
               ###############################################
               # COALESCE
               ###############################################
               h3("Coalesce: Reduces Partitions (No Shuffle)"),
               p("Coalesce merges existing partitions without moving data. No shuffle occurs."),
               
               HTML("
      <div style='background:#222;border:1px solid #444;border-radius:8px;
                  padding:15px;width:80%;margin:auto;'>
        <table style='border-collapse:collapse;width:100%;color:#eee;'>
          <tr style='background:#1e4d1e;'>
            <th style='padding:6px;border:1px solid #555;'>Partition 0</th>
          </tr>
          <tr>
            <td style='padding:8px;border:1px solid #555;'>
              1 US 10<br>
              3 UK 30<br>
              5 IN 50<br>
              2 US 20<br>
              4 UK 40<br>
              6 IN 60
            </td>
          </tr>
        </table>
      </div>
    "),
               
               tags$hr(),
               
               ###############################################
               # SHUFFLE EXAMPLE
               ###############################################
               h3("Shuffle Example: groupBy('country')"),
               p("GroupBy requires all rows with the same key to be in the same partition, so Spark must shuffle data."),
               
               HTML("
      <div style='background:#222;border:1px solid #444;border-radius:8px;
                  padding:15px;width:80%;margin:auto;'>
        <table style='border-collapse:collapse;width:100%;color:#eee;'>
          <tr style='background:#552d47;'>
            <th style='padding:6px;border:1px solid #555;'>Partition 0 (US)</th>
            <th style='padding:6px;border:1px solid #555;'>Partition 1 (UK)</th>
            <th style='padding:6px;border:1px solid #555;'>Partition 2 (IN)</th>
          </tr>
          <tr>
            <td style='padding:8px;border:1px solid #555;'>US: 2 rows</td>
            <td style='padding:8px;border:1px solid #555;'>UK: 2 rows</td>
            <td style='padding:8px;border:1px solid #555;'>IN: 2 rows</td>
          </tr>
        </table>
      </div>
    "),
               
               tags$hr(),
               
               ###############################################
               # PYSPARK CODE
               ###############################################
               h3("PySpark Example"),
               aceEditor(
                 "code_partitioning",
                 value = paste(
                   "# Increase partitions (shuffle)",
                   "df2 = df.repartition(10, 'country')",
                   "",
                   "# Reduce partitions (no shuffle)",
                   "df3 = df.coalesce(2)",
                   "",
                   "# Shuffle example: groupBy",
                   "df.groupBy('country').count().explain('formatted')",
                   sep = "\n"
                 ),
                 mode = "python",
                 theme = "monokai",
                 height = "220px"
               ),
               
               tags$hr(),
               
               ###############################################
               # SQL
               ###############################################
               h3("SQL Equivalent"),
               aceEditor(
                 "sql_partitioning",
                 value = paste(
                   "EXPLAIN FORMATTED",
                   "SELECT country, COUNT(*)",
                   "FROM people",
                   "GROUP BY country;",
                   sep = "\n"
                 ),
                 mode = "sql",
                 theme = "monokai",
                 height = "160px"
               ),
               
               tags$hr(),
               
               ###############################################
               # spark.sql()
               ###############################################
               h3("Using spark.sql()"),
               aceEditor(
                 "sql_api_partitioning",
                 value = paste(
                   "spark.sql(\"\"\"",
                   "  SELECT country, COUNT(*)",
                   "  FROM people",
                   "  GROUP BY country",
                   "\"\"\").explain('formatted')",
                   sep = "\n"
                 ),
                 mode = "python",
                 theme = "monokai",
                 height = "180px"
               )
             )
           }
           
           
           
           
           
           
           ,
           
           # --------------------------------------------------
           # NEW: Repartitioning Strategy
           # --------------------------------------------------
           "Intermediate: Repartitioning Strategy" = {
             tagList(
               
               h2("📊 Repartitioning Strategy"),
               
               p("Repartitioning is a performance‑tuning tool. It helps fix skew, improve parallelism, 
       balance workloads, and prepare data for joins or aggregations. 
       This section focuses on *why* repartitioning matters and how to handle skew."),
               
               h3("Repartitioning Concepts"),
               DTOutput("repartition_table"),
               
               tags$hr(),
               
               ###############################################
               # WHY IMBALANCE HAPPENS (DARK MODE)
               ###############################################
               h3("⚖️ Why Partition Imbalance Happens"),
               
               HTML("
      <div style='background:#222;border:1px solid #444;border-radius:8px;
                  padding:20px;width:85%;margin:auto;color:#eee;'>

        <b>1. Uneven File Splits</b><br>
        Spark partitions data based on file blocks, not values. If one block contains more rows, 
        one partition becomes larger.<br><br>

        <b>2. Skewed Keys</b><br>
        If one key appears far more often (e.g., 'CN' appears 2 million times), 
        all those rows must go to the same partition during groupBy/join.<br><br>

        <b>3. Hash Partitioning Imbalance</b><br>
        Repartitioning by a column uses hashing. If the hash distribution is uneven, 
        some partitions get more rows.<br><br>

        <b>4. Shuffle Operations</b><br>
        groupBy, join, and distinct force Spark to group identical keys together. 
        If one key dominates, one partition becomes huge.
      </div>
    "),
               
               tags$hr(),
               
               ###############################################
               # IMBALANCE VISUAL (DARK MODE)
               ###############################################
               h3("🔥 Visual: Partition Imbalance Before and After Repartitioning"),
               
               HTML("
      <div style='background:#2a1f1f;border:1px solid #553333;border-radius:8px;
                  padding:20px;width:85%;margin:auto;color:#eee;'>

        <h4 style='margin-top:0;color:#ff9999;'>Before Repartitioning (Skewed Data)</h4>
        <pre style='font-size:14px;background:#1a1a1a;color:#eee;border:1px solid #444;padding:10px;border-radius:6px;'>
Partition 0: ██████████████████████████████████████████████████████████████████  2,000,000 rows
Partition 1: ████████ 150,000 rows
Partition 2: ████ 80,000 rows
Partition 3: ██ 40,000 rows
        </pre>
        <p style='font-style:italic;color:#ff7777;margin-top:8px;'>
          One massive partition slows down the entire job — Spark must wait for the slowest task.
        </p>

        <hr style='border:none;border-top:1px solid #553333;margin:20px 0;'>

        <h4 style='color:#99ff99;'>After Repartition(8) — Balanced Workload</h4>
        <pre style='font-size:14px;background:#1a1a1a;color:#eee;border:1px solid #444;padding:10px;border-radius:6px;'>
Partition 0: ████████ 450k
Partition 1: ███████ 420k
Partition 2: ████████ 480k
Partition 3: ███████ 460k
Partition 4: ████████ 470k
Partition 5: ███████ 440k
Partition 6: ████████ 430k
Partition 7: ████████ 450k
        </pre>
        <p style='font-style:italic;color:#99ff99;margin-top:8px;'>
          Balanced partitions = faster jobs and better parallelism.
        </p>

      </div>
    "),
               
               tags$hr(),
               
               ###############################################
               # REPARTITION FOR JOINS (DARK MODE)
               ###############################################
               h3("🔗 Repartitioning for Joins"),
               
               HTML("
      <div style='background:#1e2a33;border:1px solid #335566;border-radius:8px;
                  padding:20px;width:85%;margin:auto;color:#eee;'>

        <b>Without Repartitioning</b><br>
        • df1: random partitions<br>
        • df2: random partitions<br>
        → Spark shuffles both tables<br><br>

        <b>With Repartitioning</b><br>
        df1 = df1.repartition('id')<br>
        df2 = df2.repartition('id')<br>
        → Spark avoids double shuffle<br>
      </div>
    "),
               
               tags$hr(),
               
               ###############################################
               # SALTING (DARK MODE)
               ###############################################
               h3("🧂 What Is Salting? (Fixing Extreme Skew)"),
               
               p("When one key appears millions of times, repartitioning alone cannot fix skew because Spark must place 
       all identical keys in the same partition. Salting artificially splits a hot key into multiple subkeys."),
               
               HTML("
      <div style='background:#332b1e;border:1px solid #665544;border-radius:8px;
                  padding:20px;width:85%;margin:auto;color:#eee;'>

        <b>Before Salting</b><br>
        Key 'CN' → 2,000,000 rows in one partition<br><br>

        <b>After Salting</b><br>
        CN_0 → 500k rows<br>
        CN_1 → 500k rows<br>
        CN_2 → 500k rows<br>
        CN_3 → 500k rows<br><br>

        <i style='color:#ccb;'>Salting distributes a hot key across multiple partitions.</i>
      </div>
    "),
               
               tags$hr(),
               
               ###############################################
               # PYSPARK CODE
               ###############################################
               h3("PySpark Example: Repartitioning + Salting"),
               
               aceEditor(
                 "code_repartition",
                 value = paste(
                   "# Fix skew by increasing partitions",
                   "df2 = df.repartition(50)",
                   "",
                   "# Repartition by key for joins",
                   "df3 = df.repartition('country')",
                   "",
                   "# Apply salting to fix skew on 'country'",
                   "from pyspark.sql.functions import concat, col, lit, rand",
                   "",
                   "df_salted = df.withColumn(",
                   "    'country_salted',",
                   "    concat(col('country'), lit('_'), (rand()*4).cast('int'))",
                   ")",
                   "",
                   "# Repartition using salted key",
                   "df_balanced = df_salted.repartition('country_salted')",
                   sep = "\n"
                 ),
                 mode = "python",
                 theme = "monokai",
                 height = "260px"
               ),
               
               tags$hr(),
               
               ###############################################
               # SQL
               ###############################################
               h3("SQL Equivalent"),
               
               aceEditor(
                 "sql_repartition",
                 value = paste(
                   "-- SQL does not expose repartitioning directly",
                   "-- But GROUP BY, ORDER BY, and JOIN trigger shuffles",
                   "",
                   "SELECT country, COUNT(*)",
                   "FROM table",
                   "GROUP BY country;",
                   sep = "\n"
                 ),
                 mode = "sql",
                 theme = "monokai",
                 height = "160px"
               ),
               
               tags$hr(),
               
               ###############################################
               # spark.sql()
               ###############################################
               h3("Using spark.sql()"),
               
               aceEditor(
                 "sql_api_repartition",
                 value = paste(
                   "spark.sql(\"SELECT country, COUNT(*) FROM table GROUP BY country\")",
                   sep = "\n"
                 ),
                 mode = "python",
                 theme = "monokai",
                 height = "120px"
               ),
               
               tags$hr(),
               
               ###############################################
               # SUMMARY (DARK MODE)
               ###############################################
               h3("Summary"),
               
               HTML("
      <div style='border:1px solid #444;padding:15px;width:80%;border-radius:8px;
                  background:#222;margin:auto;color:#eee;'>
        <b>Why imbalance happens:</b><br>
        • Uneven file splits<br>
        • Skewed keys<br>
        • Hash imbalance<br>
        • Shuffle operations<br><br>

        <b>How to fix it:</b><br>
        • repartition(n) → increase parallelism<br>
        • repartition(key) → group keys for joins<br>
        • coalesce(n) → reduce partitions without shuffle<br>
        • salting → fix extreme skew<br>
      </div>
    ")
             )
           }
           
           
           
           
           ,
           
           
           
           
           # --------------------------------------------------
           # INTERMEDIATE: Wide vs Narrow Transformations
           # --------------------------------------------------
           "Intermediate: Wide vs Narrow Transformations" = {
             tagList(
               
               h2("🛣 Wide vs Narrow Transformations"),
               
               p("Narrow transformations operate on a single partition. Wide transformations require data to move 
       across partitions (shuffle). These examples show exactly how they differ."),
               
               h3("Transformation Types"),
               DTOutput("wide_narrow_table"),
               
               tags$hr(),
               
               ###############################################
               # EXAMPLE DATASET (DARK MODE)
               ###############################################
               h3("Example Dataset"),
               HTML("
      <div style='background:#222;border:1px solid #444;border-radius:8px;
                  padding:15px;width:70%;margin:auto;color:#eee;'>
        <table style='border-collapse:collapse;width:100%;color:#eee;'>
          <tr style='background:#333;'>
            <th style='padding:6px;border:1px solid #555;'>id</th>
            <th style='padding:6px;border:1px solid #555;'>country</th>
            <th style='padding:6px;border:1px solid #555;'>age</th>
          </tr>
          <tr><td style='padding:6px;border:1px solid #555;'>1</td><td style='padding:6px;border:1px solid #555;'>US</td><td style='padding:6px;border:1px solid #555;'>25</td></tr>
          <tr><td style='padding:6px;border:1px solid #555;'>2</td><td style='padding:6px;border:1px solid #555;'>US</td><td style='padding:6px;border:1px solid #555;'>40</td></tr>
          <tr><td style='padding:6px;border:1px solid #555;'>3</td><td style='padding:6px;border:1px solid #555;'>UK</td><td style='padding:6px;border:1px solid #555;'>31</td></tr>
          <tr><td style='padding:6px;border:1px solid #555;'>4</td><td style='padding:6px;border:1px solid #555;'>IN</td><td style='padding:6px;border:1px solid #555;'>29</td></tr>
        </table>
      </div>
    "),
               
               tags$hr(),
               
               ###############################################
               # NARROW TRANSFORMATIONS (DARK MODE)
               ###############################################
               h3("Narrow Transformations: No Shuffle"),
               
               p("Narrow transformations operate on a single partition. Spark does not need to move data."),
               
               HTML("
      <div style='background:#1e2a33;border:1px solid #335566;border-radius:8px;
                  padding:20px;width:85%;margin:auto;color:#eee;'>

        <b>Example: select(), filter()</b><br><br>

        <table style='border-collapse:collapse;width:100%;color:#eee;'>
          <tr style='background:#2d3b55;'>
            <th style='padding:6px;border:1px solid #555;'>Partition 0</th>
            <th style='padding:6px;border:1px solid #555;'>Partition 1</th>
          </tr>
          <tr>
            <td style='padding:8px;border:1px solid #555;'>
              1 US 25<br>
              3 UK 31
            </td>
            <td style='padding:8px;border:1px solid #555;'>
              2 US 40<br>
              4 IN 29
            </td>
          </tr>
        </table>

        <p style='font-style:italic;color:#7dffb0;margin-top:10px;'>
          After select() or filter(), the rows stay in the same partitions — no shuffle.
        </p>
      </div>
    "),
               
               tags$hr(),
               
               ###############################################
               # WIDE TRANSFORMATIONS (DARK MODE)
               ###############################################
               h3("Wide Transformations: Shuffle Required"),
               
               p("Wide transformations require data movement across partitions. Spark must group or reorganize data."),
               
               HTML("
      <div style='background:#332b1e;border:1px solid #665544;border-radius:8px;
                  padding:20px;width:85%;margin:auto;color:#eee;'>

        <b>Example: groupBy('country').count()</b><br><br>

        <p><b>Before Shuffle:</b></p>
        <table style='border-collapse:collapse;width:100%;color:#eee;'>
          <tr style='background:#5a3d1e;'>
            <th style='padding:6px;border:1px solid #555;'>Partition 0</th>
            <th style='padding:6px;border:1px solid #555;'>Partition 1</th>
          </tr>
          <tr>
            <td style='padding:8px;border:1px solid #555;'>
              1 US 25<br>
              3 UK 31
            </td>
            <td style='padding:8px;border:1px solid #555;'>
              2 US 40<br>
              4 IN 29
            </td>
          </tr>
        </table>

        <p><b>After Shuffle:</b></p>
        <table style='border-collapse:collapse;width:100%;color:#eee;'>
          <tr style='background:#8a5a2e;'>
            <th style='padding:6px;border:1px solid #555;'>Partition A (US)</th>
            <th style='padding:6px;border:1px solid #555;'>Partition B (UK)</th>
            <th style='padding:6px;border:1px solid #555;'>Partition C (IN)</th>
          </tr>
          <tr>
            <td style='padding:8px;border:1px solid #555;'>US: 2 rows</td>
            <td style='padding:8px;border:1px solid #555;'>UK: 1 row</td>
            <td style='padding:8px;border:1px solid #555;'>IN: 1 row</td>
          </tr>
        </table>

        <p style='font-style:italic;color:#ffcc88;margin-top:10px;'>
          Spark must move rows so all identical keys end up together — this is a shuffle.
        </p>
      </div>
    "),
               
               tags$hr(),
               
               ###############################################
               # PYSPARK CODE
               ###############################################
               h3("PySpark Example"),
               
               aceEditor(
                 "code_wide_narrow",
                 value = paste(
                   "# Narrow transformations (no shuffle)",
                   "df2 = df.select('name', 'age')",
                   "df3 = df.filter(df.age > 30)",
                   "",
                   "# Wide transformation (shuffle)",
                   "df4 = df.groupBy('country').count()",
                   sep = "\n"
                 ),
                 mode = "python",
                 theme = "monokai",
                 height = "180px"
               ),
               
               tags$hr(),
               
               ###############################################
               # SQL
               ###############################################
               h3("SQL Equivalent"),
               
               aceEditor(
                 "sql_wide_narrow",
                 value = paste(
                   "-- Narrow",
                   "SELECT name, age FROM table;",
                   "SELECT * FROM table WHERE age > 30;",
                   "",
                   "-- Wide (shuffle)",
                   "SELECT country, COUNT(*) FROM table GROUP BY country;",
                   sep = "\n"
                 ),
                 mode = "sql",
                 theme = "monokai",
                 height = "180px"
               ),
               
               tags$hr(),
               
               ###############################################
               # spark.sql()
               ###############################################
               h3("Using spark.sql()"),
               
               aceEditor(
                 "sql_api_wide_narrow",
                 value = paste(
                   "spark.sql(\"SELECT name, age FROM table\")",
                   "spark.sql(\"SELECT * FROM table WHERE age > 30\")",
                   "spark.sql(\"SELECT country, COUNT(*) FROM table GROUP BY country\")",
                   sep = "\n"
                 ),
                 mode = "python",
                 theme = "monokai",
                 height = "160px"
               )
             )
           }
           
           
           
           
           
           ,
           
           # --------------------------------------------------
           # INTERMEDIATE: Caching & Persistence
           # --------------------------------------------------
           "Intermediate: Caching & Persistence" = {
             tagList(
               h2("💾 Caching & Persistence"),
               
               h3("Cache Levels"),
               DTOutput("cache_table"),
               
               tags$hr(),
               
               p("Caching stores DataFrames in memory (and optionally disk) to speed up repeated computations."),
               
               h3("PySpark Example"),
               aceEditor(
                 "code_cache",
                 value = paste(
                   "# Cache DataFrame in memory",
                   "df.cache()",
                   "",
                   "# Persist with specific storage level",
                   "from pyspark import StorageLevel",
                   "df.persist(StorageLevel.MEMORY_AND_DISK)",
                   "",
                   "# Remove from cache",
                   "df.unpersist()",
                   sep = "\n"
                 ),
                 mode = "python",
                 theme = "monokai",
                 height = "200px"
               ),
               
               tags$hr(),
               
               h3("SQL Equivalent"),
               aceEditor(
                 "sql_cache",
                 value = paste(
                   "-- SQL caching (Databricks / Spark SQL)",
                   "CACHE TABLE table;",
                   "",
                   "-- Uncache",
                   "UNCACHE TABLE table;",
                   sep = "\n"
                 ),
                 mode = "sql",
                 theme = "monokai",
                 height = "160px"
               ),
               
               h3("Using spark.sql()"),
               aceEditor(
                 "sql_api_cache",
                 value = paste(
                   "spark.sql(\"CACHE TABLE table\")",
                   "spark.sql(\"UNCACHE TABLE table\")",
                   sep = "\n"
                 ),
                 mode = "python",
                 theme = "monokai",
                 height = "140px"
               )
             )
           }
           
           ,
           
           
           # --------------------------------------------------
           # INTERMEDIATE: Broadcast Joins
           # --------------------------------------------------
           "Intermediate: Broadcast Joins" = {
             tagList(
               
               h2("📡 Broadcast Joins"),
               
               p("Broadcast joins send a small table to every executor, avoiding shuffles of the large table. 
       This is one of the most powerful performance optimizations in Spark."),
               
               h3("Broadcast Join Concepts"),
               DTOutput("broadcast_table"),
               
               tags$hr(),
               
               ###############################################
               # EXAMPLE DATASET (DARK MODE)
               ###############################################
               h3("Example Data"),
               
               HTML("
      <div style='background:#222;border:1px solid #444;border-radius:8px;
                  padding:15px;width:75%;margin:auto;color:#eee;'>

        <b>Large Fact Table (fact_df)</b> — 10 million rows<br><br>

        <table style='border-collapse:collapse;width:100%;margin-bottom:20px;color:#eee;'>
          <tr style='background:#333;'>
            <th style='padding:6px;border:1px solid #555;'>id</th>
            <th style='padding:6px;border:1px solid #555;'>amount</th>
            <th style='padding:6px;border:1px solid #555;'>country</th>
          </tr>
          <tr><td style='padding:6px;border:1px solid #555;'>1</td><td style='padding:6px;border:1px solid #555;'>100</td><td style='padding:6px;border:1px solid #555;'>US</td></tr>
          <tr><td style='padding:6px;border:1px solid #555;'>2</td><td style='padding:6px;border:1px solid #555;'>250</td><td style='padding:6px;border:1px solid #555;'>UK</td></tr>
          <tr><td style='padding:6px;border:1px solid #555;'>3</td><td style='padding:6px;border:1px solid #555;'>300</td><td style='padding:6px;border:1px solid #555;'>IN</td></tr>
        </table>

        <b>Small Dimension Table (dim_df)</b> — 3 rows<br><br>

        <table style='border-collapse:collapse;width:100%;color:#eee;'>
          <tr style='background:#333;'>
            <th style='padding:6px;border:1px solid #555;'>id</th>
            <th style='padding:6px;border:1px solid #555;'>category</th>
          </tr>
          <tr><td style='padding:6px;border:1px solid #555;'>1</td><td style='padding:6px;border:1px solid #555;'>A</td></tr>
          <tr><td style='padding:6px;border:1px solid #555;'>2</td><td style='padding:6px;border:1px solid #555;'>B</td></tr>
          <tr><td style='padding:6px;border:1px solid #555;'>3</td><td style='padding:6px;border:1px solid #555;'>C</td></tr>
        </table>
      </div>
    "),
               
               tags$hr(),
               
               ###############################################
               # SHUFFLE JOIN (DARK MODE)
               ###############################################
               h3("❌ Shuffle Join (No Broadcast)"),
               
               p("Without broadcast, Spark must shuffle both tables so matching keys end up in the same partition."),
               
               HTML("
      <div style='background:#2a1f1f;border:1px solid #553333;border-radius:8px;
                  padding:20px;width:85%;margin:auto;color:#eee;'>

        <b>Before Join:</b><br><br>

        <table style='border-collapse:collapse;width:100%;margin-bottom:10px;color:#eee;'>
          <tr style='background:#5a2d2d;'>
            <th style='padding:6px;border:1px solid #555;'>fact_df Partition 0</th>
            <th style='padding:6px;border:1px solid #555;'>fact_df Partition 1</th>
          </tr>
          <tr>
            <td style='padding:8px;border:1px solid #555;'>id: 1, 3, 7, ...</td>
            <td style='padding:8px;border:1px solid #555;'>id: 2, 4, 8, ...</td>
          </tr>
        </table>

        <table style='border-collapse:collapse;width:100%;color:#eee;'>
          <tr style='background:#5a2d2d;'>
            <th style='padding:6px;border:1px solid #555;'>dim_df Partition 0</th>
            <th style='padding:6px;border:1px solid #555;'>dim_df Partition 1</th>
          </tr>
          <tr>
            <td style='padding:8px;border:1px solid #555;'>id: 1, 2</td>
            <td style='padding:8px;border:1px solid #555;'>id: 3</td>
          </tr>
        </table>

        <p style='margin-top:15px;'><b>After Shuffle:</b></p>

        <pre style='font-size:14px;background:#1a1a1a;color:#eee;border:1px solid #444;padding:10px;border-radius:6px;'>
fact_df → shuffled
dim_df → shuffled
Both tables must be reorganized by key.
        </pre>

        <p style='font-style:italic;color:#ff7777;margin-top:10px;'>
          Shuffle join = expensive, slow, and unnecessary when one table is tiny.
        </p>
      </div>
    "),
               
               tags$hr(),
               
               ###############################################
               # BROADCAST JOIN (DARK MODE)
               ###############################################
               h3("✅ Broadcast Join (Fast, No Shuffle)"),
               
               p("Spark sends the small table to every executor. The large table stays where it is — no shuffle."),
               
               HTML("
      <div style='background:#1e3321;border:1px solid #335533;border-radius:8px;
                  padding:20px;width:85%;margin:auto;color:#eee;'>

        <b>Broadcasting dim_df to all executors:</b><br><br>

        <pre style='font-size:14px;background:#1a1a1a;color:#eee;border:1px solid #444;padding:10px;border-radius:6px;'>
Executor 1 receives dim_df (3 rows)
Executor 2 receives dim_df (3 rows)
Executor 3 receives dim_df (3 rows)
...
        </pre>

        <p style='margin-top:15px;'><b>Join happens locally:</b></p>

        <table style='border-collapse:collapse;width:100%;color:#eee;'>
          <tr style='background:#2d5533;'>
            <th style='padding:6px;border:1px solid #555;'>fact_df Partition 0</th>
            <th style='padding:6px;border:1px solid #555;'>fact_df Partition 1</th>
          </tr>
          <tr>
            <td style='padding:8px;border:1px solid #555;'>
              id: 1 → join with dim_df<br>
              id: 3 → join with dim_df
            </td>
            <td style='padding:8px;border:1px solid #555;'>
              id: 2 → join with dim_df<br>
              id: 4 → join with dim_df
            </td>
          </tr>
        </table>

        <p style='font-style:italic;color:#99ff99;margin-top:10px;'>
          Broadcast join = no shuffle, fast, ideal for small dimension tables.
        </p>
      </div>
    "),
               
               tags$hr(),
               
               ###############################################
               # PYSPARK CODE
               ###############################################
               h3("PySpark Example"),
               
               aceEditor(
                 "code_broadcast",
                 value = paste(
                   "from pyspark.sql.functions import broadcast",
                   "",
                   "# Broadcast the small dimension table",
                   "df_joined = fact_df.join(broadcast(dim_df), on='id', how='inner')",
                   "",
                   "# Without broadcast (shuffle)",
                   "df_joined2 = fact_df.join(dim_df, on='id', how='inner')",
                   sep = "\n"
                 ),
                 mode = "python",
                 theme = "monokai",
                 height = "200px"
               ),
               
               tags$hr(),
               
               ###############################################
               # SQL
               ###############################################
               h3("SQL Equivalent"),
               
               aceEditor(
                 "sql_broadcast",
                 value = paste(
                   "SELECT /*+ BROADCAST(dim_df) */",
                   "  f.*, d.category",
                   "FROM fact_df f",
                   "JOIN dim_df d",
                   "ON f.id = d.id;",
                   sep = "\n"
                 ),
                 mode = "sql",
                 theme = "monokai",
                 height = "180px"
               ),
               
               tags$hr(),
               
               ###############################################
               # spark.sql()
               ###############################################
               h3("Using spark.sql()"),
               
               aceEditor(
                 "sql_api_broadcast",
                 value = paste(
                   "spark.sql(\"\"\"",
                   "SELECT /*+ BROADCAST(dim_df) */",
                   "  f.*, d.category",
                   "FROM fact_df f",
                   "JOIN dim_df d",
                   "ON f.id = d.id",
                   "\"\"\")",
                   sep = "\n"
                 ),
                 mode = "python",
                 theme = "monokai",
                 height = "180px"
               ),
               
               tags$hr(),
               
               ###############################################
               # SUMMARY (DARK MODE)
               ###############################################
               h3("Summary"),
               
               HTML("
      <div style='border:1px solid #444;padding:15px;width:80%;border-radius:8px;
                  background:#222;margin:auto;color:#eee;'>
        <b>Broadcast Join:</b> Small table sent to all executors → no shuffle<br>
        <b>Shuffle Join:</b> Both tables shuffled → expensive<br><br>

        <b>Rule of thumb:</b> Broadcast when small table < 500MB<br>
      </div>
    ")
             )
           }
           
           
           
           
           
           ,
           
           # --------------------------------------------------
           # DAG VISUALIZER
           # --------------------------------------------------
           "Spark DAG Visualizer" = {
             tagList(
               
               h2("🧩 Spark DAG Visualizer"),
               
               p("Spark builds a Directed Acyclic Graph (DAG) of transformations. 
       Narrow transformations are pipelined together, while wide transformations 
       create shuffle boundaries and new stages. Actions trigger execution of the DAG."),
               
               h3("DAG Concepts"),
               DTOutput("dag_table"),
               
               tags$hr(),
               
               ###############################################
               # EXAMPLE TRANSFORMATIONS (DARK MODE)
               ###############################################
               h3("Example Transformations"),
               
               HTML("
      <div style='background:#222;border:1px solid #444;border-radius:8px;
                  padding:15px;width:70%;margin:auto;color:#eee;'>
        <pre style='font-size:14px;margin:0;color:#eee;'>
df2 = df.filter(df.age > 30)          # narrow
df3 = df2.select('country')           # narrow
df4 = df3.groupBy('country').count()  # wide (shuffle)
df4.show()                            # action
        </pre>
      </div>
    "),
               
               tags$hr(),
               
               ###############################################
               # DAG VISUAL (DARK MODE)
               ###############################################
               h3("🔍 Visual DAG Representation"),
               
               HTML("
      <div style='background:#1e2a33;border:1px solid #335566;border-radius:8px;
                  padding:25px;width:90%;margin:auto;color:#eee;
                  font-family:monospace;font-size:14px; white-space: pre;'>

<b style='color:#9cf;'>Stage 0 (Narrow Transformations)</b>

+---------------------------+
|        DataFrame          |
+---------------------------+
              |
              v
+---------------------------+
|     filter(age > 30)      |
+---------------------------+
              |
              v
+---------------------------+
|     select(country)       |
+---------------------------+

============== SHUFFLE ==============

<b style='color:#ffc;'>Stage 1 (Wide Transformation)</b>

+---------------------------+
|   groupBy(country).count  |
+---------------------------+
              |
              v
+---------------------------+
|          ACTION           |
|           show()          |
+---------------------------+

      </div>
    "),
               
               tags$hr(),
               
               ###############################################
               # EXPLANATION (DARK MODE)
               ###############################################
               h3("How Spark Builds the DAG"),
               
               HTML("
      <div style='background:#332b1e;border:1px solid #665544;border-radius:8px;
                  padding:20px;width:85%;margin:auto;color:#eee;'>

        <b>Narrow transformations:</b> Stay in the same stage (no shuffle).<br><br>
        <b>Wide transformations:</b> Require data movement → new stage.<br><br>
        <b>Actions:</b> Trigger execution of all upstream stages.<br><br>
        <b>Result:</b> Spark builds a DAG of stages separated by shuffle boundaries.
      </div>
    "),
               
               tags$hr(),
               
               ###############################################
               # PYSPARK CODE
               ###############################################
               h3("PySpark Example"),
               
               aceEditor(
                 "code_dag",
                 value = paste(
                   "df2 = df.filter(df.age > 30)",
                   "df3 = df2.groupBy('country').count()",
                   "df3.explain()  # shows the DAG",
                   sep = "\n"
                 ),
                 mode = "python",
                 theme = "monokai",
                 height = "160px"
               ),
               
               tags$hr(),
               
               ###############################################
               # SQL EQUIVALENT
               ###############################################
               h3("SQL Equivalent"),
               
               aceEditor(
                 "sql_dag",
                 value = paste(
                   "SELECT country, COUNT(*)",
                   "FROM table",
                   "WHERE age > 30",
                   "GROUP BY country;",
                   "",
                   "-- Spark SQL also builds a DAG internally",
                   sep = "\n"
                 ),
                 mode = "sql",
                 theme = "monokai",
                 height = "180px"
               ),
               
               tags$hr(),
               
               ###############################################
               # spark.sql()
               ###############################################
               h3("Using spark.sql()"),
               
               aceEditor(
                 "sql_api_dag",
                 value = paste(
                   "spark.sql(\"\"\"",
                   "SELECT country, COUNT(*)",
                   "FROM table",
                   "WHERE age > 30",
                   "GROUP BY country",
                   "\"\"\").explain()",
                   sep = "\n"
                 ),
                 mode = "python",
                 theme = "monokai",
                 height = "180px"
               ),
               
               tags$hr(),
               
               ###############################################
               # SUMMARY (DARK MODE)
               ###############################################
               h3("Summary"),
               
               HTML("
      <div style='border:1px solid #444;padding:15px;width:80%;border-radius:8px;
                  background:#222;margin:auto;color:#eee;'>
        <b>Transformations:</b> Build DAG nodes<br>
        <b>Narrow transformations:</b> Stay in the same stage<br>
        <b>Wide transformations:</b> Create shuffle boundaries → new stage<br>
        <b>Actions:</b> Trigger execution of the DAG<br>
      </div>
    ")
             )
           }
           
           
           
           ,
           
           
           
           # --------------------------------------------------
           # QUIZ (FULLY FIXED)
           # --------------------------------------------------
           "Quiz" = {
             tagList(
               h2("🧠 PySpark Quiz"),
               
               radioButtons("q1", "1. What triggers Spark execution?",
                            choices = c("select()", "filter()", "count()", "withColumn()"),
                            selected = character(0)),
               textOutput("a1"),
               tags$hr(),
               
               radioButtons("q2", "2. Spark DataFrames are:",
                            choices = c("Mutable", "Immutable", "Stored in Python memory"),
                            selected = character(0)),
               textOutput("a2"),
               tags$hr(),
               
               radioButtons("q3", "3. Which operation causes a shuffle?",
                            choices = c("select()", "filter()", "groupBy()", "withColumn()"),
                            selected = character(0)),
               textOutput("a3"),
               tags$hr(),
               
               radioButtons("q4", "4. What does a broadcast join do?",
                            choices = c(
                              "Sends a small table to all executors",
                              "Shuffles both tables",
                              "Caches the DataFrame",
                              "Sorts the data"
                            ),
                            selected = character(0)),
               textOutput("a4"),
               tags$hr(),
               
               radioButtons("q5", "5. Catalyst Optimizer is responsible for:",
                            choices = c(
                              "Executing tasks on executors",
                              "Optimizing logical and physical plans",
                              "Managing cluster resources",
                              "Caching DataFrames"
                            ),
                            selected = character(0)),
               textOutput("a5"),
               tags$hr(),
               
               radioButtons("q6", "6. When is it a good idea to manually repartition?",
                            choices = c(
                              "To fix skewed keys before a join",
                              "To make Spark run faster automatically",
                              "To avoid Catalyst optimization",
                              "To reduce memory usage on the driver"
                            ),
                            selected = character(0)),
               textOutput("a6"),
               tags$hr(),
               
               radioButtons("q7", "7. Why are broadcast joins fast?",
                            choices = c(
                              "Because Spark sends the small table to every worker so the big table doesn't need to move",
                              "Because Spark compresses the big table",
                              "Because Spark skips the join entirely",
                              "Because Spark uses more CPU cores"
                            ),
                            selected = character(0)),
               textOutput("a7"),
               tags$hr(),
               
               radioButtons("q8", "8. What is a narrow transformation?",
                            choices = c(
                              "A transformation that requires a shuffle",
                              "Each output partition depends on a single input partition",
                              "A transformation that caches data",
                              "A transformation that triggers an action"
                            ),
                            selected = character(0)),
               textOutput("a8"),
               tags$hr(),
               
               radioButtons("q9", "9. Which of the following is a wide transformation?",
                            choices = c("select()", "filter()", "withColumn()", "groupBy()"),
                            selected = character(0)),
               textOutput("a9"),
               tags$hr(),
               
               radioButtons("q10", "10. What causes a new stage in a Spark DAG?",
                            choices = c(
                              "A narrow transformation",
                              "A shuffle boundary",
                              "A DataFrame action",
                              "A Python UDF"
                            ),
                            selected = character(0)),
               textOutput("a10"),
               tags$hr(),
               
               radioButtons("q11", "11. What is the main purpose of Tungsten?",
                            choices = c(
                              "Optimize SQL queries",
                              "Improve memory and CPU efficiency with binary format and code generation",
                              "Manage cluster scheduling",
                              "Store DataFrames on disk"
                            ),
                            selected = character(0)),
               textOutput("a11"),
               tags$hr(),
               
               radioButtons("q12", "12. Which join type benefits most from broadcasting?",
                            choices = c(
                              "Joining two large fact tables",
                              "Joining a large fact table with a small dimension table",
                              "Joining two skewed tables",
                              "Joining tables with no common keys"
                            ),
                            selected = character(0)),
               textOutput("a12"),
               tags$hr(),
               
               radioButtons("q13", "13. What is data skew?",
                            choices = c(
                              "When all partitions have equal data",
                              "When one key has far more rows than others",
                              "When Spark cannot find a join key",
                              "When caching fails"
                            ),
                            selected = character(0)),
               textOutput("a13"),
               tags$hr(),
               
               radioButtons("q14", "14. What does salting help with?",
                            choices = c(
                              "Improving Catalyst optimization",
                              "Fixing skew by spreading hot keys across partitions",
                              "Reducing memory usage",
                              "Avoiding broadcast joins"
                            ),
                            selected = character(0)),
               textOutput("a14"),
               tags$hr(),
               
               radioButtons("q15", "15. What does cache() do?",
                            choices = c(
                              "Stores DataFrame in memory for faster reuse",
                              "Writes DataFrame to disk",
                              "Triggers a shuffle",
                              "Broadcasts the DataFrame"
                            ),
                            selected = character(0)),
               textOutput("a15"),
               tags$hr(),
               
               radioButtons("q16", "16. Which action materializes a DataFrame?",
                            choices = c("select()", "filter()", "groupBy()", "count()"),
                            selected = character(0)),
               textOutput("a16"),
               tags$hr(),
               
               radioButtons("q17", "17. What is the output of explain('formatted')?",
                            choices = c(
                              "The physical and logical plan",
                              "The DataFrame schema",
                              "The number of partitions",
                              "The cached RDD lineage"
                            ),
                            selected = character(0)),
               textOutput("a17"),
               tags$hr(),
               
               radioButtons("q18", "18. When should you use coalesce() instead of repartition()?",
                            choices = c(
                              "When increasing partitions",
                              "When decreasing partitions without shuffle",
                              "When preparing for a join",
                              "When fixing skew"
                            ),
                            selected = character(0)),
               textOutput("a18"),
               tags$hr(),
               
               radioButtons("q19", "19. What happens when a Python UDF is used?",
                            choices = c(
                              "Optimized by Catalyst",
                              "Executed in the JVM",
                              "Falls back to slower row-by-row execution",
                              "Broadcasts the UDF"
                            ),
                            selected = character(0)),
               textOutput("a19"),
               tags$hr(),
               
               radioButtons("q20", "20. What is a shuffle?",
                            choices = c(
                              "A memory cleanup operation",
                              "Redistribution of data across partitions",
                              "A type of join",
                              "A caching mechanism"
                            ),
                            selected = character(0)),
               textOutput("a20"),
               tags$hr(),
               
               radioButtons("q21", "21. What happens when you call df.repartition(100)?",
                            choices = c(
                              "Spark reduces partitions without shuffle",
                              "Spark increases partitions and triggers a shuffle",
                              "Spark caches the DataFrame",
                              "Spark broadcasts the DataFrame"
                            ),
                            selected = character(0)),
               textOutput("a21"),
               tags$hr(),
               
               radioButtons("q22", "22. What is the main benefit of using spark.sql()?",
                            choices = c(
                              "It bypasses Catalyst",
                              "It uses the same optimizer but allows SQL syntax",
                              "It runs faster than DataFrame API",
                              "It avoids shuffles"
                            ),
                            selected = character(0)),
               textOutput("a22"),
               tags$hr(),
               
               radioButtons("q23", "23. What is a stage in Spark?",
                            choices = c(
                              "A single task",
                              "A group of tasks separated by shuffle boundaries",
                              "A cached DataFrame",
                              "A broadcast variable"
                            ),
                            selected = character(0)),
               textOutput("a23"),
               tags$hr(),
               
               radioButtons("q24", "24. What is a task?",
                            choices = c(
                              "A unit of work executed on a single partition",
                              "A full job",
                              "A shuffle file",
                              "A broadcast variable"
                            ),
                            selected = character(0)),
               textOutput("a24"),
               tags$hr(),
               
               radioButtons("q25", "25. Which of the following is an action?",
                            choices = c("withColumn()", "select()", "filter()", "collect()"),
                            selected = character(0)),
               textOutput("a25"),
               tags$hr(),
               
               
               radioButtons("q26", "26. What does persist(StorageLevel.MEMORY_AND_DISK) do?",
                            choices = c(
                              "Stores data only in memory",
                              "Stores data in memory and spills to disk if needed",
                              "Stores data only on disk",
                              "Broadcasts the DataFrame"
                            ),
                            selected = character(0)),
               textOutput("a26"),
               tags$hr(),
               
               radioButtons("q27", "27. What is the default number of shuffle partitions?",
                            choices = c("1", "50", "200", "1000"),
                            selected = character(0)),
               textOutput("a27"),
               tags$hr(),
               
               radioButtons("q28", "28. What does df.rdd do?",
                            choices = c(
                              "Converts DataFrame to RDD",
                              "Caches the DataFrame",
                              "Triggers a shuffle",
                              "Optimizes the DataFrame"
                            ),
                            selected = character(0)),
               textOutput("a28"),
               tags$hr(),
               
               radioButtons("q29", "29. What is a broadcast variable?",
                            choices = c(
                              "A variable sent to all executors",
                              "A cached DataFrame",
                              "A shuffle file",
                              "A Python UDF"
                            ),
                            selected = character(0)),
               textOutput("a29"),
               tags$hr(),
               
               radioButtons("q30", "30. What does df.write.mode('overwrite') do?",
                            choices = c(
                              "Appends data",
                              "Deletes existing data and writes new data",
                              "Caches data",
                              "Triggers a broadcast"
                            ),
                            selected = character(0)),
               textOutput("a30"),
               tags$hr(),
               
               radioButtons("q31", "31. What is the purpose of checkpointing?",
                            choices = c(
                              "Break lineage to avoid long dependency chains",
                              "Cache data in memory",
                              "Broadcast data",
                              "Trigger a shuffle"
                            ),
                            selected = character(0)),
               textOutput("a31"),
               tags$hr(),
               
               radioButtons("q32", "32. Which join type is most expensive?",
                            choices = c("Broadcast join", "Shuffle join", "Semi join", "Cross join"),
                            selected = character(0)),
               textOutput("a32"),
               tags$hr(),
               
               radioButtons("q33", "33. What does df.describe() return?",
                            choices = c(
                              "Schema",
                              "Summary statistics",
                              "Physical plan",
                              "Cached data"
                            ),
                            selected = character(0)),
               textOutput("a33"),
               tags$hr(),
               
               radioButtons("q34", "34. What is the purpose of df.explain()?",
                            choices = c(
                              "Show the DataFrame",
                              "Show the execution plan",
                              "Show the schema",
                              "Show cached data"
                            ),
                            selected = character(0)),
               textOutput("a34"),
               tags$hr(),
               
               radioButtons("q35", "35. What does df.limit(10) do?",
                            choices = c(
                              "Triggers a shuffle",
                              "Returns a new DataFrame with at most 10 rows",
                              "Caches the DataFrame",
                              "Broadcasts the DataFrame"
                            ),
                            selected = character(0)),
               textOutput("a35"),
               tags$hr(),
               
               radioButtons("q36", "36. What is the purpose of spark.conf.set('spark.sql.shuffle.partitions', n)?",
                            choices = c(
                              "Set number of executors",
                              "Set number of shuffle partitions",
                              "Set number of cores",
                              "Set broadcast threshold"
                            ),
                            selected = character(0)),
               textOutput("a36"),
               tags$hr(),
               
               radioButtons("q37", "37. What is the default broadcast threshold?",
                            choices = c("1MB", "10MB", "50MB", "100MB"),
                            selected = character(0)),
               textOutput("a37"),
               tags$hr(),
               
               radioButtons("q38", "38. What does df.dropDuplicates() do?",
                            choices = c(
                              "Removes duplicate rows",
                              "Triggers a broadcast",
                              "Caches the DataFrame",
                              "Sorts the DataFrame"
                            ),
                            selected = character(0)),
               textOutput("a38"),
               tags$hr(),
               
               radioButtons("q39", "39. What is a job in Spark?",
                            choices = c(
                              "A single task",
                              "A set of stages triggered by an action",
                              "A shuffle file",
                              "A broadcast variable"
                            ),
                            selected = character(0)),
               textOutput("a39"),
               tags$hr(),
               
               radioButtons("q40", "40. What does df.sort('col') do?",
                            choices = c(
                              "Triggers a shuffle",
                              "Caches the DataFrame",
                              "Broadcasts the DataFrame",
                              "Removes duplicates"
                            ),
                            selected = character(0)),
               textOutput("a40"),
               tags$hr(),
               
               radioButtons("q41", "41. What is the purpose of df.sample()?",
                            choices = c(
                              "Randomly sample rows",
                              "Broadcast the DataFrame",
                              "Trigger a shuffle",
                              "Cache the DataFrame"
                            ),
                            selected = character(0)),
               textOutput("a41"),
               tags$hr(),
               
               radioButtons("q42", "42. What does df.count() do internally?",
                            choices = c(
                              "Reads only the first partition",
                              "Scans all partitions",
                              "Caches the DataFrame",
                              "Triggers a broadcast"
                            ),
                            selected = character(0)),
               textOutput("a42"),
               tags$hr(),
               
               radioButtons("q43", "43. What is the purpose of df.repartitionByRange()?",
                            choices = c(
                              "Randomly distribute rows",
                              "Range‑partition rows by a column",
                              "Broadcast the DataFrame",
                              "Cache the DataFrame"
                            ),
                            selected = character(0)),
               textOutput("a43"),
               tags$hr(),
               
               radioButtons("q44", "44. What is a physical plan?",
                            choices = c(
                              "User‑written SQL",
                              "Optimized execution plan",
                              "Schema definition",
                              "Cached data"
                            ),
                            selected = character(0)),
               textOutput("a44"),
               tags$hr(),
               
               radioButtons("q45", "45. What does df.printSchema() show?",
                            choices = c(
                              "Execution plan",
                              "Column names and types",
                              "Cached data",
                              "Partition count"
                            ),
                            selected = character(0)),
               textOutput("a45"),
               tags$hr(),
               
               radioButtons("q46", "46. What is a logical plan?",
                            choices = c(
                              "The unoptimized representation of the query",
                              "The physical execution plan",
                              "The cached RDD lineage",
                              "The shuffle file layout"
                            ),
                            selected = character(0)),
               textOutput("a46"),
               tags$hr(),
               
               radioButtons("q47", "47. What does df.write.partitionBy('col') do?",
                            choices = c(
                              "Repartitions the DataFrame",
                              "Writes data into directory partitions based on column values",
                              "Caches the DataFrame",
                              "Broadcasts the DataFrame"
                            ),
                            selected = character(0)),
               textOutput("a47"),
               tags$hr(),
               
               radioButtons("q48", "48. What is the purpose of spark.catalog?",
                            choices = c(
                              "Manage tables and databases",
                              "Manage executors",
                              "Manage shuffle files",
                              "Manage broadcast variables"
                            ),
                            selected = character(0)),
               textOutput("a48"),
               tags$hr(),
               
               radioButtons("q49", "49. What does df.toPandas() do?",
                            choices = c(
                              "Converts DataFrame to Pandas on the driver",
                              "Caches the DataFrame",
                              "Triggers a broadcast",
                              "Writes DataFrame to disk"
                            ),
                            selected = character(0)),
               textOutput("a49"),
               tags$hr(),
               
               radioButtons("q50", "50. What is the biggest risk of using toPandas()?",
                            choices = c(
                              "It triggers a shuffle",
                              "It can crash the driver if the DataFrame is too large",
                              "It disables Catalyst",
                              "It forces a broadcast join"
                            ),
                            selected = character(0)),
               textOutput("a50"),
               tags$hr()
             )
           }
           
           
           
           
           
    )
  }
  
  )
  
  
  
  
  
  # ----------------------------------------------------------
  # DAG Visualizer 
  # ----------------------------------------------------------
  output$dag_html <- renderUI({
    ops <- input$dag_ops
    if (is.null(ops) || length(ops) == 0) ops <- "read"
    
    boxes <- paste0(
      lapply(seq_along(ops), function(i) {
        paste0(
          "<div style='padding:10px;margin:10px;border:1px solid #ccc;
           display:inline-block;background:#f0f0f0;'>", ops[i], "</div>"
        )
      }),
      collapse = "<span style='font-size:24px;'> ➜ </span>"
    )
    
    HTML(paste0("<div style='margin-top:20px;'>", boxes, "</div>"))
  })
  
  # ----------------------------------------------------------
  # QUIZ LOGIC (FULLY FIXED)
  # ----------------------------------------------------------
  output$a1 <- renderText({
    req(input$q1)
    if (input$q1 == "count()") "Correct! count() is an action." else "Try again."
  })
  
  output$a2 <- renderText({
    req(input$q2)
    if (input$q2 == "Immutable") "Correct! Spark DataFrames are immutable." else "Try again."
  })
  
  output$a3 <- renderText({
    req(input$q3)
    if (input$q3 == "groupBy()") "Correct! groupBy() triggers a shuffle." else "Try again."
  })
  
  output$a4 <- renderText({
    req(input$q4)
    if (input$q4 == "Sends a small table to all executors")
      "Correct! Broadcast joins avoid shuffles by sending the small table to all executors."
    else "Try again."
  })
  
  output$a5 <- renderText({
    req(input$q5)
    if (input$q5 == "Optimizing logical and physical plans")
      "Correct! Catalyst rewrites your query into an optimized plan."
    else "Try again."
  })
  
  output$a6 <- renderText({
    req(input$q6)
    if (input$q6 == "To fix skewed keys before a join")
      "Correct! Repartitioning can help fix skew before a join."
    else "Try again."
  })
  
  output$a7 <- renderText({
    req(input$q7)
    if (input$q7 == "Because Spark sends the small table to every worker so the big table doesn't need to move")
      "Correct! Broadcasting avoids shuffling the large table."
    else "Try again."
  })
  
  # ----------------------------------------------------------
  # DataFrame operations table
  # ----------------------------------------------------------
  output$df_table <- renderDT({
    datatable(
      data.frame(
        Operation = c("select", "filter", "withColumn", "groupBy", "join"),
        Description = c(
          "Choose columns",
          "Filter rows",
          "Create/modify columns",
          "Aggregate",
          "Combine DataFrames"
        )
      )
    )
  })
  
  output$window_sample_data <- renderText({
    "
id  category  date         amount
1   A         2024-01-01   100
2   A         2024-01-02   150
3   A         2024-01-03   150
4   B         2024-01-01   200
5   B         2024-01-02   180
6   B         2024-01-03   220
"
  })
  
  
  output$catalyst_table <- renderDT({
    datatable(
      data.frame(
        Feature = c(
          "Constant Folding",
          "Predicate Pushdown",
          "Column Pruning",
          "Join Reordering",
          "Operator Selection"
        ),
        Description = c(
          "Evaluates constant expressions at compile time",
          "Pushes filters to data source",
          "Reads only required columns",
          "Reorders joins to reduce shuffle",
          "Chooses fastest physical operators"
        )
      )
    )
  })
  
  
  output$tungsten_table <- renderDT({
    datatable(
      data.frame(
        Feature = c(
          "Off‑heap Memory",
          "Binary Row Format",
          "Whole‑Stage Codegen",
          "Cache‑Aware Execution"
        ),
        Description = c(
          "Reduces GC pressure by storing data outside JVM heap",
          "Compact binary format for fast processing",
          "Generates optimized Java bytecode for pipelines",
          "Optimizes CPU cache usage for faster execution"
        )
      )
    )
  })
  
  
  output$partition_table <- renderDT({
    datatable(
      data.frame(
        Operation = c("repartition", "coalesce", "groupBy", "join"),
        Shuffle = c("Yes", "No", "Yes", "Often"),
        Description = c(
          "Increase partitions; redistributes data",
          "Reduce partitions; avoids shuffle",
          "Aggregates by key; requires shuffle",
          "May require shuffle depending on join keys"
        )
      )
    )
  })
  
  output$repartition_table <- renderDT({
    datatable(
      data.frame(
        Operation = c("repartition(n)", "repartition(col)", "coalesce(n)"),
        Shuffle = c("Yes", "Yes", "No"),
        Use_Case = c(
          "Increase partitions for parallelism",
          "Fix skew or prepare for joins",
          "Reduce partitions before writing"
        )
      )
    )
  })
  
  output$broadcast_table <- renderDT({
    datatable(
      data.frame(
        Join_Type = c("Broadcast Join", "Shuffle Join"),
        Movement = c("Small table sent to all executors", "Both tables shuffled"),
        Best_For = c("Small dimension tables", "Large tables or skewed keys")
      )
    )
  })
  
  
  output$wide_narrow_table <- renderDT({
    datatable(
      data.frame(
        Type = c("Narrow", "Narrow", "Wide"),
        Operation = c("select", "filter", "groupBy"),
        Shuffle = c("No", "No", "Yes")
      )
    )
  })
  
  
  output$cache_table <- renderDT({
    datatable(
      data.frame(
        Level = c(
          "MEMORY_ONLY",
          "MEMORY_AND_DISK",
          "DISK_ONLY",
          "MEMORY_ONLY_SER"
        ),
        Description = c(
          "Fastest; stored in memory",
          "Memory first, spill to disk",
          "Stored only on disk",
          "Serialized in memory (less RAM)"
        )
      )
    )
  })
  
  output$dag_table <- renderDT({
    datatable(
      data.frame(
        Concept = c("DAG", "Stage", "Task", "Shuffle"),
        Meaning = c(
          "Directed graph of transformations",
          "Group of tasks with no shuffle",
          "Unit of work executed on executor",
          "Data movement between partitions"
        )
      )
    )
  })
  
  output$architecture_table <- renderDT({
    datatable(
      data.frame(
        Component = c("Driver", "Cluster Manager", "Executors"),
        Role = c(
          "Builds logical plan, schedules tasks, coordinates execution",
          "Allocates resources and manages executors",
          "Run tasks, cache data, return results"
        )
      )
    )
  })
  
  
  output$pyspark_basics_table <- renderDT({
    datatable(
      data.frame(
        Concept = c(
          "SparkSession",
          "DataFrame",
          "Lazy Evaluation",
          "Transformations",
          "Actions",
          "Spark SQL"
        ),
        Meaning = c(
          "Entry point to Spark; creates driver",
          "Distributed table with schema",
          "Spark builds plan but does not run immediately",
          "Operations that build the DAG",
          "Operations that trigger execution",
          "SQL interface on top of DataFrames"
        )
      )
    )
  })
  
  output$joins_table <- renderDT({
    datatable(
      data.frame(
        Join = c("Inner", "Left", "Right", "Full Outer", "Left Semi", "Left Anti", "Cross"),
        Description = c(
          "Matching rows only",
          "All left rows + matches",
          "All right rows + matches",
          "All rows from both sides",
          "Left rows with a match",
          "Left rows with NO match",
          "Cartesian product"
        )
      )
    )
  })
  
  
  
  output$transform_table <- renderDT({
    datatable(
      data.frame(
        Type = c("Transformation", "Transformation", "Action", "Action"),
        Operation = c("select", "filter", "count", "collect"),
        Description = c(
          "Choose columns",
          "Filter rows",
          "Return number of rows",
          "Return all rows to driver"
        )
      )
    )
  })
  
  # --- ANSWER KEY ---
  answers <- list(
    q1 = "count()",
    q2 = "Immutable",
    q3 = "groupBy()",
    q4 = "Sends a small table to all executors",
    q5 = "Optimizing logical and physical plans",
    q6 = "To fix skewed keys before a join",
    q7 = "Because Spark sends the small table to every worker so the big table doesn't need to move",
    q8 = "Each output partition depends on a single input partition",
    q9 = "groupBy()",
    q10 = "A shuffle boundary",
    q11 = "Improve memory and CPU efficiency with binary format and code generation",
    q12 = "Joining a large fact table with a small dimension table",
    q13 = "When one key has far more rows than others",
    q14 = "Fixing skew by spreading hot keys across partitions",
    q15 = "Stores DataFrame in memory for faster reuse",
    q16 = "count()",
    q17 = "The physical and logical plan",
    q18 = "When decreasing partitions without shuffle",
    q19 = "Falls back to slower row-by-row execution",
    q20 = "Redistribution of data across partitions",
    q21 = "Spark increases partitions and triggers a shuffle",
    q22 = "It uses the same optimizer but allows SQL syntax",
    q23 = "A group of tasks separated by shuffle boundaries",
    q24 = "A unit of work executed on a single partition",
    q25 = "collect()"
  )
  
  # --- RENDER LOGIC FOR Q1–Q25 ---
  for (i in 1:25) {
    local({
      ii <- i
      output[[paste0("a", ii)]] <- renderText({
        req(input[[paste0("q", ii)]])
        if (input[[paste0("q", ii)]] == answers[[paste0("q", ii)]]) {
          "✅ Correct!"
        } else {
          "❌ Try again."
        }
      })
    })
  }
  # --- ANSWER KEY CONTINUED ---
  answers2 <- list(
    q26 = "Stores data in memory and spills to disk if needed",
    q27 = "200",
    q28 = "Converts DataFrame to RDD",
    q29 = "A variable sent to all executors",
    q30 = "Deletes existing data and writes new data",
    q31 = "Break lineage to avoid long dependency chains",
    q32 = "Cross join",
    q33 = "Summary statistics",
    q34 = "Show the execution plan",
    q35 = "Returns a new DataFrame with at most 10 rows",
    q36 = "Set number of shuffle partitions",
    q37 = "10MB",
    q38 = "Removes duplicate rows",
    q39 = "A set of stages triggered by an action",
    q40 = "Triggers a shuffle",
    q41 = "Randomly sample rows",
    q42 = "Scans all partitions",
    q43 = "Range‑partition rows by a column",
    q44 = "Optimized execution plan",
    q45 = "Column names and types",
    q46 = "The unoptimized representation of the query",
    q47 = "Writes data into directory partitions based on column values",
    q48 = "Manage tables and databases",
    q49 = "Converts DataFrame to Pandas on the driver",
    q50 = "It can crash the driver if the DataFrame is too large"
  )
  
  # --- RENDER LOGIC FOR Q26–Q50 ---
  for (i in 26:50) {
    local({
      ii <- i
      output[[paste0("a", ii)]] <- renderText({
        req(input[[paste0("q", ii)]])
        if (input[[paste0("q", ii)]] == answers2[[paste0("q", ii)]]) {
          "✅ Correct!"
        } else {
          "❌ Try again."
        }
      })
    })
  }
  
  
}

shinyApp(ui, server)
