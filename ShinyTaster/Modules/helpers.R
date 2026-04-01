`%||%` <- function(a, b) if (!is.null(a)) a else b

section_header <- function(text) {
  h3(style = "margin-top: 25px;", text)
}

two_column <- function(left, right) {
  fluidRow(
    column(6, left),
    column(6, right)
  )
}

example_block <- function(...) {
  div(
    style = "
      background:#111;
      border-left:4px solid #4db8ff;
      padding:12px;
      margin:12px 0;
      border-radius:6px;
    ",
    ...
  )
}
