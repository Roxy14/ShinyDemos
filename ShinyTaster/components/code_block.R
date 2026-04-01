code_block <- function(id, code, height = NULL) {
  aceEditor(
    outputId = id,
    value = code,
    mode = "r",
    theme = "chrome",   # ← light theme
    fontSize = 14,
    readOnly = TRUE,
    minLines = 6,
    maxLines = 20,
    height = height %||% "200px"
  )
}
