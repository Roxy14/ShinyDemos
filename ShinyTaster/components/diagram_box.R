diagram_box <- function(img = NULL, title = NULL, content = NULL) {
  div(
    style = "
      background:#222;
      border:1px solid #444;
      border-radius:8px;
      padding:15px;
      text-align:center;
      margin-bottom:20px;
      color:#eee;
    ",
    if (!is.null(title)) h4(title, style = "margin-top:0;margin-bottom:10px;"),
    if (!is.null(img)) img(src = img, style = "max-width:100%;border-radius:6px;margin-bottom:10px;"),
    if (!is.null(content)) content
  )
}
