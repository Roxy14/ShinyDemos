section_banner <- function(text, type = c("reactive", "observe", "event", "basics")) {
  type <- match.arg(type)

  colors <- list(
    reactive = "#4db8ff",
    observe  = "#ffcc00",
    event    = "#66ff99",
    basics   = "#9b7cff"
  )

  div(
    style = sprintf("
      background-color: #2a2f33;
      border-left: 5px solid %s;
      padding: 12px 16px;
      margin-top: 25px;
      margin-bottom: 18px;
      font-weight: 700;
      font-size: 2.0rem;      /* <-- BIGGER TITLE */
      color: #e6e6e6;
      border-radius: 4px;
    ", colors[[type]]),
    text
  )
}
