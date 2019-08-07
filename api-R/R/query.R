#' Query Data Commons Using SPARQL
#'
#' This function allows you to build R dataframes using data from
#' the Data Commons Open Knowledge Graph via SPARQL queries to
#' the Query API.
#' www.DataCommons.org
#'
#' @param queryString required, SPARQL query string.
#' @return A populated data frame with columns specified with SPARQL.
#' @export
#' @examples
#' queryString = "SELECT ?pop ?Unemployment
#'     WHERE {
#'       ?pop typeOf StatisticalPopulation .
#'       ?o typeOf Observation .
#'       ?pop dcid (\"dc/p/qep2q2lcc3rcc\" \"dc/p/gmw3cn8tmsnth\" \"dc/p/92cxc027krdcd\") .
#'       ?o observedNode ?pop .
#'       ?o measuredValue ?Unemployment
#'     }"
#' df = Query(queryString)
Query <- function(queryString) {
  # Encode the query to REST URL
  urlEncodedQuery <- URLencode(queryString, reserved = TRUE)
  reqUrl <- paste0("http://api.datacommons.org/query?sparql=", URLencode(urlEncodedQuery))
  resp <- GET(reqUrl)
  if (http_type(resp) != "application/json") {
    stop("API did not return json", call. = FALSE)
  }

  # Parse response
  parsedResp <- jsonlite::fromJSON(content(resp, "text"), simplifyVector = FALSE)
  if (http_error(resp)) {
    stop(
      sprintf(
        "Data Commons API request failed [%s]\n%s",
        status_code(resp),
        parsedResp$message
      ),
      call. = FALSE
    )
  }

  # Prettify the response into typical JSON
  columns <- parsedResp[1]$header
  numCols <- length(columns)
  numRows <- length(parsedResp[2]$rows)
  if (numRows < 1) {
    stop("No rows in response.", call. = FALSE)
  }
  if (numCols < 1) {
    stop("No cols in response.", call. = FALSE)
  }
  objList <- vector("list", numRows)
  for (obj in 1:numRows) {
    rowKVs = parsedResp[2]$rows[[obj]]$cells

    KVList = vector("list", numCols)
    for (attr in 1:numCols) {
      KVList[[attr]] = paste0("\"", columns[attr], "\"", ":", "\"", rowKVs[attr][[1]]$value, "\"")
    }
    KVText = paste(KVList, collapse=",")
    # Write JSON object
    objList[[obj]] = paste0("{", KVText, "}")
  }
  objTexts <- paste(objList, collapse = ",")
  prettyJsonResp <- paste0("[", objTexts, "]")

  # Return an R dataframe
  df <- fromJSON(prettyJsonResp)
  return(df)
}
