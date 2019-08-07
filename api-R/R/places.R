# Data Commons Node API - Places Convenience Function
#
# GetPlacesIn
#
# These functions provide R access to
#   Data Commons Node API Places convenience functions.
#   These functions are designed to making adding a new column
#   to a data frame convenient!
#   www.DataCommons.org

#' Return places of a specified type contained in specified places
#'
#' Returns a mapping between the specified places
#'   and the places of a specified type contained in them.
#'
#' Assigning output to a tibble/data frame will yield a list of contained
#' places. To convert this to 1-to-1 mapping (the containing place will
#' be repeated), use \code{tidyr::unnest}.
#'
#' @param dcids required, dcid(s) identifying a containing place.
#'   This parameter will accept a vector of strings
#'   or a single-column tibble/data frame of strings.
#'   To select a single column, use \code{select(df, col)}.
#' @param placeType required, string identifying the type of place to query for.
#' @return Named list or column of places contained in each given dcid of the
#'   given placeType. If dcids input is vector of strings, will return a named
#'   list. If dcids input is tibble/data frame, will return a new single-column
#'   tibble/data frame.
#' @export
#' @examples
#' # Atomic vector of the dcids of Santa Clara and Montgomery County.
#' countyDcids <- c('geoId/06085', 'geoId/24031')
#' # Get towns in Santa Clara and Montgomery County.
#' towns <- GetPlacesIn(countyDcids, 'Town')
#'
#' # Tibble of the dcids of Santa Clara and Montgomery County.
#' df <- tibble(countyDcid = c('geoId/06085', 'geoId/24031'))
#' # Get towns in Santa Clara and Montgomery County.
#' df$townDcid <- GetPlacesIn(df, 'Town')
#' # Since GetPlacesIn returned a mapping between counties and
#' # a list of towns, use you can use tidyr::unnest to create
#' # a 1-1 mapping between each county and its towns.
GetPlacesIn <- function(dcids, placeType) {
  dcids = ConvertibleToPythonList(dcids)
  return(dc$get_places_in(dcids, placeType))
}
