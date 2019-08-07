# Data Commons Node API - Core Functions
#
# These functions provide R access to core
#   Data Commons Node API functions:
#   get_property_labels, get_property_values, get_triples.
#   www.DataCommons.org


#' Return property labels of specified nodes
#'
#' Returns a map between nodes and outgoing (default) or incoming
#'   property labels.
#'
#' @param dcids required, vector of string(s) that identify node(s) to get
#'   property labels for.
#' @param outgoing optional, boolean indicating whether to get properties
#'   originating from
#'   the given node. TRUE by default.
#' @return Named list of properties associated with the given dcid(s) via the
#'   given direction.
#' @export
#' @examples
#' # dcid string of Santa Clara County.
#' sccDcid <- 'geoId/06085'
#' # Get incoming and outgoing properties for Santa Clara County.
#' inLabels <- GetPropertyLabels(sccDcid, outgoing = FALSE)
#' outLabels <- GetPropertyLabels(sccDcid)
#'
#' # List of dcid strings of Florida, Planned Parenthood West, and the
#' # Republican Party.
#' dcids <- c('geoId/12', 'plannedParenthood-PlannedParenthoodWest',
#'            'politicalParty/RepublicanParty')
#' # Get incoming and outgoing properties for Santa Clara County.
#' inLabels <- GetPropertyLabels(dcids, outgoing = FALSE)
#' outLabels <- GetPropertyLabels(dcids)
GetPropertyLabels <- function(dcids, outgoing = TRUE) {
  dcids = ConvertibleToPythonList(dcids)
  return(dc$get_property_labels(dcids, outgoing))
}

#' Return property values along a property for one or more nodes
#'
#' Returns all neighboring nodes of each specified node via the specified
#'   property and direction. The neighboring nodes are "values" for the
#'   property and can be leaf (primitive) nodes.
#'
#' @param dcids required, vector OR single-column tibble/data frame of
#'   string(s) that uniquely identify node(s) to get property values for.
#' @param prop required, string identifying the property to get the property
#'   values for.
#' @param outgoing optional, boolean indicating whether the property
#'   originates from the given node. TRUE by default.
#' @param valueType optional, string identifying the node type to filter the
#'   results by. NULL by default.
#' @param limit optional, integer indicating the maximum number of values to
#'   return across all properties. 100 by default.
#' @return Named list or column of values associated to given dcid(s) via the
#'   given property and direction.
#'   Will be encapsulated in a named list if dcids input is vector of strings,
#'   or a new single column tibble if dcids input is tibble/data frame.
#' @export
#' @examples
#' # Set the dcid to be that of Santa Clara County.
#' sccDcid <- 'geoId/06085'
#' # Get the landArea value of Santa Clara (a leaf node).
#' landArea <- GetPropertyValues(sccDcid, 'landArea')
#'
#' # Create a vector with Santa Clara and Miami-Dade County dcids
#' countyDcids <- c('geoId/06085', 'geoId/12086')
#' # Get all containing Cities.
#' cities <- GetPropertyValues(countyDcids, 'containedInPlace',
#'                             outgoing = FALSE, valueType = 'City')
#'
#'# Create a data frame with Santa Clara and Miami-Dade County dcids
#' df <- data.frame(countyDcid = c('geoId/06085', 'geoId/12086'))
#' # Get all containing Cities.
#' df$cityDcid <- GetPropertyValues(select(df, countyDcid), 'containedInPlace',
#'                                  outgoing = FALSE, valueType = 'City')
GetPropertyValues <- function(dcids, prop, outgoing = TRUE, valueType = NULL,
                              limit = 100) {
  dcids = ConvertibleToPythonList(dcids)
  return(dc$get_property_values(dcids, prop, outgoing, valueType, limit))
}

#' Return all triples involving specified nodes
#'
#' Returns all triples (subject-predicate-object) where the specified node is
#' either a subject or an object.
#'
#' @param dcids required, vector of string(s) that uniquely identify
#'   the node(s) to get triples for.
#' @param limit optional, integer indicating the max number of triples to get
#' for each property. 100 by default.
#' @return Map between each dcid and all triples with the dcid as the subject or
#' object. Triples are represented as (subject, predicate, object).
#' @export
#' @examples
#' # Set the dcid to be that of Santa Clara County.
#' sccDcid <- 'geoId/06085'
#' # Get triples.
#' triples <- GetTriples(sccDcid)
#'
#' # List of dcid strings of Florida, Planned Parenthood West, and the
#' # Republican Party.
#' dcids <- c('geoId/12', 'plannedParenthood-PlannedParenthoodWest',
#'            'politicalParty/RepublicanParty')
#' # Get triples.
#' triples <- GetPropertyLabels(dcids)
GetTriples <- function(dcids, limit = 100) {
  dcids = ConvertibleToPythonList(dcids)
  return(dc$get_triples(dcids, limit))
}
