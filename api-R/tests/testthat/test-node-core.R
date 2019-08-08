context("Data Commons Node API (Core) - R Client")

test_that("GetPropertyLabels returns incoming and outgoing labels", {
  skip_if_no_dcpy()

  sccDcid <- list('geoId/06085')

  expect_true('containedInPlace' %in% GetPropertyLabels(sccDcid, outgoing = FALSE)[[1]])

  expectedOutLabels = c("ansiCode", "containedInPlace", "geoId", "kmlCoordinates",
                        "landArea", "latitude", "longitude", "name", "provenance",
                        "typeOf", "waterArea")
  actualOutLabels = GetPropertyLabels(sccDcid)
  expect_true('geoId' %in% actualOutLabels[[1]])
  expect_true('name' %in% actualOutLabels[[1]])
  expect_true('typeOf' %in% actualOutLabels[[1]])
  expect_true('containedInPlace' %in% actualOutLabels[[1]])
  expect_gt(sum(actualOutLabels[[1]] %in% expectedOutLabels * 1), 6)

  dcids <- c('geoId/12', 'plannedParenthood-PlannedParenthoodWest',
             'politicalParty/RepublicanParty')
  inLabels <- GetPropertyLabels(dcids, outgoing = FALSE)
  outLabels <- GetPropertyLabels(dcids)
  expect_equal(length(inLabels), 3)
  expect_gt(length(unlist(inLabels)), 4)
  expect_equal(length(outLabels), 3)
  expect_gt(length(unlist(outLabels)), 15)
})

test_that("GetPropertyValues returns incoming and outgoing edges", {
  skip_if_no_dcpy()

  sccDcid <- 'geoId/06085'
  landArea <- GetPropertyValues(sccDcid, 'landArea')
  area <- GetPropertyValues(unname(landArea), 'value')

  expect_gt(as.numeric(area), 2000000000)
  expect_lt(as.numeric(area), 5000000000)

  countyDcids <- c('geoId/06085', 'geoId/12086')
  # Get all containing Cities.
  cityDcids <- GetPropertyValues(countyDcids, 'containedInPlace', outgoing = FALSE, valueType = 'City')

  expect_gt(length(cityDcids['geoId/12086'][[1]]), 60)
  expect_lt(length(cityDcids['geoId/12086'][[1]]), 80)
  expect_match(cityDcids['geoId/12086'][[1]][1], 'geoId/12.*')

  expect_gt(length(cityDcids['geoId/06085'][[1]]), 15)
  expect_lt(length(cityDcids['geoId/06085'][[1]]), 35)
  expect_match(cityDcids['geoId/06085'][[1]][2], 'geoId/06.*')

  # Create a tibble with Santa Clara and Miami-Dade County dcids
  df <- tibble(countyDcid = c('geoId/06085', 'geoId/12086'))
  # Get all containing Cities.
  df$cityDcid <- GetPropertyValues(df$countyDcid, 'containedInPlace', outgoing = FALSE, valueType = 'City')
  expect_setequal(df$cityDcid, cityDcids)

  # Create a data frame with Santa Clara and Miami-Dade County dcids
  df <- data.frame(countyDcid = c('geoId/06085', 'geoId/12086'))
  # Get all containing Cities.
  df$cityDcid <- GetPropertyValues(select(df, countyDcid), 'containedInPlace', outgoing = FALSE, valueType = 'City')
  expect_setequal(df$cityDcid, cityDcids)
})

test_that("GetTriples returns triples involving given dcid(s)", {
  skip_if_no_dcpy()

  sccDcid <- 'geoId/06085'
  triples <- GetTriples(sccDcid, limit=100)

  expect_equal(length(triples), 1)
  # After Node API fixed, update this test
  expect_gte(length(triples[[1]]), 100)
  expect_equal(length(triples[[1]][[1]]), 3)
  expect_equal(length(triples[[1]][[23]]), 3)
  expect_equal(length(triples[[1]][[80]]), 3)

  countyDcids <- c('geoId/06085', 'geoId/12086')
  triples2 <- GetTriples(countyDcids, limit=100)

  expect_equal(length(triples2), 2)
  expect_equal(length(triples2[[1]]), length(triples[[1]]))
  # After Node API fixed, update this test
  expect_gte(length(triples2[[2]]), 100)
  expect_equal(length(triples2[[2]][[1]]), 3)
  expect_equal(length(triples2[[2]][[23]]), 3)
  expect_equal(length(triples2[[2]][[80]]), 3)
})
