context("Data Commons Node API (Populations and Observations Extension) - R Client")

test_that("GetPopulations gets populations", {
  skip_if_no_dcpy()

  # INPUT atomic vector of the dcids of California, Kentucky, and Maryland.
  stateDcids <- c('geoId/06', 'geoId/21', 'geoId/24')
  # Get the population dcids for each state.
  femalePops <- GetPopulations(stateDcids, 'Person', list(gender = 'Female'))
  malePops <- GetPopulations(stateDcids, 'Person', list(gender = 'Male'))

  expect_equal(length(femalePops), 3)
  expect_setequal(names(femalePops), list("geoId/06", "geoId/21", "geoId/24"))
  expect_equal(length(femalePops[[1]]), 1)
  expect_match(femalePops[[1]], "dc/p/.*")

  expect_equal(length(malePops), 3)
  expect_setequal(names(malePops), list("geoId/06", "geoId/21", "geoId/24"))
  expect_equal(length(malePops[[1]]), 1)
  expect_match(malePops[[1]], "dc/p/.*")

  # INPUT tibble of the dcids of California, Kentucky, and Maryland; and random column.
  df <- tibble(countyDcid = c('geoId/06', 'geoId/21', 'geoId/24'), rand = c(1, 2, 3))
  # Get the population dcids for each state.
  femalePopsTibble <- GetPopulations(select(df, countyDcid), 'Person', list(gender = 'Female'))
  malePopsTibble <- GetPopulations(select(df, countyDcid), 'Person', list(gender = 'Male'))
  expect_setequal(femalePopsTibble, femalePops)
  expect_setequal(malePopsTibble, malePops)

  # INPUT data frame
  df <- data.frame(countyDcid = c('geoId/06', 'geoId/21', 'geoId/24'), rand = c(1, 2, 3))
  # Using df$col as input to GetPopulations will cause problems for data frames.
  # While it will work for tibbles, we encourage using select(df, col).
  expect_error(GetPopulations(df$countyDcid, 'Person', list(gender = 'Female')))
  # Correct way to select column
  expect_setequal(GetPopulations(select(df, countyDcid), 'Person', list(gender = 'Female')), as.array(unlist(femalePopsTibble)))
  expect_setequal(GetPopulations(select(df, countyDcid), 'Person', list(gender = 'Male')), as.array(unlist(malePopsTibble)))
})

test_that("GetObservations gets data", {
  skip_if_no_dcpy()

  # INPUT character vector
  # Set the dcid to be that of Santa Clara County.
  sccDcid <- 'geoId/06085'
  # Get the population dcids for Santa Clara County.
  femalePops <- GetPopulations(sccDcid, 'Person', list(gender = 'Female'))
  malePops <- GetPopulations(sccDcid, 'Person', list(gender = 'Male'))

  expect_equal(length(femalePops), 1)
  expect_identical(names(femalePops), "geoId/06085")
  expect_equal(length(femalePops[[1]]), 1)
  expect_match(femalePops[[1]], "dc/p/.*")

  expect_equal(length(malePops), 1)
  expect_identical(names(malePops), "geoId/06085")
  expect_equal(length(malePops[[1]]), 1)
  expect_match(malePops[[1]], "dc/p/.*")

  femaleCount <- GetObservations(unlist(femalePops), 'count', 'measured_value', '2016', measurementMethod = 'CenusACS5yrSurvey')
  maleCount <- GetObservations(unlist(malePops), 'count', 'measured_value', '2016', measurementMethod = 'CenusACS5yrSurvey')

  expect_gt(as.numeric(femaleCount), 500000)
  expect_gt(as.numeric(maleCount), 500000)

  # INPUT tibble of the dcids of California, Kentucky, and Maryland; and random column.
  df <- tibble(countyDcid = c('geoId/06', 'geoId/21', 'geoId/24'), rand = c(1, 2, 3))
  # Get the population dcids for each state.
  df$femalePops <- GetPopulations(select(df, countyDcid), 'Person', list(gender = 'Female'))
  df$malePops <- GetPopulations(select(df, countyDcid), 'Person', list(gender = 'Male'))
  df = unnest(df)
  # Get observations
  df$femaleCount <- GetObservations(select(df,femalePops), 'count', 'measured_value', '2016', measurementMethod = 'CenusACS5yrSurvey')
  df$maleCount <- GetObservations(select(df,malePops), 'count', 'measured_value', '2016', measurementMethod = 'CenusACS5yrSurvey')

  expect_gt(as.numeric(df$femaleCount[2]), 2000000)
  expect_gt(as.numeric(df$maleCount[2]), 2000000)
})

