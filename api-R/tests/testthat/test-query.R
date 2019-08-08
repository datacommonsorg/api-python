context("Data Commons Query API - R Client")

test_that("query returns California data", {
  califQuery1 <- "SELECT  ?name
    WHERE {
     ?a typeOf Place .
     ?a name ?name .
     ?a dcid \"geoId/06\"
    }
    "
  califQuery2 <- "SELECT  ?name
    WHERE {
     ?a typeOf Place .
     ?a name ?name .
     ?a dcid 'geoId/06'
    }
    "

  expect_identical(Query(califQuery1)[[1]], "California")
  expect_identical(Query(califQuery2)[[1]], "California")
})

test_that("query returns large dataframe with no NA's", {
  unemploymentQuery = "SELECT ?pop ?Unemployment
    WHERE {
      ?pop typeOf StatisticalPopulation .
      ?o typeOf Observation .
      ?pop dcid ('dc/p/qep2q2lcc3rcc' 'dc/p/gmw3cn8tmsnth' 'dc/p/92cxc027krdcd') .
      ?o observedNode ?pop .
      ?o measuredValue ?Unemployment
    }"
  df = Query(unemploymentQuery)

  expect_gt(dim(df)[1], 400)
  expect_equal(dim(df)[2], 2)
  expect_true('dc/p/qep2q2lcc3rcc' %in% df$`?pop`)
  expect_true('dc/p/gmw3cn8tmsnth' %in% df$`?pop`)
  expect_true('dc/p/92cxc027krdcd' %in% df$`?pop`)
  expect_true(3.8 %in% df$`?Unemployment`)
  expect_true(4.7 %in% df$`?Unemployment`)
})

