# helper function to ensure that dcids input is either
# a single element list or
# a multi-element vector
# to make sure Reticulate converts the input to a Python list
# https://rstudio.github.io/reticulate/articles/calling_python.html#type-conversions
ConvertibleToPythonList = function(input) {
  if (is.null(input) || length(input) < 1) {
    stop("input cannot be empty")
  }

  if (is.data.frame(input)) {
    # cast factors to string and coerce data frame to tibble
    input[] <- lapply(input, as.character)
    # input = tibble(input)

    if (dim(input)[[2]] > 1) {
      warning("input data frame should only contain one column;
              this time, we will take the first column")
    }
    return(input[1])
  }

  if (is.list(input)) {
    if (length(input) != 1) {
      stop("please use atomic vector if more than 1 element")
    }
    if (!is.null(names(input))) {
      stop("please use unnamed lists")
    }
    return(input)
  }

  if (!is.vector(input) || !is.atomic(input) || !(typeof(input) == "character")) {
    stop("input must be string, vector of strings, or list of 1 string")
  }

  # cast single string to list, otherwise Reticulate will not allow conversion to py list
  if (length(input) == 1) {
    return(list(input))
  }

  return(input)
}

# testthat helper function to skip tests if we don't have the 'datacommons' python module
# helpful for CRAN machines that might not have datacommons
skip_if_no_dcpy <- function() {
  have_dcpy <- py_module_available("datacommons")
  print(py_config())
  if (!have_dcpy)
    skip("python client not available for testing")
}
