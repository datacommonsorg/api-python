# zzz.R
# File for setup

# global reference to Python Client(will be initialized in .onLoad)
dc <- NULL

.onLoad <- function(libname, pkgname) {
  # use superassignment to update global reference to dc
  # setting delay_load delays loading the module until it is first used
  # thus, user can first set python version in R after loading in this
  # R API Client package
  system("pip install --upgrade --quiet git+https://github.com/google/datacommons.git@dev2")
  dc <<- reticulate::import("datacommons", delay_load = TRUE)
}
