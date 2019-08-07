### To use this R API CLient to access Data Commons REST API

In the R console, run:

```
if(!require(devtools)) install.packages("devtools")
library(devtools)
devtools::install_github("google/datacommons/api-R")
library(datacommons)
```

### To develop on this R API Client

#### Clone the Repo
[Data Commons Github Repo](https://github.com/google/datacommons)
[GitHub Cloning Docs](https://help.github.com/en/articles/cloning-a-repository)

#### Load the devtools library
```
if(!require(devtools)) install.packages("devtools")
library(devtools)
```

#### To load/reload the code
Keyboard shortcut: `Cmd/Ctrl + Shift + L`

Or in R console, run:
```
# Make sure you're inside the R API Client directory
devtools::load_all()
```

#### To generate/regenerate the docs
Keyboard shortcut: `Cmd/Ctrl + Shift + D` (if this doesn't work, go to
`Tools > Project Options > Build Tools`
and check `Generate documentation with Roxygen`)

Or in R console, run:
```
# Make sure you're inside the R API Client directory
devtools::document()
```

These commands trigger the roxygen2 package to regenerate the docs based on
any changes to the docstrings in the R/ folder. Here is an
[introduction](https://cran.r-project.org/web/packages/roxygen2/vignettes/roxygen2.html)
to using roxygen2.

#### To run tests
Keyboard shortcut: `Cmd/Ctrl + Shift + T`

Or in R console, run:
```
# Make sure you're inside the R API Client directory
devtools::test()
```

#### Working with Reticulate

In `zzz.R`, the Python Client dependency is installed via pip. On many systems,
this would default to install the Python Client to Python2. You can use pip3 to
install the Python Client in Python3. If you do so, in the R console:
```
# Modify, uncomment, and run the next line:
# use_python("/usr/local/bin/python3.7")
```
Reticulate also supports the usage of virtual environments. Learn more at https://rstudio.github.io/reticulate/articles/versions.html
