library(testthat)
library(ufosandbox)

options(warn=1)

test_check("ufosandbox", reporter = SummaryReporter$new())
