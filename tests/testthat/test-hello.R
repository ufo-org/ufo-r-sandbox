test_that("Call to Rust function `hello_world()` works", {
  i <- ufo_integer(10)
  expect_equal(i, integer(10))
})
