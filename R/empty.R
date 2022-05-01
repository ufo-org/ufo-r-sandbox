# Implementations of empty UFO vectors.

#' Creates a UFO object of type `integer`
#' @param length the length fo the ufo integer vector, has to be larger than 1
#' @param initial_value the intial value of the elements in the vector
#'                      (0 by default)
#' @param read_only sets the vector to be write-protected by the OS
#'                  (optional, false by default).
#' @param chunk_length the minimum number of elements loaded at once,
#'                     will always be rounded up to a full memory page
#'                     (optional, a page by default).
#' @param ... other ufo configuration options (see `ufo_integer_constructor`)
#' @return a ufo vector of size `length`, lazily populated with `initial_value`
#' @export
ufo_integer <- function(length = 0, initial_value = 0, ...) {
    if (length(initial_value) != 1)
        stop("Initial value must be a scalar integer")

    ufo_integer_constructor(
        length = length,
        initial_value = as.integer(initial_value),
        populate = function(start, end, initial_value, ...) {
            if (initial_value == 0) integer(end - start)
            else rep(initial_value, end - start)
        },
        ...
    )
}

# Populate function for the `ufo_integer` constructor.
#' @param start index of the the first element (in the complete vector)
#' @param end index of the last element (in the complete vector)
#' @param initial_value the initial value of the elements
#' @param ... unused
#' @returns an empty integer vector
populate_numeric <- function(start, end, initial_value, ...) {
    numeric(end - start)
}
