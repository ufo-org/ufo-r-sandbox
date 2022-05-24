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
        stop("Initial value must be a single value")

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

#' Creates a UFO object of type `numeric`
#' @param length the length fo the ufo numeric vector, has to be larger than 1
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
ufo_numeric <- function(length = 0, initial_value = 0, ...) {
    if (length(initial_value) != 1)
        stop("Initial value must be a single value")

    ufo_numeric_constructor(
        length = length,
        initial_value = as.numeric(initial_value),
        populate = function(start, end, initial_value, ...) {
            if (initial_value == 0) numeric(end - start)
            else rep(initial_value, end - start)
        },
        ...
    )
}

#' Creates a UFO object of type `logical`
#' @param length the length fo the ufo logical vector, has to be larger than 1
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
ufo_logical <- function(length = 0, initial_value = 0, ...) {
    if (length(initial_value) != 1)
        stop("Initial value must be a single value")

    ufo_logical_constructor(
        length = length,
        initial_value = as.logical(initial_value),
        populate = function(start, end, initial_value, ...) {
            if (initial_value == 0) logical(end - start)
            else rep(initial_value, end - start)
        },
        ...
    )
}

#' Creates a UFO object of type `complex`
#' @param length the length fo the ufo complex vector, has to be larger than 1
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
ufo_complex <- function(length = 0, initial_value = 0, ...) {
    if (length(initial_value) != 1)
        stop("Initial value must be a single value")

    ufo_complex_constructor(
        length = length,
        initial_value = as.complex(initial_value),
        populate = function(start, end, initial_value, ...) {
            if (initial_value == 0) complex(end - start)
            else rep(initial_value, end - start)
        },
        ...
    )
}

#' Creates a UFO object of type `character`. This is aiffy, since it ovbviates
#' R's interning mechanism. Use with caution
#' @param length the length fo the ufo complex vector, has to be larger than 1
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
ufo_character <- function(length = 0, initial_value = 0, ...) {
    if (length(initial_value) != 1)
        stop("Initial value must be a single value")

    ufo_character_constructor(
        length = length,
        initial_value = as.character(initial_value),
        populate = function(start, end, initial_value, ...) {
            if (initial_value == 0) character(end - start)
            else rep(initial_value, end - start)
        },
        ...
    )
}

#' Creates a UFO object of type `raw`
#' @param length the length fo the ufo complex vector, has to be larger than 1
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
ufo_raw <- function(length = 0, initial_value = 0, ...) {
    if (length(initial_value) != 1)
        stop("Initial value must be a single value")

    ufo_raw_constructor(
        length = length,
        initial_value = as.raw(initial_value),
        populate = function(start, end, initial_value, ...) {
            if (initial_value == 0) raw(end - start)
            else rep(initial_value, end - start)
        },
        ...
    )
}