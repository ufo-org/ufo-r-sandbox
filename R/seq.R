#' Creates a UFO object of type `integer` constainig a sequence of numbers.
#' Uses seq.int internally to generate each fragment of the sequence.
#' @param from the first number in the sequence
#' @param to the last number in the sequence
#' @param by the step of the sequence (default: 1)
#' @param read_only sets the vector to be write-protected by the OS
#'                  (optional, false by default).
#' @param chunk_length the minimum number of elements loaded at once,
#'                     will always be rounded up to a full memory page
#'                     (optional, a page by default).
#' @param ... other ufo configuration options (see `ufo_integer_constructor`)
#' @return a ufo vector lazily populated with the values of the specified
#'         sequence
#' @export
ufo_integer_seq <- function(from, to, by = 1, ...) {
    ufo_integer_constructor(
        length = (to - from) / by + 1,
        populate = function(start, end, from, by, ...) {
            as.integer(seq.int(
                from = from + (start - 1) * by,
                to   = from + (end   - 1) * by,
                by   = by
            ))
        },
        ...
    )
}

#' Creates a UFO object of type `numeric` constainig a sequence of numbers.
#' Uses seq.int internally to generate each fragment of the sequence.
#' @param from the first number in the sequence
#' @param to the last number in the sequence
#' @param by the step of the sequence (default: 1)
#' @param read_only sets the vector to be write-protected by the OS
#'                  (optional, false by default).
#' @param chunk_length the minimum number of elements loaded at once,
#'                     will always be rounded up to a full memory page
#'                     (optional, a page by default).
#' @param ... other ufo configuration options (see `ufo_integer_constructor`)
#' @return a ufo vector lazily populated with the values of the specified
#'         sequence
#' @export
ufo_numeric_seq <- function(from, to, by = 1, ...) {
    ufo_integer_constructor(
        length = (to - from) / by + 1,
        populate = function(start, end, from, by, ...) {
            as.numeric(seq.int(
                from = from + (start - 1) * by,
                to   = from + (end   - 1) * by,
                by   = by
            ))
        },
        ...
    )
}