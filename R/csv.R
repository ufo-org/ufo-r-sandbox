#' Creates a dataframe representing a CSV file, where each column is a lazily
#' popuilated UFO. In order to figure out column types, does a scan of the whole
#' file at the outset.
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
ufo_csv <- function(path, ...) {
    #connection <- file(path)
    stop("unimplemented")
}

# ufo_csv_initial_scan <- function(connection, header) {

# }

# # Tokenizer states
# INITIAL <- as.integer(0)
# FINAL <- as.integer(-1)
# CRASHED <- as.integer(-2)
# FIELD <- as.integer(1)
# UNQUOTED_FIELD <- as.integer(2)
# QUOTED_FIELD <- as.integer(3)
# QUOTE <- as.integer(4)
# TRAILING <- as.integer(5)
# ESCAPE <- as.integer(6)

# ufo_csv_next_token <- function(tokenizer) {
#     if (tokenizer$state  == FIELD) {

#     }
# }