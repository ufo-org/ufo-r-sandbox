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

new_raw_buffer <- function(max_size, values = raw(0)) { # -> env class:buffer
    buffer <- new.env()
    buffer$values = as.raw(values)
    buffer$max_size = max_size # no checks, just informative
    buffer$size = as.integer(length(buffer$values))
    buffer$pointer = as.integer(1)
    class(buffer) <- c("buffer", "raw_buffer")
    buffer
}

next_raw <- function(buffer) { # -> NULL | raw(..)
    if (buffer$pointer == buffer$size + 1) {
        NULL
    } else {
        result <- buffer$values[buffer$pointer]
        buffer$pointer = buffer$pointer + 1
        result
    }
}

has_next_raw <- function(buffer) {
    !(buffer$pointer >= buffer$size + 1)
}

fill_raw <- function(buffer, values) {
    buffer$values = as.raw(values)
    buffer$pointer = as.integer(1);
    buffer$size = as.integer(length(buffer$values))
}

print.buffer <- function(buffer, ...) {
    fields <- ls(buffer)
    for (i in 1:length(fields)) {
        field <- fields[i]
        value <- buffer[[field]]
        if (is.character(value)) {
            value <- sapply(value, function(v) paste0("'", v, "'"))
        }
        cat(paste0("[", field, "]: ", paste(value, collapse=", "), "\n"))
    }
}

as.character.buffer <- function(buffer, ...) {
    fields <- ls(buffer)
    output <- character(length(fields))
    for (i in 1:length(fields)) {
        field <- fields[i]
        value <- buffer[[field]]
        if (is.character(value)) {
            value <- sapply(value, function(v) paste0("'", v, "'"))
        }
        output[i] <- paste0("[", field, "]: ", paste(value, collapse=", "))
    }
    paste0("{", paste(output, collapse = "; "), "}")
}

new_token_buffer <- function(max_size, values = raw(max_size)) {
    buffer <- new.env()
    buffer$values = as.raw(values)
    buffer$max_size = max_size
    buffer$size = as.integer(0)
    class(buffer) <- c("buffer", "token_buffer")
    buffer
}

append_value_token_buffer <- function(buffer, value) {
    if (buffer$size == buffer$max_size) {
        buffer$max_size = buffer$max_size * 2
        new_values <- raw(buffer$max_size)
        new_values[1:length(buffer$values)] <- buffer$values
        buffer$values = new_values
    }

    buffer$size = buffer$size + 1
    buffer$values[buffer$size] = value
}

get_token_buffer <- function(buffer) {
    word <- if (buffer$size != 0) {
        rawToChar(buffer$values[1:buffer$size])
    } else {
        ""
    }
    class(word) <- c(class(word), "token")
    word
}

clear_token_buffer <- function(buffer) {
    buffer$size = as.integer(0)
}

#TOKENIZER_STATE          <- as.factor(c("FIELD", "UNQUOTED FIELD", "QUOTED FIELD", "QUOTE", "TRAILING", "ESCAPE", "FINAL", "CRASHED"))
TOKENIZER_FIELD          <- as.integer(1)
TOKENIZER_UNQUOTED_FIELD <- as.integer(2)
TOKENIZER_QUOTED_FIELD   <- as.integer(3)
TOKENIZER_QUOTE          <- as.integer(4)
TOKENIZER_TRAILING       <- as.integer(5)
TOKENIZER_ESCAPE         <- as.integer(6)
TOKENIZER_FINAL          <- as.integer(7)
TOKENIZER_CRASHED        <- as.integer(8)

new_tokenizer_state <- function(path, initial_offset, token_buffer_size, character_buffer_size) {
    state <- new.env()
    state$state = TOKENIZER_FIELD
    state$file = file(path, open = "rb")
    seek(state$file, initial_offset, origin = "start")
    state$state = TOKENIZER_FIELD
    state$initial_offset = as.integer(initial_offset)
    state$read_buffer = new_raw_buffer(character_buffer_size)
    state$token_buffer = new_token_buffer(token_buffer_size)
    state$current_offset = as.integer(initial_offset)
    state$end_of_last_token = as.integer(0)
    state$read_characters = as.integer(0)
    class(state) = "state"
    state
}

print.state <- function(state, ...) {
    fields <- ls(state)
    for (i in 1:length(fields)) {
        field <- fields[i]
        value <- state[[field]]
        if (is.character(value)) {
            value <- sapply(value, function(v) paste0("'", v, "'"))
        }
        cat(paste0("[", field, "]: ", paste(value, collapse=", "), "\n"))
    }
}

as.character.state <- function(buffer, ...) {
    fields <- ls(buffer)
    output <- character(length(fields))
    for (i in 1:length(fields)) {
        field <- fields[i]
        value <- buffer[[field]]
        if (is.character(value)) {
            value <- sapply(value, function(v) paste0("'", v, "'"))
        }
        output[i] <- paste0("[", field, "]: ", paste(value, collapse=", "))
    }
    paste0("{", paste(output, collapse = "; "), "}")
}

next_character <- function(state) {
    if (!has_next_raw(state$read_buffer)) {
        values <- readBin(state$file, raw(0), state$read_buffer$max_size)
        fill_raw(state$read_buffer, values)
    }
    value = next_raw(state$read_buffer)
    if (is.null(value)) {
        NULL
    } else {
        rawToChar(value)
    }
}

pop_token <- function(state, trim = FALSE) {
    token <- get_token_buffer(state$token_buffer)

    attributes(token)$position_start = state$end_of_last_token
    state$end_of_last_token = state$current_offset

    attributes(token)$position_end = state$current_offset

    clear_token_buffer(state$token_buffer)

    if (trim) {
        trimws(token, which = "right")
    } else {
        token
    }
}

skip_token <- function(state) {
    clear_token_buffer(state$token_buffer)
    NULL
}

#' @export
new_tokenizer <- function(path, initial_offset, token_buffer_size, character_buffer_size) {
    tokenizer <- new.env()

    tokenizer$row_delimiter = '\n'
    tokenizer$column_delimiter = ','
    tokenizer$escape = '\\'
    tokenizer$quote = '"'
    tokenizer$use_double_quote_escape = TRUE
    tokenizer$use_escape_character = TRUE
    class(tokenizer) <- "tokenizer"

    tokenizer$state <- new_tokenizer_state(path, initial_offset, token_buffer_size, character_buffer_size)

    tokenizer
}

transition <- function(state, next_state) {
    state$state = next_state
    state$current_offset = state$current_offset + 1
}

pop_token_or_skip <- function(state, skip = FALSE, trim = FALSE) {
    if (skip) {
        skip_token(state)
    } else {
        pop_token(state, trim)
    }
}

with_exit_status <- function(status, token) {
    list(status = status, token = token)
}

append_value <- function(state, value) {
    append_value_token_buffer(state$token_buffer, charToRaw(value))
}

TOKENIZER_RESULT      <- as.factor(c("OK", "END OF ROW", "END OF FILE", "PARSE ERROR", "ERROR"))
TOKENIZER_OK          <- TOKENIZER_RESULT[1]
TOKENIZER_END_OF_ROW  <- TOKENIZER_RESULT[2]
TOKENIZER_END_OF_FILE <- TOKENIZER_RESULT[3]
TOKENIZER_PARSE_ERROR <- TOKENIZER_RESULT[4]
TOKENIZER_ERROR       <- TOKENIZER_RESULT[5]

pop_token_with_status <- function(state, status, skip) {
    list(
        status = status,
        token = pop_token_or_skip(state, skip = skip)
    )
}

#' @export
next_token <- function(tokenizer, skip = FALSE) {
    state <- tokenizer$state
    while (TRUE) switch(
        state$state,
        TOKENIZER_FIELD = {
            char <- next_character(state)

            if (is.null(char)) {
                transition(state, TOKENIZER_FINAL)
                return(list(
                    status = TOKENIZER_END_OF_FILE,
                    token = pop_token_or_skip(state, skip = skip)
                ))
            }
            if (char == tokenizer$column_delimiter) {
                transition(state, TOKENIZER_FIELD)
                return(list(
                    status = TOKENIZER_OK,
                    token = pop_token_or_skip(state, skip = skip)
                ))
            }
            if (char == tokenizer$row_delimiter) {
                transition(state, TOKENIZER_FIELD)
                return(list(
                    status = TOKENIZER_END_OF_ROW,
                    token = pop_token_or_skip(state, skip = skip)
                ))
            }
            if (is_whitespace(char)) {
                transition(state, TOKENIZER_FIELD)
                next
            }
            if (char == tokenizer$quote) {
                transition(state, TOKENIZER_QUOTED_FIELD)
                next
            }
            { # char is any other character
                transition(state, TOKENIZER_UNQUOTED_FIELD)
                append_value(state, char)
                next
            }
        },

        TOKENIZER_UNQUOTED_FIELD = {
            char <- next_character(state)

            if (is.null(char)) {
                transition(state, TOKENIZER_FINAL)
                return(list(
                    status = TOKENIZER_END_OF_FILE,
                    token = pop_token_or_skip(state, trim = TRUE, skip = skip)
                ))
            }
            if (char == tokenizer$column_delimiter) {
                transition(state, TOKENIZER_FIELD)
                return(list(
                    status = TOKENIZER_OK,
                    token = pop_token_or_skip(state, trim = TRUE, skip = skip)
                ))

            }
            if (char == tokenizer$row_delimiter) {
                transition(state, TOKENIZER_FIELD)
                return(list(
                    status = TOKENIZER_END_OF_ROW,
                    token = pop_token_or_skip(state, trim = TRUE, skip = skip)
                ))
            }
            { # char is any other character
                transition(state, TOKENIZER_UNQUOTED_FIELD)
                append_value(state, char)
                next
            }
        },

        TOKENIZER_QUOTED_FIELD = {
            char <- next_character(state)

            if (is.null(char)) {
                transition(state, TOKENIZER_CRASHED)
                return(list(
                    status = TOKENIZER_PARSE_ERROR,
                    token = pop_token_or_skip(state, skip = skip)
                ))
            }
            if(is_quote_escape(tokenizer, char)) {
                transition(state, TOKENIZER_QUOTE)
                next
            }
            if(is_escape(tokenizer, char)) {
                transition(state, TOKENIZER_ESCAPE)
                next
            }
            { # char is any other character
                transition(state, TOKENIZER_QUOTED_FIELD)
                append_value(state, char)
                next
            }
        },

        TOKENIZER_QUOTE = {
            char <- next_character(state)

            if (is.null(char)) {
                transition(state, TOKENIZER_CRASHED)
                return(list(
                    status = TOKENIZER_PARSE_ERROR,
                    token = pop_token_or_skip(state, skip = skip)
                ))
            }
            if (char == tokenizer$quote) {
                transition(state, TOKENIZER_QUOTED_FIELD)
                append_value(state, char)
                next
            }
            if (is_whitespace(char)) {
                transition(state, TOKENIZER_TRAILING)
                next
            }
            if (char == tokenizer$column_delimiter) {
                transition(state, TOKENIZER_FIELD)
                return(list(
                    status = TOKENIZER_OK,
                    token = pop_token_or_skip(state, skip = skip)
                ))

            }
            if (char == tokenizer$row_delimiter) {
                transition(state, TOKENIZER_FIELD)
                return(list(
                    status = TOKENIZER_END_OF_ROW,
                    token = pop_token_or_skip(state, skip = skip)
                ))
            }
            { # char is any other character
                transition(state, TOKENIZER_CRASHED)
                return(list(
                    status = TOKENIZER_PARSE_ERROR,
                    token = pop_token_or_skip(state, skip = skip)
                ))
            }
        },
        TOKENIZER_TRAILING = {
            char <- next_character(state)

            if (is.null(char)) {
                transition(state, TOKENIZER_FINAL)
                return(list(
                    status = TOKENIZER_END_OF_FILE,
                    token = pop_token_or_skip(state, skip = skip)
                ))
            }
            if (is_whitespace(char)) {
                transition(state, TOKENIZER_TRAILING)
                next
            }
            if (char == tokenizer$column_delimiter) {
                transition(state, TOKENIZER_FIELD)
                return(list(
                    status = TOKENIZER_OK,
                    token = pop_token_or_skip(state, skip = skip)
                ))

            }
            if (char == tokenizer$row_delimiter) {
                transition(state, TOKENIZER_FIELD)
                return(list(
                    status = TOKENIZER_END_OF_ROW,
                    token = pop_token_or_skip(state, skip = skip)
                ))
            }
            { # char is any other character
                transition(state, TOKENIZER_CRASHED)
                return(list(
                    status = TOKENIZER_PARSE_ERROR,
                    token = pop_token_or_skip(state, skip = skip)
                ))
            }
        },
        TOKENIZER_ESCAPE = {
            char <- next_character(state)

            if (is.null(char)) {
                transition(state, TOKENIZER_CRASHED)
                return(list(
                    status = TOKENIZER_PARSE_ERROR,
                    token = pop_token_or_skip(state, skip = skip)
                ))
            }
            if (char == tokenizer$escape) {
                transition(state, TOKENIZER_QUOTED_FIELD)
                append_value(state, char)
                next
            }
            { # char is any other character
                transition(state, TOKENIZER_QUOTED_FIELD)
                append_value(state, tokenizer$escape)
                append_value(state, char)
                next
            }
        },
        TOKENIZER_FINAL = {
            stop("Error: Tokenizer completed reading this file")
        },
        TOKENIZER_CRASHED = {
            stop("Error: Tokenizer crashed and cannot continue")
        }
    ) # switch
} # function

is_whitespace <- function(char) char == ' ' || char == '\t'
is_escape <- function(tokenizer, char) char == tokenizer$escape && tokenizer$use_escape_character
is_quote_escape <- function(tokenizer, char) char == tokenizer$quote && tokenizer$use_double_quote_escape

is_final_state <- function(result) result$status == TOKENIZER_END_OF_FILE || result$status == TOKENIZER_ERROR || result$status == TOKENIZER_PARSE_ERROR
is_error_state <- function(result) result$status == TOKENIZER_ERROR || result$status == TOKENIZER_PARSE_ERROR

test <- function() {
    tokenizer <- new_tokenizer("test.csv", 0, 100, 100)
    tokens <- character(0)
    browser()
    while (TRUE) {
        result <- next_token(tokenizer)
        print(result)
        tokens <- c(tokens, unclass(result$token))
        if (is_final_state(result)) {
            break
        }
    }

    tokens
}

# Given an interval of N, a record like i -> L says that in order to read row
# number (i * N) of the CSV file, you must seek the CSV file to L bytes from
# start to start readingthat specific row.
#
# Because this is R, we will index this streuct starting from 1. So for
# interval=100, interesting rows are 1, 101, 201, etc.
#
# So at index 1 there will be row index 1 -> offset 0? or one?
#    at index 2 there will be row index 101 -> offset something
#    etc.
new_offset_record <- function(interval) {
    record <- new.env()
    record$row_interval = interval
    record$file_offsets = integer(0)
    class(record) <- c("buffer", "offset_buffer")
    record
}

interesting_row_number <- function(record, row_number) 0 == ((row_number - 1) %% record$row_interval)

add_next_offset <- function(record, file_offset) {
    record$file_offsets = append(record$file_offsets, file_offset)
}

add_next_offset_if_interesting <- function(record, row_number, file_offset) {
    if (0 == ((row_number - 1) %% record$row_interval)) {
        # print(paste0("interesting: ", row_number, "->", file_offset))
        record$file_offsets = append(record$file_offsets, file_offset)
    }
}


row_index_at <- function(record, record_index) {
    (record_index * (record$row_interval)) + 1
}

offset_closest_to_this_row <- function(record, row_index) {
    target_index <- floor(row_index / record$row_interval) # TODO index might be wrong.
    print(target_index)
    list (
        file_offset = record$file_offsets[target_index + 1],
        row_at_offset = (target_index * (record$row_interval)) + 1
    )
}

#' @export 
ufo_csv_scan <-  function(path, initial_offset, token_buffer_size, character_buffer_size) {
    tokenizer <- new_tokenizer(path, initial_offset, token_buffer_size, character_buffer_size)

}

test <- function() {
    r <- new_offset_record(100)
    for (i in 1:10000) {
        if (interesting_row_number(r, i)) {
            add_next_offset_if_interesting(r, i, -i)            
            print(i)
        }
    }
    r
}