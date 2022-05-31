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

.new_raw_buffer <- function(max_size, values = raw(0)) { # -> env class:buffer
    buffer <- new.env()
    buffer$values = as.raw(values)
    buffer$max_size = max_size # no checks, just informative
    buffer$size = as.integer(length(buffer$values))
    buffer$pointer = as.integer(1)
    class(buffer) <- c("buffer", "raw_buffer")
    buffer
}

.next_raw <- function(buffer) { # -> NULL | raw(..)
    if (buffer$pointer == buffer$size + 1) {
        NULL
    } else {
        result <- buffer$values[buffer$pointer]
        buffer$pointer = buffer$pointer + 1
        result
    }
}

.has_next_raw <- function(buffer) {
    !(buffer$pointer >= buffer$size + 1)
}

.fill_raw <- function(buffer, values) {
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

.new_token_buffer <- function(max_size, values=raw(max_size)) {
    buffer <- new.env()
    buffer$values = as.raw(values)
    buffer$max_size = max_size
    buffer$size = as.integer(0)
    class(buffer) <- c("buffer", "token_buffer")
    buffer
}

.append_token_buffer <- function(buffer, value) {
    if (buffer$size == buffer$max_size) {
        buffer$max_size = buffer$max_size * 2
        new_values <- raw(buffer$max_size)
        new_values[1:length(buffer$values)] <- buffer$values
        buffer$values = new_values
    }

    buffer$size = buffer$size + 1
    buffer$values[buffer$size] = value
}

.get_token_buffer <- function(buffer) {
    print(buffer)
    word <- buffer$values[1:buffer$size]
    word <- rawToChar(word)
    class(word) <- c(class(word), "token")
    word
}

.clear_token_buffer <- function(buffer) {
    buffer$size = as.integer(0)
}

TOKENIZER_INITIAL        <- as.integer(0)
TOKENIZER_FIELD          <- as.integer(1)
TOKENIZER_UNQUOTED_FIELD <- as.integer(2)
TOKENIZER_QUOTED_FIELD   <- as.integer(3)
TOKENIZER_QUOTE          <- as.integer(4)
TOKENIZER_TRAILING       <- as.integer(5)
TOKENIZER_ESCAPE         <- as.integer(6)
TOKENIZER_FINAL          <- as.integer(7)
TOKENIZER_CRASHED        <- as.integer(8)

.new_tokenizer_state <- function(path, initial_offset, token_buffer_size, character_buffer_size) {
    state <- new.env()
    state$state = TOKENIZER_INITIAL
    state$file = file(path, open = "rb")
    seek(state$file, initial_offset, origin = "start") 
    state$state = TOKENIZER_FIELD
    state$initial_offset = as.integer(initial_offset)
    state$read_buffer = .new_raw_buffer(character_buffer_size)
    state$token_buffer = .new_token_buffer(token_buffer_size)
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

.next_character <- function(state) { 
    if (!.has_next_raw(state$read_buffer)) {
        values <- readBin(state$file, raw(0), state$read_buffer$max_size)
        .fill_raw(state$read_buffer, values)
    }
    value = .next_raw(state$read_buffer)
    if (is.null(value)) {
        NULL
    } else {
        rawToChar(value)
    }
}

.pop_token <- function(state, trim = FALSE) {    
    token <- .get_token_buffer(state$token_buffer)

    attributes(token)$position_start = state$end_of_last_token
    state$end_of_last_token = state$current_offset

    attributes(token)$position_end = state$current_offset    

    .clear_token_buffer(s$token_buffer)

    if (trim) {
        trimws(token, which = "right")
    } else {
        token
    }
}

.skip_token <- function(state) {    
    .clear_token_buffer(s$token_buffer)    
    NULL
}

.new_tokenizer <- function() {
    tokenizer <- new.env()
    
    tokenizer$row_delimiter = '\n'
    tokenizer$column_delimiter = ','
    tokenizer$escape = '\\'
    tokenizer$quote = '"'
    tokenizer$use_double_quote_escape = TRUE
    tokenizer$use_escape_character = TRUE
    class(tokenizer) <- "tokenizer"

    tokenizer
}

.transition <- function(state, next_state) {
    state$state = next_state
    state$current_offset = state$current_offset + 1
}

.pop_token_or_skip <- function(state, skip = FALSE, trim = FALSE) {
    if (skip) {
        .skip_token(state)
    } else {
        .pop_token(state, trim)
    }
}

.with_exit_status <- function(status, token) {
    list(status=status, token=token)
}

.append <- function(status, value) {
    .append_token_buffer(status$token_buffer, value)
}


TOKENIZER_OK          = "OK"
TOKENIZER_END_OF_ROW  = "END OF ROW"
TOKENIZER_END_OF_FILE = "END OF FILE"
TOKENIZER_PARSE_ERROR = "PARSE ERROR"
TOKENIZER_ERROR       = "ERROR"

.next_token <- function(tokenizer, state, skip) {
    while (true) switch(
        state$state,
        TOKENIZER_FIELD = {
            char <- .next_character(state)
            
            if (is.null(char)) { 
                .transition(state, TOKENIZER_FINAL)
                return(list(
                    status=TOKENIZER_END_OF_FILE, 
                    token=.pop_token_or_skip(state, skip=skip) 
                ))
            }
            if (char == tokenizer$column_delimiter) { 
                .transition(state, TOKENIZER_FIELD)
                return(list(
                    status=TOKENIZER_OK, 
                    token=.pop_token_or_skip(state, skip=skip)
                ))
            }
            if (char == tokenizer$row_delimiter) { 
                .transition(state, TOKENIZER_FIELD)
                return(list(
                    status=TOKENIZER_END_OF_ROW,
                    token=.pop_token_or_skip(state, skip=skip)
                ))
            }
            if (is_whitespace(char)) {
                .transition(state, TOKENIZER_FIELD)
                continue
            }
            if (char == tokenizer$quote) {
                .transition(state, TOKENIZER_QUOTED_FIELD)
                continue
            }
            { # char is any other character
                .transition(state, TOKENIZER_UNQUOTED_FIELD)
                .append(state, char)
                continue
            }                                                    
        },

        TOKENIZER_UNQUOTED_FIELD = {
            char <- .next_character(state)

            if (is.null(char)) {
                .transition(state, TOKENIZER_FINAL)
                return(list(
                    status=TOKENIZER_END_OF_FILE,                    
                    token=.pop_token_or_skip(state, trim=TRUE, skip=skip)                    
                ))
            }
            if (char == tokenizer$column_delimiter) {
                .transition(state, TOKENIZER_FIELD)
                return(list(
                    status=TOKENIZER_OK,                    
                    token=.pop_token_or_skip(state, trim=TRUE, skip=skip)                    
                ))

            }
            if (char == tokenizer$row_delimiter) {
                .transition(state, TOKENIZER_FIELD)
                return(list(
                    status=TOKENIZER_END_OF_ROW,                    
                    token=.pop_token_or_skip(state, trim=TRUE, skip=skip)                    
                ))
            }
            { # char is any other character
                .transition(state, TOKENIZER_UNQUOTED_FIELD)
                .append(state, char)
                continue
            }
        },

        TOKENIZER_QUOTED_FIELD = {
            char <- .next_character(state)
            
            if(is_quote_escape(tokenizer, char)) {
                .transition(state, TOKENIZER_QUOTE)
                continue
            }
            if(is_escape(tokenizer, char)) {
                .transition(state, TOKENIZER_ESCAPE)
                continue
            }
            if (is.null(char)) {
                .transition(state, TOKENIZER_CRASHED)
                return(list(
                    status=TOKENIZER_PARSE_ERROR,                    
                    token=.pop_token_or_skip(state, skip=skip)                    
                ))
            }
            { # char is any other character
                .transition(state, TOKENIZER_QUOTED_FIELD)
                .append(state, char)
                continue
            }
        },

        TOKENIZER_QUOTE = {
            char <- .next_character(state)

            if (char == tokenizer$quote) {
                .transition(state, TOKENIZER_QUOTED_FIELD)
                .append(state, char)
                continue
            }
            if (is.null(char)) {
                .transition(state, TOKENIZER_CRASHED)
                return(list(
                    status=TOKENIZER_PARSE_ERROR,                    
                    token=.pop_token_or_skip(state, skip=skip)                    
                ))
            }
            if (is_whitespace(char)) {
                .transition(state, TOKENIZER_TRAILING)
                continue
            }
            if (char == tokenizer$column_delimiter) {
                .transition(state, TOKENIZER_FIELD)
                return(list(
                    status=TOKENIZER_OK,                    
                    token=.pop_token_or_skip(state, skip=skip)                    
                ))

            }
            if (char == tokenizer$row_delimiter) {
                .transition(state, TOKENIZER_FIELD)
                return(list(
                    status=TOKENIZER_END_OF_ROW,                    
                    token=.pop_token_or_skip(state, skip=skip)                    
                ))
            }
            { # char is any other character
                .transition(state, TOKENIZER_CRASHED)
                return(list(
                    status=TOKENIZER_PARSE_ERROR,                    
                    token=.pop_token_or_skip(state, skip=skip)                    
                ))
            }
        },
        TOKENIZER_TRAILING = {
            char <- .next_character(state)

            if (is_whitespace(char)) {
                .transition(state, TOKENIZER_TRAILING)
                continue
            }
            if (char == tokenizer$column_delimiter) {
                .transition(state, TOKENIZER_FIELD)
                return(list(
                    status=TOKENIZER_OK,                    
                    token=.pop_token_or_skip(state, skip=skip)                    
                ))

            }
            if (char == tokenizer$row_delimiter) {
                .transition(state, TOKENIZER_FIELD)
                return(list(
                    status=TOKENIZER_END_OF_ROW,                    
                    token=.pop_token_or_skip(state, skip=skip)                    
                ))
            }
            if (is.null(char)) {
                .transition(state, TOKENIZER_FINAL)
                return(list(
                    status=TOKENIZER_END_OF_FILE,                    
                    token=.pop_token_or_skip(state, skip=skip)                    
                ))
            }
            { # char is any other character
                .transition(state, TOKENIZER_CRASHED)
                return(list(
                    status=TOKENIZER_PARSE_ERROR,                    
                    token=.pop_token_or_skip(state, skip=skip)                    
                ))
            }
        },
        TOKENIZER_ESCAPE = {
            char <- .next_character(state)

            if (char == tokenizer$escape) {
                .transition(state, TOKENIZER_QUOTED_FIELD)
                .append(state, char)
                continue
            }
            if (is.null(char)) {
                .transition(state, TOKENIZER_CRASHED)
                return(list(
                    status=TOKENIZER_PARSE_ERROR,                    
                    token=.pop_token_or_skip(state, skip=skip)                    
                ))
            }
            { # char is any other character
                .transition(state, TOKENIZER_QUOTED_FIELD)
                .append(state, tokenizer$escape)
                .append(state, char)
                continue
            }
        },
        TOKENIZER_INITIAL = {
            stop("Error: Tokenizer was now properly opened")
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