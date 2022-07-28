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

new_tokenizer_from_scan_info <- function(scan_info, initial_offset) {
    new_tokenizer(
        path = attributes(scan_info)$path,
        token_buffer_size = attributes(scan_info)$token_buffer_size,
        character_buffer_size = attributes(scan_info)$character_buffer_size,
        initial_offset = initial_offset
    )
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

#TOKENIZER_RESULT      <- as.factor(c("OK", "END OF ROW", "END OF FILE", "PARSE ERROR", "ERROR"))
TOKENIZER_OK          <- as.integer(1) #TOKENIZER_RESULT[1]
TOKENIZER_END_OF_ROW  <- as.integer(2) #TOKENIZER_RESULT[2]
TOKENIZER_END_OF_FILE <- as.integer(3) #TOKENIZER_RESULT[3]
# TOKENIZER_PARSE_ERROR <- TOKENIZER_RESULT[4]
# TOKENIZER_ERROR       <- TOKENIZER_RESULT[5]

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
                stop("Tokenizer encountered the end of file",
                     " while parsing a quoted field")
                # return(list(
                #     status = TOKENIZER_PARSE_ERROR,
                #     token = pop_token_or_skip(state, skip = skip)
                # ))
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
                stop("Tokenizer encountered the end of file",
                     " while parsing an escaped quote")
                # return(list(
                #     status = TOKENIZER_PARSE_ERROR,
                #     token = pop_token_or_skip(state, skip = skip)
                # ))
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
                stop("Tokenizer encountered invalid character ", char, 
                     " while parsing an escaped quote")
                # return(list(
                #     status = TOKENIZER_PARSE_ERROR,
                #     token = pop_token_or_skip(state, skip = skip)
                # ))
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
                stop("Tokenizer encountered invalid character ", char,
                     " while parsing trailing characters after a field")
                # return(list(
                #     status = TOKENIZER_PARSE_ERROR,
                #     token = pop_token_or_skip(state, skip = skip)
                # ))
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
            stop("Tokenizer already completed reading this file and cannot continue")
        },
        TOKENIZER_CRASHED = {
            stop("Tokenizer crashed and cannot continue")
        }
    ) # switch
} # function

is_whitespace <- function(char) char == ' ' || char == '\t'
is_escape <- function(tokenizer, char) char == tokenizer$escape && tokenizer$use_escape_character
is_quote_escape <- function(tokenizer, char) char == tokenizer$quote && tokenizer$use_double_quote_escape

is_final_state <- function(result) result$status == TOKENIZER_END_OF_FILE # || result$status == TOKENIZER_ERROR || result$status == TOKENIZER_PARSE_ERROR
# is_error_state <- function(result) result$status == TOKENIZER_ERROR || result$status == TOKENIZER_PARSE_ERROR

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

# Token types
TOKEN_EMPTY      <- 0
TOKEN_NA         <- 2
TOKEN_LOGICAL    <- 4
TOKEN_INTEGER    <- 8
TOKEN_NUMERIC    <- 16
TOKEN_CHARACTER  <- 32

to_typed_token <- function(token) {
    # Assuming we got a legal string of len 1, otherwise: 
    # is.null(token) || is.na(token) || length(token) == 0

    if (token == "") {
        attributes(token)$type <- TOKEN_EMPTY
        # attributes(token)$typed_value <- NULL # redundant
        return(token)
    }

    if (is.na(token) || token == "NA") {
        attributes(token)$type <- TOKEN_NA
        attributes(token)$typed_value <- NA
        return(token)
    }

    maybe_logical <- suppressWarnings(as.logical(token))
    if (!is.na(maybe_logical)) {
        attributes(token)$type <- TOKEN_LOGICAL
        attributes(token)$typed_value <- maybe_logical
        return(token)
    }

    maybe_numeric <- suppressWarnings(as.numeric(token))
    if (!is.na(maybe_numeric)) {
        if (abs(maybe_numeric - round(maybe_numeric)) < (.Machine$double.eps^0.5)){
            attributes(token)$type <- TOKEN_INTEGER
            attributes(token)$typed_value <- as.integer(maybe_numeric)
            return(token)
        } else {
            attributes(token)$type <- TOKEN_NUMERIC
            attributes(token)$typed_value <- maybe_numeric
            return(token)
        }
    }

    attributes(token)$type <- TOKEN_CHARACTER
    attributes(token)$typed_value <- as.character(token)
    return(token)
}

unify_types <- function(a, b) {
    if (is.na(a)) return(b)
    if (is.na(b)) return(a)
    if (a > b)  a else b
}

CHARACTER_NA_VALUES = c("", "NA")

untyped_token_to_typed_value <- function(type, token) {
    if (type == TOKEN_EMPTY)     return(NA)
    if (type == TOKEN_NA)        return(NA)
    if (type == TOKEN_LOGICAL)   return(as.logical(token))
    if (type == TOKEN_INTEGER)   return(as.integer(token))
    if (type == TOKEN_NUMERIC)   return(as.numeric(token))
    if (type == TOKEN_CHARACTER) return(if (token %in% CHARACTER_NA_VALUES) as.character(NA) else as.character(token))
    stop("unreachable")
}

new_typed_value_vector <- function(type, n) {
    if (type == TOKEN_EMPTY)     return(rep(NA, n))
    if (type == TOKEN_NA)        return(rep(NA, n))
    if (type == TOKEN_LOGICAL)   return(rep(as.logical(NA), n))
    if (type == TOKEN_INTEGER)   return(rep(as.integer(NA), n))
    if (type == TOKEN_NUMERIC)   return(rep(as.numeric(NA), n))
    if (type == TOKEN_CHARACTER) return(rep(NA, n))
    stop("unreachable")
}

token_type_to_mode <- function(type) {
    if (type == TOKEN_EMPTY)     return("logical")
    if (type == TOKEN_NA)        return("logical")
    if (type == TOKEN_LOGICAL)   return("logical")
    if (type == TOKEN_INTEGER)   return("integer")
    if (type == TOKEN_NUMERIC)   return("numeric")
    if (type == TOKEN_CHARACTER) return("character")
    stop("unreachable")
}

#' @export 
ufo_csv_scan <- function(path, offset_interval, header, token_buffer_size, character_buffer_size) {
    tokenizer <- new_tokenizer(path, as.integer(0), token_buffer_size, character_buffer_size)
    offset_record <- new_offset_record(offset_interval)

    column_names <- NA
    column_types <- NA
    # distinct_values <- NA

    column <- as.integer(1)
    row <- as.integer(1)
    max_columns <- as.integer(1)

    construct_result <- function() {
        stretched_column_names <- character(max_columns)
        stretched_column_names[1:length(column_names)] <- column_names

        stretched_column_types <- integer(max_columns)
        stretched_column_types[1:length(column_types)] <- column_types

        data <- data.frame(
            column_names = stretched_column_names,
            column_types = stretched_column_types
        )

        attributes(data)$path = path
        attributes(data)$column_count = max_columns
        attributes(data)$row_count = row
        attributes(data)$header = header
        attributes(data)$token_buffer_size = token_buffer_size
        attributes(data)$character_buffer_size = character_buffer_size
        attributes(data)$offset_record = offset_record
        
        class(data) <- "scan_info"
        
        data
    }

    # header
    if (header) {
        parse_header <- TRUE
        while (parse_header) {
            result <- next_token(tokenizer)

            if (result$token == "") {
                column_names[column] <- NA
            } else {
                column_names[column] <- result$token
            }

            switch(result$status,
                TOKENIZER_OK = {
                    column <- column + 1
                },
                TOKENIZER_END_OF_ROW = {
                    max_columns <- column
                    column <- 1
                    # row <- row + 1;
                    parse_header <- FALSE
                },
                TOKENIZER_END_OF_FILE = {
                    return(construct_result())
                }
            )
        }
    }

    # body
    while (TRUE) {
        result <- next_token(tokenizer)

        if (is.na(column_types[column]) || column_types[column] != TOKEN_CHARACTER) {
            token <- to_typed_token(result$token)
            column_types[column] <- unify_types(attributes(token)$type, column_types[column])
        }

        if (column == 1) {
            offset <- attributes(result$token)$position_start
            add_next_offset_if_interesting(offset_record, row, offset)
        }
 
        switch(result$status,
            TOKENIZER_OK = {
                column <- column + 1
            },
            TOKENIZER_END_OF_ROW = {
                max_columns <- max(column, max_columns)
                column <- 1
                row <- row + 1
            },
            TOKENIZER_END_OF_FILE = {
                return(construct_result())
            }
        )
    }
}

#' @export
ufo_csv_read_column <- function(scan_info, target_column, start_from_row = 1, end_at_row) {
    if (attributes(scan_info)$row_count == 0) {
        stop("unimplemented")
    }

    if (missing(end_at_row)) {
        end_at_row <- attributes(scan_info)$row_count
    }

    column_type <- scan_info$column_types[target_column]
    target_rows <- end_at_row - start_from_row + 1
    values <- new_typed_value_vector(column_type, target_rows)
    
    offset_info <- offset_closest_to_this_row(attributes(scan_info)$offset_record, start_from_row)
    # seek to offset_info$file_offset
    # skip the rows between offset_info$row_at_offset and start_from_row
    cat(paste0("Start from offset: ", offset_info$file_offset, " at row: ", offset_info$row_at_offset, "\n"))

    tokenizer <- new_tokenizer_from_scan_info(scan_info, offset_info$file_offset)

    row <- offset_info$row_at_offset
    column <- 1

    while (end_at_row >= row) {

        #print(paste0("row: ", row, " column: ", column, " targeted: ", target_column, " -> ", column == target_column))
        result <- next_token(tokenizer)
        #print(paste0("token: ", result))

        if (column == target_column) {
            typed_value = untyped_token_to_typed_value(column_type, result$token)
            values[row - start_from_row] <- typed_value
        }

        #print(paste0("values: ", values))

        switch(result$status,
            TOKENIZER_OK = {
                column <- column + 1
            },
            TOKENIZER_END_OF_ROW = {
                column <- 1
                row <- row + 1
            },
            TOKENIZER_END_OF_FILE = {
                break
            }
        )
    }

    return(values)
}

test <- function(n=1,s=1, e=5) {
    # browser()
    scan_info <- ufo_csv_scan(path="test.csv", offset_interval=10, header=T, token_buffer_size=100, character_buffer_size=100)
    print(scan_info)
    ufo_csv_read_column(scan_info, target_column = n, start_from_row = s, end_at_row = e)
    # print(ufo_csv_read_column(scan_info, target_column = 2))
    # print(ufo_csv_read_column(scan_info, target_column = 3))
    # print(ufo_csv_read_column(scan_info, target_column = 4))
    # print(ufo_csv_read_column(scan_info, target_column = 5))
}


read_csv <- function (path, header=TRUE, offset_interval = 1000, token_buffer_size = 4000, character_buffer_size = 4000) {
    scan_info <- ufo_csv_scan(path, offset_interval = offset_interval, header = header, token_buffer_size = token_buffer_size, character_buffer_size = character_buffer_size)

    names <- sapply(1:length(scan_info$column_names), function(i) {
        if (is.na(scan_info$column_names[i])) {
            paste0("column.", i) 
        } else {
            paste0(unlist(strsplit(scan_info$column_names[i], " ")), collapse=".")
        }
    })

    columns <- lapply(1:length(scan_info$column_names), function(i) {
        ufo_csv_read_column(scan_info, target_column = i)
    })
    names(columns) <- scan_info$names
    
    csv <- do.call(data.frame, columns)
    return(csv)
}

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
ufo_csv <- function(path, header=TRUE, offset_interval = 1000, token_buffer_size = 4000, character_buffer_size = 4000, ...) {
    scan_info <- ufo_csv_scan(path, offset_interval = offset_interval, header = header, token_buffer_size = token_buffer_size, character_buffer_size = character_buffer_size)

    names <- sapply(1:length(scan_info$column_names), function(i) {
        if (is.na(scan_info$column_names[i])) {
            paste0("column.", i)
        } else {
            paste0(unlist(strsplit(scan_info$column_names[i], " ")), collapse=".")
        }
    })

    columns <- lapply(1:length(scan_info$column_names), function(i) {
        column_type <- scan_info$column_types[i]
        column_mode <- token_type_to_mode(column_type)
        row_count <- attributes(scan_info)$row_count

        # small optimization, we don't actually need to read these from disk
        if (column_type == TOKEN_EMPTY || column_type == TOKEN_NA) {
            return(ufo_logical(length = row_count, initial_value = NA))
        }

        populate <- function(start, end, scan_info, target_column, ...) {
            ufo_csv_read_column(
                scan_info = scan_info,
                target_column = target_column,
                start_from_row = start,
                end_at_row = end
            )
        }

        ufo_vector_constructor(
            mode = column_mode,
            length = row_count,
            populate = populate,
            scan_info = scan_info,
            target_column = i,
            ...
        )
    })

    names(columns) <- names
    columns
}
