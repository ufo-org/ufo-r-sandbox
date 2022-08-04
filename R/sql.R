# Always used explicitly
# library(DBI)
# library(RSQLite)

sqlite_column_type <- function(table, column, ...)  {
    connection <- DBI::dbConnect(drv = RSQLite::SQLite(), ...)
    result <- DBI::dbSendQuery(connection, paste0("PRAGMA table_info(", DBI::dbQuoteIdentifier(connection, table), ")"))
    columns <- DBI::dbFetch(result)
    DBI::dbClearResult(result)

    one_column <- columns[columns$name == column, ]
    if (nrow(one_column) == 0) {
        stop("Column \"", column, "\" not found in table \"", table, "\"")
    }
    if (nrow(one_column) != 1) {
        stop("Search for column \"", column, "\" in table \"", table, "\" returned ", nrow(one_column), " results (expoected one)")
    }
    
    if (one_column$type == "INTEGER") return("integer")
    if (one_column$type == "REAL")    return("numeric")
    if (one_column$type == "TEXT")    return("character")
    if (one_column$type == "BLOB")    return("raw")

    stop("Column \"", column, "\" from table \"", table,"\" has type ", one_column$type, " which cannot be represented as an R vector")
}

sqlite_column_length <- function(table, column, ...)  {
    connection <- DBI::dbConnect(drv = RSQLite::SQLite(), ...)
    result <- DBI::dbSendQuery(connection, paste0("SELECT COUNT(*) FROM ", DBI::dbQuoteIdentifier(connection, table), ""))
    column_length <- DBI::dbFetch(result)
    DBI::dbClearResult(result)

    if (nrow(column_length) == 0) {
        stop("Column \"", column, "\" not found in table \"", table, "\"")
    }
    if (nrow(column_length) != 1) {
        stop("Query for length of column \"", column, "\" in table \"", table, "\" returned ", nrow(column_length), " results (expected one)")
    }

    column_length$`COUNT(*)`
}

sqlite_select_values <- function(start, end, table, column, ...) {
    connection <- DBI::dbConnect(drv = RSQLite::SQLite(), ...)

    DBI::dbSendQuery(connection, paste0(
        "SELECT", DBI::dbQuoteIdentifier(connection, column), "FROM (", 
            "SELECT ROW_NUMBER() OVER(ORDER BY ROWID) __ufo_index, ", 
                DBI::dbQuoteIdentifier(connection, column), 
            " FROM ", DBI::dbQuoteIdentifier(connection, table), 
        ") WHERE __ufo_index >= ", start, " AND  __ufo_index < ", end))   
}

sqlite_select_integer_values <- function(start, end, table, column, ...) {
    result <- sqlite_select_values(start=start, end=end, table=table, column=column, ...)

    output <- integer(end - start)
    cursor <- as.integer(0)

    while (!DBI::dbHasCompleted(result)) {
        data <- DBI::dbFetch(result)
        end_position <- cursor + as.integer(nrow(data))
        output[cursor:end_position] <- as.integer(data[, column])
    }

    DBI::dbClearResult(result)
    output
}


#' Creates a UFO object of representing a column from an SQL database
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
#'         column
#' @export
ufo_sql_db_column <- function(table, column, driver = "SQLite", chunk_length = 0, ...) {

    column_type <- NULL
    column_length <- NULL

    if (driver == "SQLite" || driver == "SQLITE" || driver == "sqlite") {
        column_type   <- sqlite_column_type(table = table, column = column, ...)
        column_length <- sqlite_column_length(table = table, column = column, ...)
    } else {
        stop(paste0("Unsupported database driver: ", db_type, ". Use one of: SQLite"))
    }

    

    ufo_vector_constructor(mode = column_type, length = column_length, ...)
               populate = populate, 
               #writeback = writeback,
               #reset = reset, destroy = destroy,
               #finalizer = finalizer, read_only = read_only,
               chunk_length = chunk_length, ...)
    )


    # ufo_integer_constructor(
    #     length = (to - from) / by + 1,
    #     populate = function(start, end, from, by, ...) {
    #         as.numeric(seq.int(
    #             from = from + (start - 1) * by,
    #             to   = from + (end   - 1) * by,
    #             by   = by
    #         ))
    #     },
    #     ...
    # )
}