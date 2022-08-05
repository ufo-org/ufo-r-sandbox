# Always used explicitly
# library(DBI)
# library(RSQLite)

sqlite_table_columns <- function(db, table, ...)  {
    connection <- do.call(DBI::dbConnect, c(drv = RSQLite::SQLite(), db, ...))
    result <- DBI::dbSendQuery(connection, paste0("PRAGMA table_info(", DBI::dbQuoteIdentifier(connection, table), ")"))
    columns <- DBI::dbFetch(result)
    DBI::dbClearResult(result)
    print(columns)
    columns$name
}

sqlite_column_type <- function(db, table, column, ...)  {
    connection <- do.call(DBI::dbConnect, c(drv = RSQLite::SQLite(), db, ...))
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

sqlite_column_length <- function(db, table, column, ...)  {
    connection <- do.call(DBI::dbConnect, c(drv = RSQLite::SQLite(), db, ...))
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

# Start and end are zero-indexed
sqlite_populate_vector <- function(db, start, end, table, column, constructor, converter, ...) {

    # print(paste0("db:          ", db))
    # print(paste0("start:       ", start))
    # print(paste0("end:         ", end))
    # print(paste0("table:       ", table))
    # print(paste0("column:      ", column))
    # print(paste0("constructor: ", constructor))
    # print(paste0("converter:   ", converter))
    # print(paste0("...:         ", list(...)))

    connection <- do.call(DBI::dbConnect, c(drv = RSQLite::SQLite(), db, ...))

    result <- DBI::dbSendQuery(connection, paste0(
        "SELECT", DBI::dbQuoteIdentifier(connection, column), "FROM (", 
            "SELECT ROW_NUMBER() OVER(ORDER BY ROWID) __ufo_index, ",   # ROWID is 1-indexed
                DBI::dbQuoteIdentifier(connection, column), 
            " FROM ", DBI::dbQuoteIdentifier(connection, table), 
        ") WHERE __ufo_index > ", start, " AND  __ufo_index <= ", end)) # conversion from 0-index to 1-index via these conditions

    output <- constructor()
    cursor <- as.integer(0)

    while (!DBI::dbHasCompleted(result)) {
        data <- DBI::dbFetch(result)
        end_position <- cursor + as.integer(nrow(data))
        output[cursor:end_position] <- converter(data[, column])
    }

    # print("output:")
    # print(output)

    DBI::dbClearResult(result)
    output
}

sqlite_populate_integer <- function(db, start, end, table, column, ...) {
    sqlite_populate_vector(
        db=db, start=start, end=end, 
        table=table, column=column, 
        constructor=integer, converter=as.integer,
        ...
    )
}

sqlite_populate_character <- function(db, start, end, table, column, ...) {
    sqlite_populate_vector(
        db=db, start=start, end=end, 
        table=table, column=column, 
        constructor=character, converter=as.character,
        ...
    )
}

sqlite_populate_numeric <- function(db, start, end, table, column, ...) {
    sqlite_populate_vector(
        db=db, start=start, end=end, 
        table=table, column=column, 
        constructor=numeric, converter=as.numeric,
        ...
    )
}

sqlite_populate_raw <- function(db, start, end, table, column, ...) {
    sqlite_populate_vector(
        db=db, start=start, end=end, 
        table=table, column=column, 
        constructor=raw, converter=as.raw,
        ...
    )
}

sqlite_populate_logical <- function(db, start, end, table, column, ...) {
    sqlite_populate_vector(
        db=db, start=start, end=end, 
        table=table, column=column, 
        constructor=logical, converter=as.logical,
        ...
    )
}

#' Creates a UFO object of representing a column from an SQL database.
#' @param db database connection information
#' @param table the name of the table in the database
#' @param column the name of the column in the database
#' @param driver a string describing the database driver, one of: SQLite
#' @param read_only sets the vector to be write-protected by the OS
#'                  (optional, false by default).
#' @param chunk_length the minimum number of elements loaded at once,
#'                     will always be rounded up to a full memory page
#'                     (optional, a page by default).
#' @param ... other ufo or db configuration options
#' @return a ufo vector lazily populated with the values of the specified
#'         column
#' @export
ufo_sql_column <- function(db, table, column, ..., driver = "SQLite") {

    column_type <- NULL
    column_length <- NULL

    if (driver == "SQLite" || driver == "SQLITE" || driver == "sqlite") {
        column_type   <- sqlite_column_type(db=db, table=table, column=column, ...)
        column_length <- sqlite_column_length(db=db, table=table, column=column, ...)
    } else {
        stop(paste0("Unsupported database driver: ", driver, ". ",
                    "Use one of: SQLite"))
    }

    populate <- if (column_type == "integer") sqlite_populate_integer
        else if (column_type == "numeric") sqlite_populate_numeric
        else if (column_type == "raw") sqlite_populate_raw
        else if (column_type == "character") sqlite_populate_character
        else if (column_type == "logical") sqlite_populate_logical
        else { 
            stop("Column \"", column, "\" from table \"", table,"\" has type ",
                 type, " which cannot be represented as a UFO. ",
                 "It has to be one of: integer, numeric, raw, character, "
                 "or logical.")
        }

    # populate(db=db, start=1, end=column_length + 1, table=table, column=column, ...)

    ufo_vector_constructor(mode = column_type, length = column_length,
               populate = populate, 
               db = db, table = table, column = column,
               ...
    )
}

#' Creates a UFO object representing a table from an SQL database. 
#' @param db database connection information
#' @param table the name of the table in the database
#' @param driver a string describing the database driver, one of: SQLite
#' @param read_only sets the vector to be write-protected by the OS
#'                  (optional, false by default).
#' @param chunk_length the minimum number of elements loaded at once,
#'                     will always be rounded up to a full memory page
#'                     (optional, a page by default).
#' @param ... other ufo or db configuration options
#' @return a list containig ufo vectors lazily populated with the values
#'         of individual columns in the specified table 
#' @export
ufo_sql_table <- function(db, table, ..., driver = "SQLite") {

    columns <- NULL

    if (driver == "SQLite" || driver == "SQLITE" || driver == "sqlite") {
        columns <- sqlite_table_columns(db, table, ...)
    } else {
        stop(paste0("Unsupported database driver: ", driver, ". Use one of: SQLite"))
    }

    result <- lapply(columns, function(column) ufo_sql_column(db, table, column, ..., driver))
    names(result) <- columns
}