# user_data <- list(from=0, by=1, to=20);
populate_integer_seq <- function(start, end, from, by, ...) {
    seq.int(
        from = from + (start - 1) * by,
        to   = from + (end   - 1) * by,
        by   = by
    )
}

# user_data <- list(from=0, by=1, to=20);
populate_numeric_seq <- function(start, end, from, by, ...) {
    seq(
        from = from + (start - 1) * by,
        to   = from + (end   - 1) * by,
        by   = by
    )
}

populate_integer <- function(start, end, size, ...) {
    integer(size)
}

populate_numeric <- function(start, end, size, ...) {
    numeric(size)
}

populate_integer_bin <- function(start, end, path, ...) {
    source <- file(path, open="rb", raw=FALSE)
    seek(source, origin="start", where=start)
    vector <- readBin(source, what="integer", n=end-start)
    close(source)
    vector
}

populate_numeric_bin <- function(start, end, path, ...) {
    source <- file(path, open="rb", raw=FALSE)
    seek(source, origin="start", where=start)
    vector <- readBin(source, what="numeric", n=end-start)
    close(source)
    vector
}

populate_psql <- function(start, end, user, host, port, database, table, column, ...) { # TODO: open connection beforehand
    connection <- DBI::dbConnect(
        RPostgres::Postgres(),
        dbname = database,
        host = host, port = port,
        user = user, password = pw
    )

    query <- DBI::dbSendQuery(connection,
        "SELECT ", column, " ",
        "FROM ufo_", table, "_", column, "_subscript ",
        "WHERE nth >= ", start, " AND nth < ", end
    )

    result <- DBI::dbFetch(query)
    DBI::dbClearResult(query)
    DBI::dbDisconnect(connection)

    result
}

call_function <- function(f, user_data, ...) {
    do.call(f, c(list(...), user_data))
}

# 0 2 4 6 8 10 12 14 26 18 20
# 1 2 3 4 5 6  7  8  9  10 11
#     ^^^^^^^^^^^