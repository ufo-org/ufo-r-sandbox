# Based on code from matter::matter_str
open_indexed_files <- function(paths, offset, extent) {
    if (all(extent == 0)) 
        return(character(0))

    if (length(offset) != length(extent)) 
        stop("length of 'offset' [", length(offset), "] ",
             "must equal length of 'extent' [",
             length(extent), "]")

    paths <- normalizePath(paths, mustWork = FALSE)
    if (!file.exists(paths)) {
        # data <- rep(list(" "), length(extent))e
        # filemode <- force(filemode)
        result <- file.create(paths)
        if (!all(result))
            stop("error creating file(s)")
    }
}

#' Load a file from disk into an out-of-memory vector according to offsets.
#'
#' The elements of the vector are extracted from the file from the locations
#' delineated by a list of offsets and field lengths (extents).
#'
#' @export
ufo_mmap_character <- function(paths, offset, extent, writeback = FALSE, fill = " ", ...) {
    open_indexed_files(paths, offset, extent)

    if (length(paths) != length(extent) && length(paths) != 1)
        stop("Paths can be of the same length as extent (",
             extent, ") or of length 1, but it is: ", length(paths))

    print("construct!")
    ufo_character_constructor(
        length = length(offset),
        populate = ufo_mmap_character_populate,
        paths = paths,
        offsets = offset,
        extents = extent,
        fill = fill,
        writeback = if (writeback) ufo_mmap_character_writeback else NULL,
        ...
    )
}

ufo_mmap_character_populate <- function(start, end, paths, offsets, extents, ...) {
    which_file <- function(index) {
        ifelse(length(paths) == 1, paths, paths[index])
    }

    files <- unique(sapply((start+1):end, which_file))
    mmaps <- lapply(files, function(f) mmap::mmap(f, mode = mmap::char()))
    names(mmaps) <- files

    sapply((start+1):end, function(index) {
        data <- mmaps[[which_file(index)]]
        print("offset")
        print(offsets[index])
        print("extent")
        print(extents[index])

        span <- offsets[index]:(offsets[index]+extents[index])
        print("span")
        print(span)

        word <- mmap:::`[.mmap`(data, span)
        char <- rawToChar(word)
        print("char")
        print(char)
        char
    })
}

ufo_mmap_character_writeback <- function(start, end, data, paths, offsets, extents, fill, ...) {
    which_file <- function(index) {
        ifelse(length(paths) == 1, paths, paths[index])
    }

    files <- unique(sapply((start+1):end, which_file))
    mmaps <- lapply(files, function(f) mmap::mmap(f, mode = mmap::char()))
    names(mmaps) <- files

    for (index in (start+1:end)) {
        print(paste0("WRITEBACK!", index))

        map <- mmaps[[which_file(index)]]

        span <- offsets[index]:(offsets[index]+extents[index])
        print("span")
        print(span)

        current_length <- nchar(data[index])
        expected_length <- length(span)
        replacement_value <- paste0(data[index], paste(character(expected_length - current_length + 1), collapse=fill))

        print("replacing ")
        word <- mmap:::`[.mmap`(map, span)
        print(rawToChar(word))

        print("with")
        print(replacement_value)

        word <- mmap:::`[<-.mmap`(map, span, value=charToRaw(replacement_value))
        word <- mmap:::`[.mmap`(map, span)

        mmap:::msync(map)

        print("and now")
        print(rawToChar(word))

        NULL
    }
}