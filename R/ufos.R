# Core, universal, "low-level" UFO objects and functions.

#' Get a reference to the UFO core.
#' @param writeback_path location of directory where UFOs write modified
#'                       dematerialized data
#' @param high_water_mark maximum size of materialized UFO chunks (in MB)
#' @param low_water_mark size of materialized UFO chunks after GC (in MB)
#' @return An external pointer object with functions attached.
#' @export
ufo_system_start <-
  function(writeback_path, high_water_mark, low_water_mark) {

  cat("UFO_SYSTEM_GET_OR_CREATE\n");

  directory <- if (missing(writeback_path)) {
    getOption("ufos.writeback_path", default = "/tmp")
  } else {
    writeback_path
  }

  high_water_mark_mb <- if (missing(high_water_mark)) {
    getOption("ufos.high_water_mark_mb", default = 100)
  } else {
    high_water_mark
  }

  low_water_mark_mb <- if (missing(low_water_mark)) {
    getOption("ufos.low_water_mark_mb", default = 10)
  } else {
    low_water_mark
  }

  UfoSystem$initialize(
    as.character(directory),
    as.integer(high_water_mark_mb * 1024 * 1024),
    as.integer(low_water_mark_mb * 1024 * 1024)
  )
}

#' A UFO core singleton that is lazily loaded and hangs around until the
#' package is unloaded.
.ufo_core <- onet(initializer = ufo_system_start)

#' Kills UFO core. I refuse to rename it.
jeff_goldbloom <- function(...) {
  destroy(.ufo_core)
  invisible(NULL)
}

#' Shut the UFO core system down
#' @export
ufo_system_shutdown <- jeff_goldbloom;

#' Finalizer for the entire UFO framework, called on session exit.
.onLoad <- function(libname, pkgname) {
  reg.finalizer(.GlobalEnv, jeff_goldbloom, onexit = TRUE)
  invisible(NULL)
}

#' Finalizer for the entire UFO framework, called on package unload.
#' AFAIK this cannot be relied on to be called on shutdown.
.onUnload <- function(libname, pkgname) {
  jeff_goldbloom(libname = libname, pkgname = pkgname)
}

#' Produces a lazily populated UFO vector of the given length and mode.
#' The vector will be populated on access section by section. The vector
#' will garbage collect cvhunks in response to memory pressure, so it can
#' be larger-than-memory.
#' @param mode vector type, one of: vector, numeric (or double), integer,
#'             logical, character, complex, raw.
#' @param length number of elements in the vector, must be > 1.
#' @param populate a function used to generate data inside the vector,
#' @param writeback a function called when a modified chunk of the vector
#'                  is garbage collected (optional).
#' @param reset a function called when a modified chunk is reset discarding
#'              dirty, manually written values.
#' @param destroy a function called when a modified chunk is being destroyed
#' @param finalizer a function called when the entire vector is garbage
#'                  collected (optional).
#' @param read_only sets the vector to be write-protected by the OS
#'                  (optional, false by default).
#' @param chunk_length the minimum number of elements loaded at once,
#'                     will always be rounded up to a full memory page
#'                     (optional, a page by default).
#' @param ... user data passed to populate, writeback, and finalizer functions,
#' @return a lazily loaded vector of the specified type and length.
#' @export
ufo_vector_constructor <- function(mode, length, populate, 
                       writeback = NULL, reset = NULL, destroy = NULL, 
                       finalizer = NULL,
                       read_only = FALSE, chunk_length = 0, ...) {
    # compile <- function(f)
    #   if (is.function(f)) {
    #     compiler::cmpfun(f)
    #   } else {
    #     f
    #   }

    # populate <- compile(populate)
    # writeback <- compile(writeback)
    # finalizer <- compile(finalizer)
    # reset <- compile(reset)
    # destroy <- compile(destroy)

    system <- get_or_create(.ufo_core)
    system$new_ufo(mode = mode, length = length, user_data = list(...),
                   populate = populate, writeback = writeback,
                   reset = reset, destroy = destroy,
                   finalizer = finalizer, read_only = read_only,
                   chunk_length = chunk_length)
}

#' Produces a lazily populated UFO integer vector of the given length.
#' The vector will be populated on access section by section. The vector
#' will garbage collect cvhunks in response to memory pressure, so it can
#' be larger-than-memory.
#' @param length number of elements in the vector, must be > 1.
#' @param populate a function used to generate data inside the vector,
#' @param writeback a function called when a modified chunk of the vector
#'                  is garbage collected (optional).
#' @param reset a function called when a modified chunk is reset discarding
#'              dirty, manually written values.
#' @param destroy a function called when a modified chunk is being destroyed
#' @param finalizer a function called when the entire vector is garbage
#'                  collected (optional).
#' @param read_only sets the vector to be write-protected by the OS
#'                  (optional, false by default).
#' @param chunk_length the minimum number of elements loaded at once,
#'                     will always be rounded up to a full memory page
#'                     (optional, a page by default).
#' @param ... user data passed to populate, writeback, and finalizer functions,
#' @return a lazily loaded integer vector of the specified length.
#' @export
ufo_integer_constructor <- function(length, populate, writeback = NULL, reset = NULL, destroy = NULL, finalizer = NULL,
                        read_only = FALSE, chunk_length = 0, ...) {
    ufo_vector_constructor(mode = "integer", length = length,
               populate = populate, writeback = writeback,
               reset = reset, destroy = destroy,
               finalizer = finalizer, read_only = read_only,
               chunk_length = chunk_length, ...)
}

#' Produces a lazily populated UFO numeric vector of the given length.
#' The vector will be populated on access section by section. The vector
#' will garbage collect chunks in response to memory pressure, so it can
#' be larger-than-memory.
#' @param length number of elements in the vector, must be > 1.
#' @param populate a function used to generate data inside the vector,
#' @param writeback a function called when a modified chunk of the vector
#'                  is garbage collected (optional).
#' @param reset a function called when a modified chunk is reset discarding
#'              dirty, manually written values.
#' @param destroy a function called when a modified chunk is being destroyed
#' @param finalizer a function called when the entire vector is garbage
#'                  collected (optional).
#' @param read_only sets the vector to be write-protected by the OS
#'                  (optional, false by default).
#' @param chunk_length the minimum number of elements loaded at once,
#'                     will always be rounded up to a full memory page
#'                     (optional, a page by default).
#' @return a lazily loaded numeric vector of the specified length.
#' @export
ufo_numeric_constructor <- function(length, populate, writeback = NULL, reset = NULL, destroy = NULL, finalizer = NULL,
                        read_only = FALSE, chunk_length = 0, ...) {
    ufo_vector_constructor(mode = "numeric", length = length, 
               populate = populate, writeback = writeback,
               reset = reset, destroy = destroy,
               finalizer = finalizer, read_only = read_only,
               chunk_length = chunk_length, ...)
}

#' Produces a lazily populated UFO logical vector of the given length.
#' The vector will be populated on access section by section. The vector
#' will garbage collect chunks in response to memory pressure, so it can
#' be larger-than-memory.
#' @param length number of elements in the vector, must be > 1.
#' @param populate a function used to generate data inside the vector,
#' @param writeback a function called when a modified chunk of the vector
#'                  is garbage collected (optional).
#' @param reset a function called when a modified chunk is reset discarding
#'              dirty, manually written values.
#' @param destroy a function called when a modified chunk is being destroyed
#' @param finalizer a function called when the entire vector is garbage
#'                  collected (optional).
#' @param read_only sets the vector to be write-protected by the OS
#'                  (optional, false by default).
#' @param chunk_length the minimum number of elements loaded at once,
#'                     will always be rounded up to a full memory page
#'                     (optional, a page by default).
#' @param ... user data passed to populate, writeback, and finalizer functions,
#' @return a lazily loaded logical vector of the specified length.
#' @export
ufo_logical_constructor <- function(length, populate, writeback = NULL, reset = NULL, destroy = NULL, finalizer = NULL,
                        read_only = FALSE, chunk_length = 0, ...) {
    ufo_vector_constructor(mode = "logical", length = length,
               populate = populate, writeback = writeback,
               reset = reset, destroy = destroy,
               finalizer = finalizer, read_only = read_only,
               chunk_length = chunk_length, ...)
}

#' Produces a lazily populated UFO character vector of the given length.
#' The vector will be populated on access section by section. The vector
#' will garbage collect chunks in response to memory pressure, so it can
#' be larger-than-memory.
#' @param length number of elements in the vector, must be > 1.
#' @param populate a function used to generate data inside the vector,
#' @param writeback a function called when a modified chunk of the vector
#'                  is garbage collected (optional).
#' @param reset a function called when a modified chunk is reset discarding
#'              dirty, manually written values.
#' @param destroy a function called when a modified chunk is being destroyed
#' @param finalizer a function called when the entire vector is garbage
#'                  collected (optional).
#' @param read_only sets the vector to be write-protected by the OS
#'                  (optional, false by default).
#' @param chunk_length the minimum number of elements loaded at once,
#'                     will always be rounded up to a full memory page
#'                     (optional, a page by default).
#' @param ... user data passed to populate, writeback, and finalizer functions,
#' @return a lazily loaded character vector of the specified length.
#' @export
ufo_character_constructor <- function(length, populate, writeback = NULL, reset = NULL, destroy = NULL, finalizer = NULL,
                          read_only = FALSE, chunk_length = 0, ...) {
    ufo_vector_constructor(mode = "character", length = length, 
               populate = populate, writeback = writeback,
               reset = reset, destroy = destroy,
               finalizer = finalizer, read_only = read_only,
               chunk_length = chunk_length, ...)
}

#' Produces a lazily populated UFO complex vector of the given length.
#' The vector will be populated on access section by section. The vector
#' will garbage collect chunks in response to memory pressure, so it can
#' be larger-than-memory.
#' @param length number of elements in the vector, must be > 1.
#' @param populate a function used to generate data inside the vector,
#' @param writeback a function called when a modified chunk of the vector
#'                  is garbage collected (optional).
#' @param reset a function called when a modified chunk is reset discarding
#'              dirty, manually written values.
#' @param destroy a function called when a modified chunk is being destroyed
#' @param finalizer a function called when the entire vector is garbage
#'                  collected (optional).
#' @param read_only sets the vector to be write-protected by the OS
#'                  (optional, false by default).
#' @param chunk_length the minimum number of elements loaded at once,
#'                     will always be rounded up to a full memory page
#'                     (optional, a page by default).
#' @param ... user data passed to populate, writeback, and finalizer functions,
#' @return a lazily loaded complex vector of the specified length.
#' @export
ufo_complex_constructor <- function(length, populate, writeback = NULL, reset = NULL, destroy = NULL, finalizer = NULL,
                        user_data = NULL, read_only = FALSE, chunk_length = 0, ...) {
    ufo_vector_constructor(mode = "complex", length = length, 
               populate = populate, writeback = writeback,
               reset = reset, destroy = destroy,
               finalizer = finalizer, read_only = read_only,
               chunk_length = chunk_length, ...)
}

#' Produces a lazily populated UFO raw vector of the given length.
#' The vector will be populated on access section by section. The vector
#' will garbage collect chunks in response to memory pressure, so it can
#' be larger-than-memory.
#' @param length number of elements in the vector, must be > 1.
#' @param populate a function used to generate data inside the vector,
#' @param writeback a function called when a modified chunk of the vector
#'                  is garbage collected (optional).
#' @param reset a function called when a modified chunk is reset discarding
#'              dirty, manually written values.
#' @param destroy a function called when a modified chunk is being destroyed
#' @param finalizer a function called when the entire vector is garbage
#'                  collected (optional).
#' @param read_only sets the vector to be write-protected by the OS
#'                  (optional, false by default).
#' @param chunk_length the minimum number of elements loaded at once,
#'                     will always be rounded up to a full memory page
#'                     (optional, a page by default).
#' @param ... user data passed to populate, writeback, and finalizer functions,
#' @return a lazily loaded raw vector of the specified length.
#' @export
ufo_raw_constructor <- function(length, populate, writeback = NULL, reset = NULL, destroy = NULL, finalizer = NULL,
                    read_only = FALSE, chunk_length = 0, ...) {
    ufo_vector_constructor(mode = "raw", length = length,
               populate = populate, writeback = writeback,
               reset = reset, destroy = destroy,
               finalizer = finalizer, read_only = read_only,
               chunk_length = chunk_length, ...)
}

# These are just signatures of various functions used by UFOs, for reference.
.ufo_populate_prototype <- function(start, end, ...) NULL
.ufo_writeback_prototype <- function(start, end, data, ...) NULL
.ufo_reset_prototype <- function(...) NULL
.ufo_destroy_prototype <- function(...) NULL
.ufo_finalizer_prototype <- function(...) NULL