onet <- function(initializer) {
    singleton <- new.env();
    singleton$instance = NULL;
    singleton$initializer = initializer;
    class(singleton) <- "onet";
    singleton
}

get_or_create <- function(singleton) UseMethod("get_or_create")
destroy <- function(singleton) UseMethod("destroy")

get_or_create.onet <- function(singleton, ...) {
    if (is.null(singleton$instance)) {
        singleton$instance <- singleton$initializer(...)
    }
    singleton$instance
}

destroy.onet <- function(singleton) {
    if (!is.null(singleton$instance)) {
        singleton$instance <- NULL
    }
}