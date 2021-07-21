`%||%` <- function(x, y) if (is.null(x)) return(y) else return(x)

enum_items = function(enum) unlist(as.list(enum), use.names = F)
