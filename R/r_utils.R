`%||%` = function(x, y) if (is.null(x)) return(y) else return(x)

enum_items = function(enum) unlist(as.list(enum), use.names = F)

list.append = function(.data, ...) {
  if (is.list(.data)) c(.data, list(...)) else c(.data, ..., recursive = FALSE)
}

# Sets dictionary[key] = value if value is not None.
.set = function(value, key, dictionary){
  if (!is.null(value)) {
    temp_dict = deparse(substitute(dictionary))
    dictionary[[key]] = value
    assign(temp_dict, dictionary, envir = parent.frame())
  }
}

paws_error_code = function(error){
  return(error[["error_response"]][["__type"]] %||% error[["error_response"]][["Code"]])
}
