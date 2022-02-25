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

# Checks if a list exists with in another list.
#' @import data.table
list.exist.in = function(a, b){
  main_list = rbindlist(b, fill = TRUE)
  if (length(names(main_list)) == 0) return(FALSE)
  sub_list = as.data.table(a)
  if(!all(names(sub_list) %in% names(main_list))) return(FALSE)
  setcolorder(main_list, names(sub_list))
  main_list[, check := FALSE][sub_list, check := TRUE, on = names(sub_list)]
  return(any(main_list$check))
}
