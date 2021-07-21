# NOTE: This code has been modified from AWS Sagemaker Python:
# https://github.com/aws/sagemaker-python-sdk/blob/master/src/sagemaker/workflow/entities.py

#' @import R6
#' @import R6sagemaker.common

#' @title Base object for workflow entities.
#' @description Entities must implement the to_request method.
#' @keywords internal
Entity = R6Class("Entity",
  public = list(

    #' @description Get the request structure for workflow service calls.
    to_request = function(){
      NotImplementedError$new()
    }
  )
)


DefaultEnumMeta = R6Class("DefaultEnumMeta",
  public = list(
    default = NULL,
    factory = function(value = self$default){
      if (identical(value, DefaultEnumMeta$public_fields$default))
        return(get(class(self)[[1]])$private_fields[[1]])
      return(get(class(self)[[2]])$private_fields[[1]])
    }
  )
)

#' @title Base object for expressions.
#' @description Expressions must implement the expr property.
#' @keywords internal
Expression = R6Class("Expression",
  active = list(

    #' @field expr
    #' Get the expression structure for workflow service calls.
    expr = function(){
      NotImplementedError$new()
    }
  ),
  private = list(
    .validate_value = function (obj, classes) {
      for (cls in classes) {
        if (inherits(obj, cls)) return(obj)
        SageMakerError$new(
          sprintf("Invalid value class %s, expected %s", dQuote(class(obj)),
                  paste(dQuote(classes), collapse = " or "))
        )
      }
    }
  )
)
