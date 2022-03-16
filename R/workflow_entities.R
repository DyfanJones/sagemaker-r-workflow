# NOTE: This code has been modified from AWS Sagemaker Python:
# https://github.com/aws/sagemaker-python-sdk/blob/master/src/sagemaker/workflow/entities.py

#' @import R6
#' @import sagemaker.core

#' @title Base object for workflow entities.
#' @description Entities must implement the to_request method.
#' @keywords internal
#' @export
Entity = R6Class("Entity",
  public = list(

    #' @description Get the request structure for workflow service calls.
    to_request = function(){
      NotImplementedError$new()
    },

    #' @description format class
    format = function(){
      format_class(self)
    }
  )
)

#' @title Base object for expressions.
#' @description Expressions must implement the expr property.
#' @keywords internal
#' @export
Expression = R6Class("Expression",
  public = list(

    #' @description format class
    format = function(){
      format_class(self)
    }
  ),
  active = list(

    #' @field expr
    #' Get the expression structure for workflow service calls.
    expr = function(){
      return(invisible(NULL))
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
