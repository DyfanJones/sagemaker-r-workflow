# NOTE: This code has been modified from AWS Sagemaker Python:
# https://github.com/aws/sagemaker-python-sdk/blob/master/src/sagemaker/workflow/entities.py

#' @import R6
#' @import sagemaker.core

#' @include workflow_functions.R

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

#' @title Base object for pipeline variables
#' @description PipelineVariables must implement the expr property.
#' @keywords Internal
#' @export
PipelineVariable = R6Class("PipelineVariable",
  inherit = PropertiesMeta,
  public = list(

    #' @description Prompt the pipeline to convert the pipeline variable to String in runtime
    to_string = function(){
      return(Join$new(on="", values=list(self)))
    },

    #' @description Simulate the Python string's built-in method: startswith
    #' @param prefix (str, tuple): The (tuple of) string to be checked.
    #' @param start (int): To set the start index of the matching boundary (default: None).
    #' @param end (int): To set the end index of the matching boundary (default: None).
    #' @return bool: Always return False as Pipeline variables are parsed during execution runtime
    startswith = function(prefix,
                          start=NULL,
                          end=NULL){
      return(FALSE)
    },

    #' @description Simulate the Python string's built-in method: endswith
    #' @param suffix (str, tuple): The (tuple of) string to be checked.
    #' @param start (int): To set the start index of the matching boundary (default: None).
    #' @param end (int): To set the end index of the matching boundary (default: None).
    #' @return bool: Always return False as Pipeline variables are parsed during execution runtime
    endswith = function(suffix,
                        start=NULL,
                        end=NULL){
      return(FALSE)
    }
  ),
  active = list(
    #' @field expr
    #'Get the expression structure for workflow service calls.
    expr = function(){
      invisible()
    }
  )
)
