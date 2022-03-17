# NOTE: This code has been modified from AWS Sagemaker Python:
# https://github.com/aws/sagemaker-python-sdk/blob/master/src/sagemaker/workflow/functions.py

#' @include workflow_entities.R

#' @import R6

#' @title Join Class
#' @description Join together properties.
#' @export
Join = R6Class("Join",
  inherit = Expression,
  public = list(

    #' @field on
    #' The primitive types and parameters to join.
    on = NULL,

    #' @field values
    #' The string to join the values on (Defaults to "").
    values = NULL,

    #' @description Initialize Join Class
    #' @param on (str): The string to join the values on (Defaults to "").
    #' @param values (List[Union[PrimitiveType, Parameter]]): The primitive types
    #'              and parameters to join.
    initialize = function(on="",
                          values=""){
      self$on = on
      self$values = values
    }
  ),
  active = list(

    #' @field expr
    #' The expression dict for a `Join` function.
    expr = function(){
      return(list(
        "Std:Join" = list(
          "On"=self$on,
          "Values" = lapply(
            self$values, function(value) if("expr" %in% names(value)) value$expr else value)
          )
      ))
    }
  )
)
