# NOTE: This code has been modified from AWS Sagemaker Python:
# https://github.com/aws/sagemaker-python-sdk/blob/master/src/sagemaker/workflow/execution_variables.py

#' @include workflow_entities.R

#' @import R6
#' @import sagemaker.core
#' @import sagemaker.common

#' @title Workflow ExecutionVariable class
#' @description Pipeline execution variables for workflow.
#' @export
ExecutionVariable = R6Class("ExecutionVariable",
  inherit = Expression,
  public = list(

    #' @field name
    #' The name of the execution variable.
    name = NULL,

    #' @description Create a pipeline execution variable.
    #' @param name (str): The name of the execution variable.
    initialize = function(name){
      stopifnot(
        is.character(name)
      )
      self$name = name
    }
  ),
  active =list(

    #' @field expr
    #' The 'Get' expression dict for an `ExecutionVariable`.
    expr = function(){
      return(list("Get"=sprintf("Execution.%s", self$name)))
    }
  )
)

#' @title Enum-like class for all ExecutionVariable instances.
#' @description Considerations to move these as module-level constants should be made.
#' @export
ExecutionVariables = R6Class("ExecutionVariables",
  public = list(


    #' @field START_DATETIME
    #'
    START_DATETIME = ExecutionVariable$new("StartDateTime"),

    #' @field CURRENT_DATETIME
    #'
    CURRENT_DATETIME = ExecutionVariable$new("CurrentDateTime"),

    #' @field PIPELINE_NAME
    #'
    PIPELINE_NAME = ExecutionVariable$new("PipelineName"),

    #' @field PIPELINE_ARN
    #'
    PIPELINE_ARN = ExecutionVariable$new("PipelineArn"),

    #' @field PIPELINE_EXECUTION_ARN
    #'
    PIPELINE_EXECUTION_ARN = ExecutionVariable$new("PipelineExecutionArn"),

    #' @description format class
    format = function(){
      format_class(self)
    }
  )
)
