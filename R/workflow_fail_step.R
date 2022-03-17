# NOTE: This code has been modified from AWS Sagemaker Python:
# https://github.com/aws/sagemaker-python-sdk/blob/master/src/sagemaker/workflow/fail_step.py

#' @include workflow_entities.R
#' @include workflow_steps.R
#' @include r_utils.R

#' @import R6
#' @import sagemaker.core

#' @title Workflow FailStep
#' @description FailStep` for SageMaker Pipelines Workflows.
#' @export
FailStep = R6Class("FailStep",
  inherit = Step,
  public = list(

    #' @field error_message
    #' An error message defined by the user.
    error_message = NULL,

    #' @description Constructs a `FailStep`.
    #' @param name (str): The name of the `FailStep`. A name is required and must be
    #'              unique within a pipeline.
    #' @param error_message (str or PipelineNonPrimitiveInputTypes):
    #'              An error message defined by the user.
    #'              Once the `FailStep` is reached, the execution fails and the
    #'              error message is set as the failure reason (default: None).
    #' @param display_name (str): The display name of the `FailStep`.
    #'              The display name provides better UI readability. (default: None).
    #' @param description (str): The description of the `FailStep` (default: None).
    #' @param depends_on (List[str] or List[Step]): A list of `Step` names or `Step` instances
    #'              that this `FailStep` depends on.
    #'              If a listed `Step` name does not exist, an error is returned (default: None).
    initialize = function(name,
                          error_message=NULL,
                          display_name=NULL,
                          description=NULL,
                          depends_on=NULL){
      stopifnot(
        is.character(name),
        is.character(display_name) || is.null(display_name),
        is.character(description) || is.null(description)
      )
      super$initialize(
        name, display_name, description, StepTypeEnum$FAIL, depends_on
      )
      self$error_message = error_message %||% ""
    }
  ),
  active = list(

    #' @field arguments
    #' The arguments dictionary that is used to define the `FailStep`.
    arguments = function(){
      return(list(ErrorMessage=self$error_message))
    },

    #' @field properties
    #' A `Properties` object is not available for the `FailStep`.
    #' Executing a `FailStep` will terminate the pipeline.
    #' `FailStep` properties should not be referenced.
    properties = function(){
      RuntimeError$new(
        "FailStep is a terminal step and the Properties object is not available for it."
      )
    }
  )
)
