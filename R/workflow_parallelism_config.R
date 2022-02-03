# NOTE: This code has been modified from AWS Sagemaker Python:
# https://github.com/aws/sagemaker-python-sdk/blob/master/src/sagemaker/workflow/parallelism_config.py

#' @import R6
#' @import sagemaker.core

#' @title ParallelismConfiguration
#' @description Parallelism config for SageMaker pipeline
#' @export
ParallelismConfiguration = R6Class("ParallelismConfiguration",
  public = list(

    #' @description Create a ParallelismConfiguration
    #' @param max_parallel_execution_steps, int:
    #'              max number of steps which could be parallelized
    initialize = function(max_parallel_execution_steps){
      stopifnot(
        is.numeric(max_parallel_execution_steps)
      )
      self$max_parallel_execution_steps = max_parallel_execution_steps
    },

    #' @description The request structure.
    to_request = function(){
      return(list(
        "MaxParallelExecutionSteps"=self$max_parallel_execution_steps
        )
      )
    },

    #' @description format class
    format = function(){
      format_class(self)
    }
  )
)
