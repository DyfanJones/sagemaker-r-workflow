# NOTE: This code has been modified from AWS Sagemaker Python:
# https://github.com/aws/sagemaker-python-sdk/blob/master/src/sagemaker/workflow/pipeline.py

#' @include r_utils.R
#' @include workflow_parameters.R
#' @include workflow_execution_variables.R
#' @include workflow_entities.R

#' @import R6
#' @import sagemaker.core

#' @title Workflow PipeLineExperimentConfig class
#' @description Experiment config for SageMaker pipeline.
#' @export
PipeLineExperimentConfig = R6Class("PipeLineExperimentConfig",
  inherit = Entity,
  public = list(

    #' @description Create a PipelineExperimentConfig
    #' @param experiment_name (Union[str, Parameter, ExecutionVariable, Expression]):
    #'              the name of the experiment that will be created.
    #' @param trial_name (Union[str, Parameter, ExecutionVariable, Expression]):
    #'              the name of the trial that will be created.
    #' @examples
    #' # Use pipeline name as the experiment name and pipeline execution id as the trial name::
    #' PipeLineExperimentConfig$new(
    #'      ExecutionVariables$PIPELINE_NAME, ExecutionVariables$PIPELINE_EXECUTION_ID)
    #' # Use a customized experiment name and pipeline execution id as the trial name::
    #' PipeLineExperimentConfig$new(
    #'      'MyExperiment', ExecutionVariables$PIPELINE_EXECUTION_ID)
    initialize = function(experiment_name,
                          trial_name){
      self$experiment_name = experiment_name
      self$trial_name = trial_name
    },

    #' @description Returns: the request structure.
    to_request = function(){
      return(list(
        "ExperimentName"=self$experiment_name,
        "TrialName"=self$trial_name)
      )
    }
  ),
  lock_objects = F
)

#' @title Workflow PipelineExperimentConfigProperty class
#' @description Reference to pipeline experiment config property.
#' @export
PipelineExperimentConfigProperty = R6Class("PipelineExperimentConfigProperty",
  inherit = Expression,
  public = list(

    #' @description Create a reference to pipeline experiment property.
    #' @param name (str): The name of the pipeline experiment config property.
    initialize = function(name){
      self$name = name
    }
  ),

  active = list(
    #' @field expr
    #' The 'Get' expression dict for a pipeline experiment config property.
    expr = function(){
      return(list("Get"=sprintf("PipelineExperimentConfig.%s", self$name)))
    }
  ),
  lock_objects = F
)

#' @title Workflow PipelineExperimentConfigProperties enum like class
#' @description Enum-like class for all pipeline experiment config property references.
#' @export
PipelineExperimentConfigProperties = Enum(
  EXPERIMENT_NAME = PipelineExperimentConfigProperty$new("ExperimentName"),
  TRIAL_NAME = PipelineExperimentConfigProperty$new("TrialName")
)
