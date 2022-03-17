# NOTE: This code has been modified from AWS Sagemaker Python:
# https://github.com/aws/sagemaker-python-sdk/blob/master/src/sagemaker/workflow/condition_step.py

#' @include workflow_conditions.R
#' @include workflow_steps.R
#' @include workflow_step_collections.R
#' @include workflow_utilities.R
#' @include workflow_entities.R
#' @include workflow_properties.R
#' @include r_utils.R

#' @import R6
#' @import sagemaker.core
#' @import sagemaker.common
#' @import sagemaker.mlcore

#' @title Workflow ConditionStep class
#' @description Conditional step for pipelines to support conditional
#'              branching in the execution of steps.
#' @export
ConditionStep = R6Class("ConditionStep",
  inherit = Step,
  public = list(

    #' @field conditions
    #' The name of the step.
    conditions = NULL,

    #' @field if_steps
    #' A list of `sagemaker.workflow.steps.Step` and
    #' `sagemaker.workflow.step_collections.StepCollection` instances
    if_steps = NULL,

    #' @field else_steps
    #' A list of `sagemaker.workflow.steps.Step` and
    #' `sagemaker.workflow.step_collections.StepCollection` instances
    else_steps = NULL,

    #' @description Construct a ConditionStep for pipelines to support conditional branching.
    #'              If all of the conditions in the condition list evaluate to True, the `if_steps` are
    #'              marked as ready for execution. Otherwise, the `else_steps` are marked as ready for
    #'              execution.
    #' @param name (str): The name of the step.
    #' @param depends_on (List[str]): The list of step names the current step depends on
    #' @param display_name (str): The display name of the condition step.
    #' @param description (str): The description of the condition step.
    #' @param conditions (List[Condition]): A list of `sagemaker.workflow.conditions.Condition`
    #'              instances.
    #' @param if_steps (List[Union[Step, StepCollection]]): A list of `sagemaker.workflow.steps.Step`
    #'              and `sagemaker.workflow.step_collections.StepCollection` instances that are
    #'              marked as ready for execution if the list of conditions evaluates to True.
    #' @param else_steps (List[Union[Step, StepCollection]]): A list of `sagemaker.workflow.steps.Step`
    #'              and `sagemaker.workflow.step_collections.StepCollection` instances that are
    #'              marked as ready for execution if the list of conditions evaluates to False.
    initialize = function(name,
                          depends_on=NULL,
                          display_name=NULL,
                          description=NULL,
                          conditions=NULL,
                          if_steps=NULL,
                          else_steps=NULL){
      super$initialize(
        name, display_name, description, StepTypeEnum$CONDITION, depends_on
      )
      self$conditions = conditions %||% list()
      self$if_steps = if_steps %||% list()
      self$else_steps = else_steps %||% list()

      root_path = sprintf("Steps.%s", name)
      root_prop = Properties$new(path=root_path)
      root_prop[["Outcome"]] = Properties$new(sprintf("%s.Outcome", root_path))
      private$.properties = root_prop
    }
  ),
  active = list(

    #' @field arguments
    #' The arguments dict that is used to define the conditional branching in the pipeline.
    arguments = function(){
      return(list(
        Conditions=lapply(self$conditions, function(condition) condition$to_request()),
        IfSteps=list_to_request(self$if_steps),
        ElseSteps=list_to_request(self$else_steps))
      )
    },

    #' @field properties
    #' A simple Properties object with `Outcome` as the only property
    properties = function(){
      return(private$.properties)
    }
  ),
  private = list(
    .properties = NULL
  )
)

#' @title Workflow JsonGet class
#' @description Get JSON properties from PropertyFiles.
#' @export
JsonGet = R6Class("JsonGet",
  inherit = Expression,
  public = list(

    #' @field step_name
    #' The step from which to get the property file.
    step_name = NULL,

    #' @field property_file
    #' Either a PropertyFile instance or the name of a property file.
    property_file = NULL,

    #' @field json_path
    #' The JSON path expression to the requested value.
    json_path = NULL,

    #' @description Initialize JsonGet class
    #' @param step_name (Step): The step from which to get the property file.
    #' @param property_file (Union[PropertyFile, str]): Either a PropertyFile instance
    #'              or the name of a property file.
    #' @param json_path (str): The JSON path expression to the requested value.
    initialize = function(step_name,
                          property_file,
                          json_path){
      self$step_name = step_name
      self$property_file = property_file
      self$json_path = json_path
    }
  ),
  active = list(

    #' @field expr
    #' The expression dict for a `JsonGet` function.
    expr = function(){
      if(!is.character(self$step_name) || !nzchar(self$step_name)){
        ValueError$new(
          "Please give a valid step name as a string"
        )
      }
      if (inherits(self$property_file, "PropertyFile")){
        name = self$property_file$name
      } else {
          name = self$property_file
      }
      return(list(
        "Std:JsonGet"=list(
          "PropertyFile"=list("Get"=sprintf("Steps.%s.PropertyFiles.%s",self$step_name, name)),
          "Path"=self$json_path)
        )
      )
    }
  )
)
