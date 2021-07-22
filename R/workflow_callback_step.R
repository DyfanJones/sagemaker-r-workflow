# NOTE: This code has been modified from AWS Sagemaker Python:
# https://github.com/aws/sagemaker-python-sdk/blob/master/src/sagemaker/workflow/callback_step.py

#' @include workflow_entities.R
#' @include workflow_properties.R
#' @include workflow_steps.R
#' @include r_utils.R

#' @import R6
#' @import R6sagemaker.common

#' @title Workflow CallbackOuputTypeEnum class
#' @description CallbackOutput type enum.
#' @export
CallbackOuputTypeEnum = Enum(
  String = "String",
  Integer = "Integer",
  Boolean = "Boolean",
  Float = "Float"
)

#' @title Workflow CallbackOutput class
#' @description Output for a callback step.
#' @export
CallbackOutput = R6Class("CallbackOutput",
  public = list(

    #' @field output_name
    #' The output name
    output_name = NULL,

    #' @field output_type
    #' The output type
    output_type = NULL,

    #' @description Initialize CallbackOutput class
    #' @param output_name (str): The output name
    #' @param output_type (CallbackOutputTypeEnum): The output type
    initialize = function(output_name,
                          output_type = CallbackOutputTypeEnum$String){
      self$output_name = output_name
      self$output_type = output_type
    },

    #' @description Get the request structure for workflow service calls.
    to_request = function(){
      return(list(
        "OutputName"=self$output_name,
        "OutputType"=self$output_type)
      )
    },

    #' @description The 'Get' expression dict for a `CallbackOutput`.
    #' @param step_name (str): The name of the step the callback step associated
    #'              with this output belongs to.
    expr = function(step_name){
      return(private$.expr(self$output_name, step_name))
    },

    #' @description format class
    format = function(){
      format_class(self)
    }
  ),
  private = list(

    # An internal classmethod for the 'Get' expression dict for a `CallbackOutput`.
    # Args:
    # name (str): The name of the callback output.
    # step_name (str): The name of the step the callback step associated
    # with this output belongs to.
    .expr = function(name, step_name){
      return(list("Get"=sprintf("Steps.%s.OutputParameters['%s']",step_name , name)))
    }
  )
)

#' @title Callback step for workflow.
#' @export
CallbackStep = R6Class("CallbackStep",
  inherit = Step,
  public = list(

    #' @field sqs_queue_url
    #' An SQS queue URL for receiving callback messages.
    sqs_queue_url = NULL,

    #' @field inputs
    #' Input arguments that will be provided in the SQS message body of callback messages
    inputs = NULL,

    #' @field outputs
    #' Outputs that can be provided when completing a callback.
    outputs = NULL,

    #' @field cache_config
    #' A list of step names this `sagemaker.workflow.steps.TransformStep`
    cache_config = NULL,

    #' @description Constructs a CallbackStep.
    #' @param name (str): The name of the callback step.
    #' @param sqs_queue_url (str): An SQS queue URL for receiving callback messages.
    #' @param inputs (dict): Input arguments that will be provided
    #'              in the SQS message body of callback messages.
    #' @param outputs (List[CallbackOutput]): Outputs that can be provided when completing a callback.
    #' @param cache_config (CacheConfig):  A `sagemaker.workflow.steps.CacheConfig` instance.
    #' @param depends_on (List[str]): A list of step names this `sagemaker.workflow.steps.TransformStep`
    #'              depends on
    initialize = function(name,
                          sqs_queue_url,
                          inputs,
                          outputs,
                          cache_config=NULL,
                          depends_on=NULL){
      stopifnot(
        is.character(name),
        is.character(sqs_queue_url),
        is.list(inputs),
        is.list(outputs),
        inherits(cache_config, "CacheConfig") || is.null(cache_config),
        is.list(depends_on) || is.null(depends_on)
      )
      super$initialize(name, StepTypeEnum$CALLBACK, depends_on)
      self$sqs_queue_url = sqs_queue_url
      self$outputs = outputs
      self$cache_config = cache_config
      self$inputs = inputs

      root_path = sprintf("Steps.%s", name)
      root_prop = Properties$new(path=root_path)

      property_dict = list()
      for (output in outputs){
        property_dict[[output$output_name]] = Properties$new(
          sprintf("%s.OutputParameters['%s']",
                  root_path, output$output_name)
        )
      }
      private$.properties = list(Outputs = property_dict)
    },

    #' @description Updates the dictionary with cache configuration.
    to_request = function(){
      request_dict = super$to_request()
      if (!is.null(self$cache_config))
        request_dict = modifyList(request_dict, self$cache_config$config)

      request_dict[["SqsQueueUrl"]] = self$sqs_queue_url
      request_dict[["OutputParameters"]] = list(Map(function(op){op$to_request()}, self$outputs))
      return(request_dict)
    }
  ),
  active =list(

    #' @field arguments
    #' The arguments dict that is used to define the callback step
    arguments = function(){
      return(self$inputs)
    },

    #' @field properties
    #' A Properties object representing the output parameters of the callback step.
    properties = function(){
      return(private$.properties)
    }
  ),
  private = list(
    .properties = NULL
  )
)
