# NOTE: This code has been modified from AWS Sagemaker Python:
# https://github.com/aws/sagemaker-python-sdk/blob/master/src/sagemaker/workflow/lambda_step.py

#' @include workflow_entities.R
#' @include workflow_properties.R
#' @include workflow_steps.R

#' @import R6
#' @import sagemaker.core

#' @title LambdaOutput type enum.
#' @export
LambdaOutputTypeEnum = Enum(
  String = "String",
  Integer = "Integer",
  Boolean = "Boolean",
  Float = "Float"
)

#' @title Workflow LambdaOutput class
#' @description Output for a lambdaback step.
#' @export
LambdaOutput = R6Class("LambdaOutput",
  public = list(

    #' @field output_name
    #'        (str): The output name
    output_name = NULL,

    #' @field output_type
    #'        (LambdaOutputTypeEnum): The output type
    output_type = NULL,

    #' @description Initialize LambdaOutput class
    #' @param output_name (str): The output name
    #' @param output_type (LambdaOutputTypeEnum): The output type
    initialize = function(output_name,
                          output_type=enum_items(LambdaOutputTypeEnum)){
      self$output_name = output_name
      self$output_type = match.arg(output_type)
    },

    #' @description Get the request structure for workflow service calls.
    to_request = function(){
      return(list(
        "OutputName"=self$output_name,
        "OutputType"=self$output_type)
      )
    },

    #' @description The 'Get' expression dict for a `LambdaOutput`.
    #' @param step_name (str): The name of the step the lambda step associated
    expr = function(step_name){
      return(private$.expr(self$output_name, step_name))
    },

    #' @description format class
    format = function(){
      format_class(self)
    }
  ),
  private = list(

    # An internal classmethod for the 'Get' expression dict for a `LambdaOutput`.
    # Args:
    #   name (str): The name of the lambda output.
    # step_name (str): The name of the step the lambda step associated
    # with this output belongs to.
    .expr = function(name, step_name){
      return(list("Get"=sprintf("Steps.%s.OutputParameters['%s']",step_name, name)))
    }
  )
)

#' @title Workflow LambdaStep class
#' @description Lambda step for workflow.
#' @export
LambdaStep = R6Class("LambdaStep",
  inherit = Step,
  public = list(

    #' @description Constructs a LambdaStep.
    #' @param name (str): The name of the lambda step.
    #' @param lambda_func (str): An instance of sagemaker.lambda_helper.Lambda.
    #'              If lambda arn is specified in the instance, LambdaStep just invokes the function,
    #'              else lambda function will be created while creating the pipeline.
    #' @param display_name (str): The display name of the Lambda step.
    #' @param description (str): The description of the Lambda step.
    #' @param inputs (dict): Input arguments that will be provided
    #'              to the lambda function.
    #' @param outputs (List[LambdaOutput]): List of outputs from the lambda function.
    #' @param cache_config (CacheConfig):  A `sagemaker.workflow.steps.CacheConfig` instance.
    #' @param depends_on (List[str]): A list of step names this `sagemaker.workflow.steps.LambdaStep`
    #'              depends on
    initialize = function(name,
                          lambda_func,
                          display_name=NULL,
                          description=NULL,
                          inputs=NULL,
                          outputs=NULL,
                          cache_config=NULL,
                          depends_on=NULL){
      stopifnot(
        is.character(name),
        inherits(lambda_func, "Lambda"),
        is.null(display_name) || is.character(display_name),
        is.null(description) || is.character(description),
        is.null(inputs) || is.list(inputs),
        is.null(outputs) || is.list(outputs),
        is.null(cache_config) || inherits(cache_config, "CacheConfig"),
        is.null(depends_on) || is.list(depends_on)
      )
      super$initialize(
        name, display_name, description, StepTypeEnum$LAMBDA, depends_on
      )
      self$lambda_func = lambda_func
      self$outputs = outputs %||% list()
      self$cache_config = cache_config
      self$inputs = inputs %||% list()

      root_path = sprintf("Steps.%s", name)
      root_prop = Properties$new(path=root_path)

      property_dict = list()
      for (output in self$outputs){
        property_dict[[output$output_name]] = Properties$new(
          sprintf("%s.OutputParameters['%s']",
            root_path, output$output_name)
        )
      }
      root_prop[["Outputs"]] = property_dict
      private$.properties = root_prop
    },

    #' @description Updates the dictionary with cache configuration.
    to_request = function(){
      request_dict = super$to_request()
      if (!islistempty(self$cache_config))
        request_dict= modifyList(request_dict, self$cache_config$config)

      function_arn = private$.get_function_arn()
      request_dict[["FunctionArn"]] = function_arn

      request_dict[["OutputParameters"]] = Map(function(op){op$to_request()}, self$outputs)

      return(request_dict)
    }
  ),
  active = list(

    #' @field arguments
    #' The arguments dict that is used to define the lambda step.
    arguments = function(){
      return(self$inputs)
    },

    #' @field properties
    #' A Properties object representing the output parameters of the lambda step.
    properties = function(){
      return(private$.properties)
    }
  ),
  private = list(

    # Returns the lamba function arn
    # Method creates a lambda function and returns it's arn.
    # If the lambda is already present, it will build it's arn and return that.
    .get_function_arn = function(){
      region = self$lambda_func$session$paws_region_name
      if (tolower(region) == "cn-north-1" || tolower(region) == "cn-northwest-1"){
        partition = "aws-cn"
      } else {
        partition = "aws"
      }
      if (is.null(self$lambda_func$function_arn)) {
        account_id = self$lambda_func$session$account_id()
        tryCatch({
          response = self$lambda_func$create()
          return(response[["FunctionArn"]])
        }, ValueError = function(e){
          if(!grepl("ResourceConflictException", e$message)) {
            ValueError$new(e$message)
          }
          return (paste0(
            sprintf("arn:%s:lambda:%s:%s:", partition, region, account_id),
            sprintf("function:%s", self$lambda_func$function_name)
          ))
        })
      } else {
        return(self$lambda_func$function_arn)
      }
    }
  ),
  lock_objects = F
)
