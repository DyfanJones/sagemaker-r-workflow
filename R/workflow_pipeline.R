# NOTE: This code has been modified from AWS Sagemaker Python:
# https://github.com/aws/sagemaker-python-sdk/blob/master/src/sagemaker/workflow/pipeline.py

#' @include r_utils.R
#' @include workflow_callback_step.R
#' @include workflow_lambda_step.R
#' @include workflow_entities.R
#' @include workflow_execution_variables.R
#' @include workflow_parameters.R
#' @include workflow_pipeline_experiment_config.R
#' @include workflow_parallelism_config.R
#' @include workflow_properties.R
#' @include workflow_steps.R
#' @include workflow_step_collections.R
#' @include workflow_utilities.R

#' @import jsonlite
#' @import R6
#' @import sagemaker.core
#' @importFrom stats setNames

#' @title Workflow Pipeline class
#' @description Pipeline for workflow.
#' @export
Pipeline = R6Class("Pipeline",
  inherit = Entity,
  public = list(

    #' @description Initialize Pipeline Class
    #' @param name (str): The name of the pipeline.
    #' @param parameters (Sequence[Parameter]): The list of the parameters.
    #' @param pipeline_experiment_config (Optional[PipelineExperimentConfig]): If set,
    #'              the workflow will attempt to create an experiment and trial before
    #'              executing the steps. Creation will be skipped if an experiment or a trial with
    #'              the same name already exists. By default, pipeline name is used as
    #'              experiment name and execution id is used as the trial name.
    #'              If set to None, no experiment or trial will be created automatically.
    #' @param steps (Sequence[Union[Step, StepCollection]]): The list of the non-conditional steps
    #'              associated with the pipeline. Any steps that are within the
    #'              `if_steps` or `else_steps` of a `ConditionStep` cannot be listed in the steps of a
    #'              pipeline. Of particular note, the workflow service rejects any pipeline definitions that
    #'              specify a step in the list of steps of a pipeline and that step in the `if_steps` or
    #'              `else_steps` of any `ConditionStep`.
    #' @param sagemaker_session (Session): Session object that manages interactions
    #'              with Amazon SageMaker APIs and any other AWS services needed. If not specified, the
    #'              pipeline creates one using the default AWS configuration chain.
    initialize = function(name,
                          parameters,
                          pipeline_experiment_config,
                          steps=NULL,
                          sagemaker_session=NULL){
      self$name = name
      self$parameters = parameters
      self$pipeline_experiment_config = (
        if(!missing(pipeline_experiment_config))
          pipeline_experiment_config
        else
          PipelineExperimentConfig$new(
            ExecutionVariables$PIPELINE_NAME, ExecutionVariables$PIPELINE_EXECUTION_ID
          )
      )
      self$steps = steps %||% list()
      self$sagemaker_session = sagemaker_session %||% Session$new()
    },

    #' @description Gets the request structure for workflow service calls.
    to_request = function(){
      return(list(
        "Version"=private$.version,
        "Metadata"=private$.metadata,
        "Parameters"=list_to_request(self$parameters),
        "PipelineExperimentConfig"=(
          if(!is.null(self$pipeline_experiment_config))
            self$pipeline_experiment_config$to_request()
          else
            NULL
        ),
        "Steps"=list_to_request(self$steps)
        )
      )
    },

    #' @description Creates a Pipeline in the Pipelines service.
    #' @param role_arn (str): The role arn that is assumed by the pipeline to create step artifacts.
    #' @param description (str): A description of the pipeline.
    #' @param tags (List[Dict[str, str]]): A list of {"Key": "string", "Value": "string"} dicts as
    #'              tags.
    #' @param parallelism_config (Optional[ParallelismConfiguration]): Parallelism configuration
    #'              that is applied to each of the executions of the pipeline. It takes precedence
    #'              over the parallelism configuration of the parent pipeline.
    #' @return A response dict from the service.
    create = function(role_arn,
                      description=NULL,
                      tags=NULL,
                      parallelism_config=NULL){
      tags = .append_project_tags(tags)
      kwargs = private$.create_args(role_arn, description, parallelism_config)
      update_args(
        kwargs,
        Tags=tags
      )
      return(do.call(self$sagemaker_session$sagemaker$create_pipeline, kwargs))
    },

    #' @description Describes a Pipeline in the Workflow service.
    #' @return Response dict from the service. See `boto3 client documentation
    #'              \url{https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_pipeline}
    describe = function(){
      return(self$sagemaker_session$sagemaker$describe_pipeline(PipelineName=self$name))
    },

    #' @description Updates a Pipeline in the Workflow service.
    #' @param role_arn (str): The role arn that is assumed by pipelines to create step artifacts.
    #' @param description (str): A description of the pipeline.
    #' @param parallelism_config (Optional[ParallelismConfiguration]): Parallelism configuration
    #'              that is applied to each of the executions of the pipeline. It takes precedence
    #'              over the parallelism configuration of the parent pipeline.
    #' @return A response dict from the service.
    update = function(role_arn,
                      description=NULL,
                      parallelism_config=NULL){
      kwargs = private$.create_args(role_arn, description, parallelism_config)
      return(do.call(self$sagemaker_session$sagemaker$update_pipeline, kwargs))
    },

    #' @description Creates a pipeline or updates it, if it already exists.
    #' @param role_arn (str): The role arn that is assumed by workflow to create step artifacts.
    #' @param description (str): A description of the pipeline.
    #' @param tags (List[Dict[str, str]]): A list of {"Key": "string", "Value": "string"} dicts as
    #'              tags.
    #' @param parallelism_config (Optional[Config for parallel steps, Parallelism configuration that
    #'              is applied to each of. the executions
    #' @return response dict from service
    upsert = function(role_arn,
                      description=NULL,
                      tags=NULL,
                      parallelism_config=NULL){
      tryCatch({
        response = self$create(role_arn, description, tags, parallelism_config)
      }, error = function(e){
        error_code = paws_error_code(e)
        if(error_code == "ValidationException"
           && grepl("Pipeline names must be unique within", e$message)){
          response = self$update(role_arn, description)
          if(!is.null(tags)){
            old_tags = self$sagemaker_session$sagemaker$list_tags(
              ResourceArn=response[["PipelineArn"]]
            )[["Tags"]]

            tag_keys = lapply(tags, function(tag) tag[["Key"]])
            for (old_tag in old_tags){
              if (!(old_tag[["Key"]] %in% names(tag_keys)))
                tags =list.append(tags, old_tag)
            }
            self$sagemaker_session$sagemaker$add_tags(
              ResourceArn=response[["PipelineArn"]], Tags=tags
            )
          }
        } else {
          stop(e)
        }
      })
      return(response)
    },

    #' @description Deletes a Pipeline in the Workflow service.
    #' @return A response dict from the service.
    delete = function(){
      return(self$sagemaker_session$sagemaker$delete_pipeline(PipelineName=self$name))
    },

    #' @description Starts a Pipeline execution in the Workflow service.
    #' @param parameters (Dict[str, Union[str, bool, int, float]]): values to override
    #'              pipeline parameters.
    #' @param execution_display_name (str): The display name of the pipeline execution.
    #' @param execution_description (str): A description of the execution.
    #' @param parallelism_config (Optional[ParallelismConfiguration]): Parallelism configuration
    #'              that is applied to each of the executions of the pipeline. It takes precedence
    #'              over the parallelism configuration of the parent pipeline.
    #' @return A `.PipelineExecution` instance, if successful.
    start = function(parameters=NULL,
                     execution_display_name=NULL,
                     execution_description=NULL,
                     parallelism_config=NULL){
      exists = TRUE
      tryCatch({
        self$describe()
        },
        error = function(e){
          assign("exists", FALSE, envir = parent.env(environment()))
      })
      if (!isTRUE(exists))
        ValueError$new(
          "This pipeline is not associated with a Pipeline in SageMaker. ",
          "Please invoke create() first before attempting to invoke start()."
        )
      kwargs = list(PipelineName=self$name)
      update_args(
        kwargs,
        PipelineParameters=format_start_parameters(parameters),
        PipelineExecutionDescription=execution_description,
        PipelineExecutionDisplayName=execution_display_name,
        ParallelismConfiguration=parallelism_config
      )
      response = do.call(self$sagemaker_session$sagemaker$start_pipeline_execution, kwargs)
      return(.PipelineExecution$new(
        arn=response[["PipelineExecutionArn"]],
        sagemaker_session=self$sagemaker_session)
      )
    },

    #' @description Converts a request structure to string representation for workflow service calls.
    definition = function(){
      request_dict = self$to_request()
      request_dict[["PipelineExperimentConfig"]] = interpolate(
        request_dict[["PipelineExperimentConfig"]], list(), list()
      )
      callback_output_to_step_map = .map_callback_outputs(self$steps)
      lambda_output_to_step_name = .map_lambda_outputs(self$steps)
      request_dict[["Steps"]] = interpolate(
        request_dict[["Steps"]],
        callback_output_to_step_map=callback_output_to_step_map,
        lambda_output_to_step_map=lambda_output_to_step_name
      )

      return(jsonlite::toJSON(request_dict, auto_unbox = T))
    }
  ),
  private = list(
    .version = "2020-12-01",
    .metadata = list(),

    # Constructs the keyword argument dict for a create_pipeline call.
    # Args:
    #   role_arn (str): The role arn that is assumed by pipelines to create step artifacts.
    # description (str): A description of the pipeline.
    # parallelism_config (Optional[ParallelismConfiguration]): Parallelism configuration
    # that is applied to each of the executions of the pipeline. It takes precedence
    # over the parallelism configuration of the parent pipeline.
    # Returns:
    #   A keyword argument dict for calling create_pipeline.
    .create_args = function(role_arn, description, parallelism_config){
      pipeline_definition = self$definition()
      kwargs = list(
        PipelineName=self$name,
        RoleArn=role_arn
      )
      # If pipeline definition is large, upload to S3 bucket and
      # provide PipelineDefinitionS3Location to request instead.
      if (nchar(pipeline_definition) < (1024 * 100)) {
        kwargs[["PipelineDefinition"]] = pipeline_definition
      } else {
        desired_s3_uri = s3_path_join(
          "s3://", self$sagemaker_session$default_bucket(), self$name
        )
        S3Uploader$new()$upload_string_as_file_body(
          body=pipeline_definition,
          desired_s3_uri=desired_s3_uri,
          sagemaker_session=self$sagemaker_session
        )
        kwargs[["PipelineDefinitionS3Location"]] = list(
          "Bucket"=self$sagemaker_session$default_bucket(),
          "ObjectKey"=self$name
        )
      }

      update_args(
        kwargs, PipelineDescription=description, ParallelismConfiguration=parallelism_config
      )
      return(kwargs)
    }
  ),
  lock_objects = F
)

#' @title Formats start parameter overrides as a list of dicts.
#' @description This list of dicts adheres to the request schema of:
#'              `{"Name": "MyParameterName", "Value": "MyValue"}`
#' @param parameters (Dict[str, Any]): A dict of named values where the keys are
#'              the names of the parameters to pass values into.
#' @export
format_start_parameters = function(parameters){
  if (is.null(parameters))
    return(NULL)
  return(lapply(
    names(parameters), function(name) list("Name"=name, "Value"=as.character(parameters[[name]]))
    )
  )
}

#' @title Replaces Parameter values in a list of nested Dict[str, Any] with their workflow expression.
#' @param request_obj (RequestType): The request dict.
#' @param callback_output_to_step_map (list[str, str]): A dict of output name -> step name.
#' @param lambda_output_to_step_map (list[str, str]): Placeholder
#' @return RequestType: The request dict with Parameter values replaced by their expression.
#' @export
interpolate = function(request_obj,
                       callback_output_to_step_map,
                       lambda_output_to_step_map){
  return(.interpolate(
    request_obj,
    callback_output_to_step_map=callback_output_to_step_map,
    lambda_output_to_step_map=lambda_output_to_step_map)
  )
}

# Walks the nested request dict, replacing Parameter type values with workflow expressions.
# Args:
#   obj (Union[RequestType, Any]): The request dict.
# callback_output_to_step_map (Dict[str, str]): A dict of output name -> step name.
.interpolate = function(obj,
                        callback_output_to_step_map,
                        lambda_output_to_step_map){
  if (inherits(obj, c("Expression", "Parameter", "Properties")))
    return(obj$expr)

  if (inherits(obj, "CallbackOutput")){
    step_name = callback_output_to_step_map[[obj$output_name]]
    return(obj$expr(step_name))
  }
  if (inherits(obj, "LambdaOutput")){
    step_name = lambda_output_to_step_map[[obj$output_name]]
    return(obj$expr(step_name))
  }
  if (is.list(obj)){
    new = lapply(obj,
      function(value) interpolate(value, callback_output_to_step_map, lambda_output_to_step_map)
    )
  } else {
    return(obj)
  }
  return(new)
}

# Iterate over the provided steps, building a map of callback output parameters to step names.
# Args:
#   step (List[Step]): The steps list.
.map_callback_outputs = function(steps){
  callback_output_map = list()
  for (step in steps){
    if (inherits(step, "CallbackStep")){
      if (!is.null(step$outputs)) {
        for (output in step$outputs) {
          callback_output_map[[output$output_name]] = step$name
        }
      }
    }
  }
  return(callback_output_map)
}

# Iterate over the provided steps, building a map of lambda output parameters to step names.
# Args:
#   step (List[Step]): The steps list.
.map_lambda_outputs = function(steps){
  lambda_output_map = list()
  for (step in steps){
    if (inherits(step, "LambdaStep")){
      if (!is.null(step$outputs)){
        for (output in step$outputs){
          lambda_output_map[[output$output_name]] = step$name
        }
      }
    }
  }
  return(lambda_output_map)
}

# Updates the request arguments dict with a value, if populated.
# This handles the case when the service API doesn't like NoneTypes for argument values.
# Args:
#     request_args (Dict[str, Any]): The request arguments dict.
#     kwargs: key, value pairs to update the args dict with.
update_args = function(args, ...){
  kwargs = list(...)
  args_name = deparse(substitute(args))
  for (key in names(kwargs)){
    if(!is.null(kwargs[[key]])){
      args = modifyList(args, setNames(list(kwargs[[key]]), key))
    }
  }
  assign(args_name, args, envir = parent.frame())
}

#' @title Workflow .PipeLineExecution class
#' @description Internal class for encapsulating pipeline execution instances.
#' @keywords internal
#' @export
.PipeLineExecution = R6Class(".PipeLineExecution",
  public = list(

    #' @field arn
    #' The arn of the pipeline execution
    arn = NULL,

    #' @field sagemaker_session
    #'  Session object which manages interactions with Amazon SageMaker
    sagemaker_session = NULL,

    #' @description Initialize .PipeLineExecution class
    #' @param arn (str): The arn of the pipeline execution.
    #' @param sagemaker_session (sagemaker.session.Session): Session object which
    #'              manages interactions with Amazon SageMaker APIs and any other
    #'              AWS services needed. If not specified, the
    #'              pipeline creates one using the default AWS configuration chain.
    initialize = function(arn,
                          sagemaker_session=NULL){
      self$arn = arn
      self$sagemaker_session = sagemaker_session %||% sagemaker.core::Session$new()
    },

    #' @description Stops a pipeline execution.
    stop = function(){
      return(self$sagemaker_session$sagemaker$stop_pipeline_execution(
        PipelineExecutionArn=self$arn)
      )
    },

    #' @description Describes a pipeline execution.
    #' @return Information about the pipeline execution. See
    #'              `boto3 client describe_pipeline_execution
    #'              \url{https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_pipeline_execution}.
    describe = function(){
      return(self$sagemaker_session$sagemaker$describe_pipeline_execution(
        PipelineExecutionArn=self$arn)
      )
    },

    #' @description Describes a pipeline execution's steps.
    #' @return Information about the steps of the pipeline execution. See
    #'              `boto3 client list_pipeline_execution_steps
    #'              \url{https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.list_pipeline_execution_steps}.
    list_steps = function(){
      response = self$sagemaker_session$sagemaker$list_pipeline_execution_steps(
        PipelineExecutionArn=self$arn
      )
      return(response[["PipelineExecutionSteps"]])
    },

    #' @description Waits for a pipeline execution.
    #' @param delay (int): The polling interval. (Defaults to 30 seconds)
    #' @param max_attempts (int): The maximum number of polling attempts.
    #'              (Defaults to 60 polling attempts)
    wait = function(delay=30,
                    max_attempts=60){
      waiter_id = "PipelineExecutionComplete"
      for (attempt in seq_len(max_attemps)){
        writeLines("-", sep="")
        flush(stdout())

        response = self$sagemaker_session$sagemaker$describe_pipeline_execution(
          PipelineExecutionArn = self$arn
        )

        status=response[["PipelineExecutionStatus"]]

        if (status == "Succeeded"){
          writeLines("!", sep="\n")
          flush(stdout())
          LOGGER$info(response[["PipelineExecutionDescription"]])
          return(repsonse)
        } else if(status == "Stopped"){
          LOGGER$warn(paste(
            "PipeLine job ended with status 'Stopped' rather than ' Suceeded'.",
            "This could mean the PipeLine job timed out or stopped early for some other reason:",
            "Consider checking whether it completed as you expect.")
          )
          break
        } else if (status == "Failed"){
          writeLines("*", sep="\n")
          flush(stdout())
          message = sprintf("Error for %s: Failed. Reason: %s",
            response[["PipelineArn"]], response[["FailureReason"]] %||% "(No reason provided)")
          sagemaker.core::UnexpectedStatusError$new(message)
        }
        Sys.sleep(delay)
      }
    },

    #' @description format class
    format = function(){
      format_class(self)
    }
  )
)
