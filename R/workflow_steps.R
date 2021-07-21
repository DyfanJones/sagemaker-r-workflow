# NOTE: This code has been modified from AWS Sagemaker Python:
# https://github.com/aws/sagemaker-python-sdk/blob/master/src/sagemaker/workflow/steps.py

#' @include workflow_properties.R
#' @include workflow_functions.R
#' @include r_utils.R

#' @import R6
#' @import R6sagemaker.common
#' @import R6sagemaker.mlcore

#' @title Workflow StepTypeEnum class
#' @description Enum of step types.
#' @export
StepTypeEnum = Enum(
  CONDITION = "Condition",
  CREATE_MODEL = "Model",
  PROCESSING = "Processing",
  REGISTER_MODEL = "RegisterModel",
  TRAINING = "Training",
  TRANSFORM = "Transform",
  CALLBACK = "Callback",
  TUNING = "Tuning"
)

#' @title Workflow Step class
#' @description Pipeline step for workflow.
#' @export
Step = R6Class("Step",
  inherit = Entity,
  public = list(

    #' @field name
    #' The name of the step.
    name = NULL,

    #' @field step_type
    #' The type of the step.
    step_type = NULL,

    #' @field depends_on
    #' The list of step names the current step depends on
    depends_on = NULL,

    #' @description Initialize Workflow Step
    #' @param name (str): The name of the step.
    #' @param step_type (StepTypeEnum): The type of the step.
    #' @param depends_on (List[str]): The list of step names the current step depends on
    initialize = function(name,
                          step_type=enum_items(StepTypeEnum),
                          depends_on=NULL){
      self$name = name
      self$step_type = match.arg(step_type)
      self$depends_on = depends_on
    },

    #' @description Gets the request structure for workflow service calls.
    to_request = function(){
      request_dict = list(
        "Name"=self$name,
        "Type"=self$step_type,
        "Arguments"=self$arguments
      )
      if (!is.null(self$depends_on))
        request_dict[["DependsOn"]] = self$depends_on
      return(request_dict)
    },

    #' @description Add step names to the current step depends on list
    #' @param step_names (list): placeholder
    add_depends_on = function(step_names){
      if (missing(step_names))
        return(invisible(NULL))

      if (is.null(self.depends_on))
        self$depends_on = list()
      self$depends_on = c(self$depends_on, step_names)
    },

    #' @description formats class
    format = function(){
      format_class(self)
    }
  ),
  active = list(
    #' @field arguments
    #' The arguments to the particular step service call.
    arguments = function(){
      NotImplementedError$new()
    },

    #' @field properties
    #' The properties of the particular step.
    properties = function(){
      NotImplementedError$new()
    },

    #' @field ref
    #' Gets a reference dict for steps
    ref = function(){
      return(list(Name = self$name))
    }
  )
)

#' @title Workflow CacheConfig class
#' @description Configuration class to enable caching in pipeline workflow.
#' @export
CacheConfig = R6Class("CacheConfig",
  public = list(

    #' @description Initialize Workflow CacheConfig
    #'              If caching is enabled, the pipeline attempts to find a previous execution of a step
    #'              that was called with the same arguments. Step caching only considers successful execution.
    #'              If a successful previous execution is found, the pipeline propagates the values
    #'              from previous execution rather than recomputing the step. When multiple successful executions
    #'              exist within the timeout period, it uses the result for the most recent successful execution.
    #' @param enable_caching (bool): To enable step caching. Defaults to `FALSE`.
    #' @param expire_after (str): If step caching is enabled, a timeout also needs to defined.
    #'              It defines how old a previous execution can be to be considered for reuse.
    #'              Value should be an ISO 8601 duration string. Defaults to `NULL`.
    inititialize = function(enable_caching=FALSE,
                            expire_after=NULL){
      self$enable_caching = enable_caching
      self$expire_after = expire_after
    }
  ),
  active = list(

    #' @field config
    #' Configures caching in pipeline steps.
    config = function(){
      config = list("Enabled" = self$enable_caching)
      config[["ExpireAfter"]] = self$expire_after
      return(list("CacheConfig"=config))
    }
  )
)

#' @title Workflow TraingingStep class
#' @description Training step for workflow.
#' @export
TrainingStep = R6Class("TrainingStep",
  inherit = Step,
  public = list(

    #' @description Construct a TrainingStep, given an `EstimatorBase` instance.
    #'              In addition to the estimator instance, the other arguments are those that are supplied to
    #'              the `fit` method of the `sagemaker.estimator.Estimator`.
    #' @param name (str): The name of the training step.
    #' @param estimator (EstimatorBase): A `sagemaker.estimator.EstimatorBase` instance.
    #' @param inputs (str or dict or sagemaker.inputs.TrainingInput
    #'              or sagemaker.inputs.FileSystemInput): Information
    #'              about the training data. This can be one of three types:
    #'       \itemize(
    #'           \item((str) the S3 location where training data is saved, or a file:// path in
    #'                 local mode.)
    #'           \item((dict[str, str] or dict[str, sagemaker.inputs.TrainingInput]) If using multiple
    #'                 channels for training data, you can specify a dict mapping channel names to
    #'                 strings or :func:`~sagemaker.inputs.TrainingInput` objects.)
    #'           \item((sagemaker.inputs.TrainingInput) - channel configuration for S3 data sources
    #'                 that can provide additional information as well as the path to the training
    #'                 dataset.
    #'                 See :func:`sagemaker.inputs.TrainingInput` for full details.)
    #'           \item((sagemaker.inputs.FileSystemInput) - channel configuration for
    #'                 a file system data source that can provide additional information as well as
    #'                 the path to the training dataset.)
    #'           )
    #' @param cache_config (CacheConfig):  A `sagemaker.workflow.steps.CacheConfig` instance.
    #' @param depends_on (List[str]): A list of step names this `sagemaker.workflow.steps.TrainingStep`
    #'              depends on
    initialize = function(name,
                          estimator,
                          inputs=NULL,
                          cache_config=NULL,
                          depends_on=NULL){
      stopifnot(
        is.character(name),
        inherits(estimator, "EstimatorBase"),
        is.list(inputs) || is.character(inputs) || is.null(inputs),
        inherits(cache_config, "CacheConfig") || is.null(cache_config),
        is.list(depends_on) || is.null(depends_on)
      )
      super$initialize(name, StepTypeEnum$TRAINING, depends_on)
      self$estimator = estimator
      self$inputs = inputs
      private$.properties = Properties$new(
        path=sprintf("Steps.%s",name), shape_name="DescribeTrainingJobResponse"
      )
      self$cache_config = cache_config
    },

    #' @description A Properties object representing the DescribeTrainingJobResponse data model.
    to_request = function(){
      request_dict = super$to_request()
      if (!is.null(self$cache_config))
        request_dict = modifyList(request_dict, self$cache_config$config)

      return(request_dict)
    }
  ),
  active = list(

    #' @field arguments
    #' The arguments dict that is used to call `create_training_job`.
    #' NOTE: The CreateTrainingJob request is not quite the args list that workflow needs.
    #' The TrainingJobName and ExperimentConfig attributes cannot be included.
    arguments = function(){
      self$estimator$.prepare_for_training()
      train_args = self$estimator$.__enclos_env__$private$.get_train_args(
        self$inputs, experiment_config=list()
      )
      request_dict = .invoke(
        self$estimator$sagemaker_session$.__enclos_env__$private$.get_train_request,
        train_args
      )
      request_dict[["TrainingJobName"]] = NULL

      return(request_dict)
    },

    #' @field properties
    #' A Properties object representing the DescribeTrainingJobResponse data model.
    properties = function(){
      return(private$.properties)
    }
  ),
  private = list(
    .properties = NULL
  )
)

#' @title Workflow CreateModel class
#' @description CreateModel step for workflow.
#' @export
CreateModelStep = R6Class("CreateModelStep",
  inherit = Step,
  public = list(

    #' @description Construct a CreateModelStep, given an `sagemaker.model.Model` instance.
    #'              In addition to the Model instance, the other arguments are those that are supplied to
    #'              the `_create_sagemaker_model` method of the `sagemaker.model.Model._create_sagemaker_model`.
    #' @param name (str): The name of the CreateModel step.
    #' @param model (Model): A `sagemaker.model.Model` instance.
    #' @param inputs (CreateModelInput): A `sagemaker.inputs.CreateModelInput` instance.
    #'              Defaults to `None`.
    #' @param depends_on (List[str]): A list of step names this `sagemaker.workflow.steps.CreateModelStep`
    #'              depends on
    initialize = function(name,
                          model,
                          inputs,
                          depends_on=NULL){
      stopifnot(
        is.character(name),
        inherit(model, "Model"),
        inherit(inputs, "CreateModelInput"),
        is.list(depends_on) || is.null(depends_on)
      )

      super$initialize(name, StepTypeEnum$CREATE_MODEL, depends_on)
      self$model = model
      self$inputs = inputs %||% CreateModelInput$new()

      private$.properties = Properties$new(
        path=sprintf("Steps.%s", name), shape_name="DescribeModelOutput")
    }
  ),

  active = list(

    #' @field arguments
    #' The arguments dict that is used to call `create_model`.
    #' NOTE: The CreateModelRequest is not quite the args list that workflow needs.
    #' ModelName cannot be included in the arguments.
    arguments = function(){
      request_dict = self$model$sagemaker_session$.__enclos_env__$private$.create_model_request(
        name="",
        role=self$model$role,
        container_defs=self$model$prepare_container_def(
          instance_type=self$inputs$instance_type,
          accelerator_type=self$inputs$accelerator_type),
        vpc_config=self$model$vpc_config,
        enable_network_isolation=self$model$enable_network_isolation()
      )
      request_dict[["ModelName"]] = NULL

      return(request_dict)
    },

    #' @field properties
    #' A Properties object representing the DescribeModelResponse data model.
    properties = function(){
      return(private$.properties)
    }
  ),
  private = list(
    .properties = NULL
  )
)

#' @title Workflow TransformStep class
#' @description Transform step for workflow.
#' @export
TransformStep = R6Class("TransformStep",
  inherit = Step,
  public = list(

    #' @description Constructs a TransformStep, given an `Transformer` instance.
    #'              In addition to the transformer instance, the other arguments are those that are supplied to
    #'              the `transform` method of the `sagemaker.transformer.Transformer`.
    #' @param name (str): The name of the transform step.
    #' @param transformer (Transformer): A `sagemaker.transformer.Transformer` instance.
    #' @param inputs (TransformInput): A `sagemaker.inputs.TransformInput` instance.
    #' @param cache_config (CacheConfig):  A `sagemaker.workflow.steps.CacheConfig` instance.
    #' @param depends_on (List[str]): A list of step names this `sagemaker.workflow.steps.TransformStep`
    #'              depends on
    initialize = function(name,
                          transformer,
                          inputs,
                          cache_config=NULL,
                          depends_on=NULL){
      stopifnot(
        is.character(name),
        inherits(transformer, "Transformer"),
        inherits(inputs, "TransformInput"),
        inherits(cache_config, "CacheConfig") || is.null(cache_config),
        is.list(depends_on) || is.null(depends_on)
      )
      super$initialize(name, StepTypeEnum$TRANSFORM, depends_on)
      self$transformer = transformer
      self$inputs = inputs
      self$cache_config = cache_config
      private$.properties = Properties$new(
        path=sprintf("Steps.%s", name), shape_name="DescribeTransformJobResponse"
      )
    },

    #' @description Updates the dictionary with cache configuration.
    to_request = function(){
      request_dict = super$to_request()
      if (!is.null(self$cache_config))
        request_dict = modifyList(
          request_dict,
          self$cache_config$config
        )

      return(request_dict)
    }
  ),
  active = list(

    #' @field arguments
    #' The arguments dict that is used to call `create_transform_job`.
    #' NOTE: The CreateTransformJob request is not quite the args list that workflow needs.
    #' TransformJobName and ExperimentConfig cannot be included in the arguments.
    arguments = function(){
      transform_args = self$transformer$.__enclos_env__$private$.get_transform_args(
        data=self$inputs$data,
        data_type=self$inputs$data_type,
        content_type=self$inputs$content_type,
        compression_type=self$inputs$compression_type,
        split_type=self$inputs$split_type,
        input_filter=self$inputs$input_filter,
        output_filter=self$inputs$output_filter,
        join_source=self$inputs$join_source,
        model_client_config=self$inputs$model_client_config,
        experiment_config=list()
      )

      request_dict = .invoke(
        self$transformer$sagemaker_session$.__enclos_env__$private$.get_transform_request,
        transform_args
      )
    },

    #' @field properties
    #' A Properties object representing the DescribeTransformJobResponse data model.
    properties = function(){
      return(private$.properties)
    }
  ),
  private = list(
    .properties = NULL
  )
)

#' @title Workflow ProcessingStep Class
#' @description Processing step for workflow.
#' @export
ProcessingStep = R6Class("ProcessingStep",
  inherit = Step,
  public = list(

    #' @description Construct a ProcessingStep, given a `Processor` instance.
    #'              In addition to the processor instance, the other arguments are those that are supplied to
    #'              the `process` method of the `sagemaker.processing.Processor`.
    #' @param name (str): The name of the processing step.
    #' @param processor (Processor): A `sagemaker.processing.Processor` instance.
    #' @param inputs (List[ProcessingInput]): A list of `sagemaker.processing.ProcessorInput`
    #'              instances. Defaults to `None`.
    #' @param outputs (List[ProcessingOutput]): A list of `sagemaker.processing.ProcessorOutput`
    #'              instances. Defaults to `None`.
    #' @param job_arguments (List[str]): A list of strings to be passed into the processing job.
    #'              Defaults to `None`.
    #' @param code (str): This can be an S3 URI or a local path to a file with the framework
    #'              script to run. Defaults to `None`.
    #' @param property_files (List[PropertyFile]): A list of property files that workflow looks
    #'              for and resolves from the configured processing output list.
    #' @param cache_config (CacheConfig):  A `sagemaker.workflow.steps.CacheConfig` instance.
    #' @param depends_on (List[str]): A list of step names this `sagemaker.workflow.steps.ProcessingStep`
    #'              depends on
    initialize = function(name,
                          processor,
                          inputs=NULL,
                          outputs=NULL,
                          job_arguments=NULL,
                          code=NULL,
                          property_files=NULL,
                          cache_config=NULL,
                          depends_on=NULL){
      stopifnot(
        is.character(name),
        inherits(processor, "Processor"),
        is.list(inputs) || is.null(inputs),
        is.list(outputs) || is.null(outputs),
        is.list(job_arguments) || is.null(job_arguments),
        is.character(code) || is.null(code),
        is.list(property_files) || is.null(property_files),
        inherits(cache_config, "CacheConfig") || is.null(cache_config),
        is.list(depends_on) || is.null(depends_on)
      )
      super$initialize(name, StepTypeEnum$PROCESSING, depends_on)
      self$processor = processor
      self$inputs = inputs
      self$outputs = outputs
      self$job_arguments = job_arguments
      self$code = code
      self$property_files = property_files

      # Examine why run method in sagemaker.processing.Processor mutates the processor instance
      # by setting the instance's arguments attribute. Refactor Processor.run, if possible.
      self$processor$arguments = job_arguments

      private$.properties = Properties$new(
        path=sprintf("Steps.%s", name), shape_name="DescribeProcessingJobResponse"
      )
      self$cache_config = cache_config
    },

    #' @description Get the request structure for workflow service calls.
    to_request = function(){
      request_dict = super$to_request()
      if (!is.null(self$cache_config))
        request_dict = modifyList(
          request_dict,
          self$cache_config$config
        )
      if (!is.null(self$property_files))
        request_dict[["PropertyFiles"]] = lapply(
          self$property_files, function(property_file) property_file$expr
        )
      return(request_dict)
    }
  ),

  active = list(

    #' @field arguments
    #' The arguments dict that is used to call `create_processing_job`.
    #' NOTE: The CreateProcessingJob request is not quite the args list that workflow needs.
    #' ProcessingJobName and ExperimentConfig cannot be included in the arguments.
    arguments = function(){
      ll = self$processor$.__enclos_env__$private$.normalize_args(
        arguments=self$job_arguments,
        inputs=self$inputs,
        outputs=self$outputs,
        code=self$code
      )
      process_args = ProcessingJob$new()$.__enclos_env__$private$.get_process_args(
        self$processor, ll$normalized_inputs, ll$normalized_outputs, experiment_config=list()
      )
      request_dict = .invoke(
        self$processor$sagemaker_session._get_process_request,
        process_args
      )
      request_dict[["ProcessingJobName"]] = NULL

      return(request_dict)
    },

    #' @field properties
    #' A Properties object representing the DescribeProcessingJobResponse data model.
    properties = function(){
      return(private$.properties)
    }
  ),
  private = list(
    .properties = NULL
  )
)

#' @title Workflow TuningStep class
#' @description Tuning step for workflow.
#' @export
TuningStep = R6Class("TuningStep",
  inherit = Step,
  public = list(

    #' @description Construct a TuningStep, given a `HyperparameterTuner` instance.
    #'              In addition to the tuner instance, the other arguments are those that are supplied to
    #'              the `fit` method of the `sagemaker.tuner.HyperparameterTuner`.
    #' @param name (str): The name of the tuning step.
    #' @param tuner (HyperparameterTuner): A `sagemaker.tuner.HyperparameterTuner` instance.
    #' @param inputs : Information about the training data. Please refer to the
    #'              ``fit()`` method of the associated estimator, as this can take
    #'              any of the following forms:
    #'        \itemize{
    #'           \item{(str) - The S3 location where training data is saved.}
    #'           \item{(dict[str, str] or dict[str, sagemaker.inputs.TrainingInput]) -
    #'                 If using multiple channels for training data, you can specify
    #'                 a dict mapping channel names to strings or
    #'                 :func:`~sagemaker.inputs.TrainingInput` objects.}
    #'           \item{(sagemaker.inputs.TrainingInput) - Channel configuration for S3 data sources
    #'                 that can provide additional information about the training dataset.
    #'                 See :func:`sagemaker.inputs.TrainingInput` for full details.}
    #'           \item{(sagemaker.session.FileSystemInput) - channel configuration for
    #'                 a file system data source that can provide additional information as well as
    #'                 the path to the training dataset.}
    #'           \item{(sagemaker.amazon.amazon_estimator.RecordSet) - A collection of
    #'                 Amazon :class:~`Record` objects serialized and stored in S3.
    #'                 For use with an estimator for an Amazon algorithm.}
    #'           \item{(sagemaker.amazon.amazon_estimator.FileSystemRecordSet) -
    #'                 Amazon SageMaker channel configuration for a file system data source for
    #'                 Amazon algorithms.}
    #'           \item{(list[sagemaker.amazon.amazon_estimator.RecordSet]) - A list of
    #'                 :class:~`sagemaker.amazon.amazon_estimator.RecordSet` objects,
    #'                 where each instance is a different channel of training data.}
    #'           \item{(list[sagemaker.amazon.amazon_estimator.FileSystemRecordSet]) - A list of
    #'                 :class:~`sagemaker.amazon.amazon_estimator.FileSystemRecordSet` objects,
    #'                 where each instance is a different channel of training data.}
    #'          }
    #' @param job_arguments (List[str]): A list of strings to be passed into the processing job.
    #'              Defaults to `None`.
    #' @param cache_config (CacheConfig):  A `sagemaker.workflow.steps.CacheConfig` instance.
    #' @param depends_on (List[str]): A list of step names this `sagemaker.workflow.steps.ProcessingStep`
    #'              depends on
    initialize = function(name,
                          tuner,
                          inputs=NULL,
                          job_arguments=NULL,
                          cache_config=NULL,
                          depends_on=NULL){
      stopifnot(
        is.character(name),
        inherits(tuner, "HyperparameterTuner"),
        is.list(outputs) || is.null(outputs),
        is.list(job_arguments) || is.null(job_arguments),
        inherits(cache_config, "CacheConfig") || is.null(cache_config),
        is.list(depends_on) || is.null(depends_on)
      )
      super$initialize(name, StepTypeEnum$TUNING, depends_on)
      self$tuner = tuner
      self$inputs = inputs
      self$job_arguments = job_arguments
      private$.properties = Properties$new(
        path=sprintf("Steps.%s", name),
        shape_names=c(
          "DescribeHyperParameterTuningJobResponse",
          "ListTrainingJobsForHyperParameterTuningJobResponse")
      )
      self$cache_config = cache_config
    },

    #' @description Updates the dictionary with cache configuration.
    to_request = function(){
      request_dict = super$to_request()
      if (!is.null(self.cache_config))
        request_dict = modifyList(request_dict, self$cache_config$config)

      return(request_dict)
    },

    #' @description Get the model artifact s3 uri from the top performing training jobs.
    #' @param top_k (int): the index of the top performing training job
    #'              tuning step stores up to 50 top performing training jobs, hence
    #'              a valid top_k value is from 0 to 49. The best training job
    #'              model is at index 0
    #' @param s3_bucket (str): the s3 bucket to store the training job output artifact
    #' @param prefix (str): the s3 key prefix to store the training job output artifact
    get_top_model_s3_uri = function(top_k,
                                    s3_bucket,
                                    prefix=""){
      stopifnot(
        is.integer(top_k),
        is.character(s3_bucket),
        is.character(prefix) || is.null(prefix)
      )
      values = list("s3:/", s3_bucket)
      if (prefix != "" && !is.null(prefix))
        values = c(values, prefix)

      return(Join$new(
        on="/",
        values=c(
          values,
          self$properties$TrainingJobSummaries[[top_k]]$TrainingJobName,
          "output/model.tar.gz")
          )
      )
    }
  ),
  active = list(

    #' @field arguments
    #' The arguments dict that is used to call `create_hyper_parameter_tuning_job`.
    #' NOTE: The CreateHyperParameterTuningJob request is not quite the
    #' args list that workflow needs.
    #' The HyperParameterTuningJobName attribute cannot be included.
    arguments = function(){
      if (!is.null(self$tuner$estimator)) {
        self$tuner$estimator$.prepare_for_training()
      } else {
        for (estimator in self$tuner$estimator_list){
          estimator$.prepare_for_training()
        }
      }

      self$tuner$.__enclos_env__$private$.prepare_for_tuning()
      tuner_args = self$tuner$.__enclos_env__$private$..get_tuner_args(self$inputs)
      request_dict = .invoke(self$tuner$sagemaker_session$.__enclos_env__$private$.get_tuning_request, tuner_args)
      request_dict[["HyperParameterTuningJobName"]] = NULL

      return(request_dict)
    },

    #' @field properties
    #' A Properties object representing
    #' `DescribeHyperParameterTuningJobResponse` and
    #' `ListTrainingJobsForHyperParameterTuningJobResponse` data model.
    properties = function(){
      return(private$.properties)
    }
  ),
  private = list(
    .properties = NULL
  )
)
