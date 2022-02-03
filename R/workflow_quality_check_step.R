# NOTE: This code has been modified from AWS Sagemaker Python:
# https://github.com/aws/sagemaker-python-sdk/blob/master/src/sagemaker/workflow/quality_check_step.py

#' @include r_utils.R
#' @include workflow_entities.R
#' @include workflow_properties.R
#' @include workflow_steps.R
#' @include workflow_check_job_config.R

#' @import R6
#' @import sagemaker.core
#' @import sagemaker.common
#' @import sagemaker.mlcore
#' @importFrom stats setNames

.CONTAINER_BASE_PATH = "/opt/ml/processing"
.CONTAINER_INPUT_PATH = "input"
.CONTAINER_OUTPUT_PATH = "output"
.BASELINE_DATASET_INPUT_NAME = "baseline_dataset_input"
.RECORD_PREPROCESSOR_SCRIPT_INPUT_NAME = "record_preprocessor_script_input"
.POST_ANALYTICS_PROCESSOR_SCRIPT_INPUT_NAME = "post_analytics_processor_script_input"
.MODEL_MONITOR_S3_PATH = "model-monitor"
.BASELINING_S3_PATH = "baselining"
.RESULTS_S3_PATH = "results"
.DEFAULT_OUTPUT_NAME = "quality_check_output"
.MODEL_QUALITY_TYPE = "MODEL_QUALITY"
.DATA_QUALITY_TYPE = "DATA_QUALITY"


#' @title QualityCheckConfig class
#' @description Quality Check Config.
#' @export
QualityCheckConfig = R6Class("QualityCheckConfig",
  public = list(

    #' @field baseline_dataset
    #' str or PipelineNonPrimitiveInputTypes): The path to the
    #' baseline_dataset file. This can be a local path or an S3 uri string
    baseline_dataset = NULL,

    #' @field dataset_format
    #' (dict): The format of the baseline_dataset.
    dataset_format=NULL,

    #' @field output_s3_uri
    #' (str or PipelineNonPrimitiveInputTypes): Desired S3 destination of
    #' the constraint_violations and statistics json files (default: None).
    #' If not specified an auto generated path will be used:
    #' "s3://<default_session_bucket>/model-monitor/baselining/<job_name>/results"
    output_s3_uri=NULL,

    #' @field post_analytics_processor_script
    #' (str): The path to the record post-analytics
    #' processor script (default: None). This can be a local path or an S3 uri string
    #' but CANNOT be any of PipelineNonPrimitiveInputTypes.
    post_analytics_processor_script=NULL
  )
)

#' @title DataQualityCheckConfig class
#' @description Data Quality Check Config.
#' @export
DataQualityCheckConfig = R6Class("DataQualityCheckConfig",
  inherit = QualityCheckConfig,
  public = list(

    #' @field record_preprocessor_script
    #' (str): The path to the record preprocessor script
    #' (default: None).
    #' This can be a local path or an S3 uri string
    #' but CANNOT be any of PipelineNonPrimitiveInputTypes.
    record_preprocessor_script=NULL
  )
)

#' @title ModelQualityCheckConfig Class
#' @description Model Quality Check Config.
#' @export
ModelQualityCheckConfig = R6Class("ModelQualityCheckConfig",
  inherit = QualityCheckConfig,
  public = list(

    #' @field problem_type
    #' (str or PipelineNonPrimitiveInputTypes): The type of problem of this model
    #' quality monitoring.
    #' Valid values are "Regression", "BinaryClassification", "MulticlassClassification".
    problem_type = NULL,

    #' @field inference_attribute
    #' (str or PipelineNonPrimitiveInputTypes): Index or JSONpath to
    #' locate predicted label(s) (default: None).
    inference_attribute = NULL,

    #' @field probability_attribute
    #' (str or PipelineNonPrimitiveInputTypes): Index or JSONpath to
    #' locate probabilities (default: None).
    probability_attribute = NULL,

    #' @field ground_truth_attribute
    #' (str or PipelineNonPrimitiveInputTypes: Index or JSONpath to
    #' locate actual label(s) (default: None).
    ground_truth_attribute = NULL,

    #' @field probability_threshold_attribute
    #' (str or PipelineNonPrimitiveInputTypes): Threshold to
    #' convert probabilities to binaries (default: None).
    probability_threshold_attribute = NULL
  )
)

#' @title QualityCheckStep class
#' @description QualityCheck step for workflow.
#' @export
QualityCheckStep = R6Class("QualityCheckStep",
  inherit = Step,
  public = list(

    #' @description Constructs a QualityCheckStep.
    #' @param name (str): The name of the QualityCheckStep step.
    #' @param quality_check_config (QualityCheckConfig): A QualityCheckConfig instance.
    #' @param check_job_config (CheckJobConfig): A CheckJobConfig instance.
    #' @param skip_check (bool or PipelineNonPrimitiveInputTypes): Whether the check
    #'              should be skipped (default: False).
    #' @param register_new_baseline (bool or PipelineNonPrimitiveInputTypes): Whether
    #'              the new baseline should be registered (default: False).
    #' @param model_package_group_name (str or PipelineNonPrimitiveInputTypes): The name of a
    #'              registered model package group, among which the baseline will be fetched
    #'              from the latest approved model (default: None).
    #' @param supplied_baseline_statistics (str or PipelineNonPrimitiveInputTypes): The S3 path
    #'              to the supplied statistics object representing the statistics JSON file
    #'              which will be used for drift to check (default: None).
    #' @param supplied_baseline_constraints (str or PipelineNonPrimitiveInputTypes): The S3 path
    #'              to the supplied constraints object representing the constraints JSON file
    #'              which will be used for drift to check (default: None).
    #' @param display_name (str): The display name of the QualityCheckStep step (default: None).
    #' @param description (str): The description of the QualityCheckStep step (default: None).
    #' @param cache_config (CacheConfig):  A `sagemaker.workflow.steps.CacheConfig` instance
    #'              (default: None).
    #' @param depends_on (List[str] or List[Step]): A list of step names or step instances
    #'              this `sagemaker.workflow.steps.QualityCheckStep` depends on (default: None).
    initialize = function(name,
                          quality_check_config,
                          check_job_config,
                          skip_check=FALSE,
                          register_new_baseline=FALSE,
                          model_package_group_name=NULL,
                          supplied_baseline_statistics=NULL,
                          supplied_baseline_constraints=NULL,
                          display_name=NULL,
                          description=NULL,
                          cache_config=NULL,
                          depends_on=NULL){
      stopifnot(
        is.character(name),
        inherits(quality_check_config, "QualityCheckConfig"),
        inherits(check_job_config, "CheckJobConfig"),
        is.logical(skip_check),
        is.logical(register_new_baseline),
        is.null(model_package_group_name) || is.character(model_package_group_name),
        is.null(supplied_baseline_constraints) || is.character(supplied_baseline_constraints),
        is.null(display_name) || is.character(display_name),
        is.null(description) || is.character(description),
        is.null(cache_config) || inherits(cache_config, "CacheConfig"),
        is.null(depends_on) || is.list(depends_on)
      )
      if (!inherits(quality_check_config, "DataQualityCheckConfig") && !inherits(
        quality_check_config, "ModelQualityCheckConfig")
      ){
        RuntimeError$new(
          "The quality_check_config can only be object of ",
          "DataQualityCheckConfig or ModelQualityCheckConfig"
        )
      }
      super$initialize(
        name, display_name, description, StepTypeEnum$QUALITY_CHECK, depends_on
      )
      self$skip_check = skip_check
      self$register_new_baseline = register_new_baseline
      self$check_job_config = check_job_config
      self$quality_check_config = quality_check_config
      self$model_package_group_name = model_package_group_name
      self$supplied_baseline_statistics = supplied_baseline_statistics
      self$supplied_baseline_constraints = supplied_baseline_constraints
      self$cache_config = cache_config

      if (inherits(self$quality_check_config, "DataQualityCheckConfig")) {
        private$.model_monitor = self$check_job_config$.__enclos_env__$private$.generate_model_monitor(
          "DefaultModelMonitor"
        )
      } else {
        private$.model_monitor = self$check_job_config$.__enclos_env__$private$.generate_model_monitor(
          "ModelQualityMonitor"
        )
      }
      private$.model_monitor$latest_baselining_job_name = (
        private$.model_monitor$.__enclos_env__$private$.generate_baselining_job_name()
      )

      baseline_job_inputs_with_nones = private$.generate_baseline_job_inputs()
      private$.baseline_job_inputs = Filter(Negate(is.null), baseline_job_inputs_with_nones)

      private$.baseline_output = private$.generate_baseline_output()
      private$.baselining_processor = private$.generate_baseline_processor(
        baseline_dataset_input=baseline_job_inputs_with_nones[["baseline_dataset_input"]],
        baseline_output=private$.baseline_output,
        post_processor_script_input=baseline_job_inputs_with_nones[[
          "post_processor_script_input"
        ]],
        record_preprocessor_script_input=baseline_job_inputs_with_nones[[
          "record_preprocessor_script_input"
        ]]
      )

      root_path = sprintf("Steps.%s", name)
      root_prop = Properties$new(path=root_path)
      root_prop[["CalculatedBaselineConstraints"]] = Properties$new(
        sprintf("%s.CalculatedBaselineConstraints",root_path)
      )
      root_prop[["CalculatedBaselineStatistics"]] = Properties$new(
        sprintf("%s.CalculatedBaselineStatistics",root_path)
      )
      root_prop[["BaselineUsedForDriftCheckStatistics"]] = Properties$new(
        sprintf("%s.BaselineUsedForDriftCheckStatistics", root_path)
      )
      root_prop[["BaselineUsedForDriftCheckConstraints"]] = Properties$new(
        sprintf("%s.BaselineUsedForDriftCheckConstraints", root_path)
      )
      private$.properties = root_prop
    },

    #' @description Updates the dictionary with cache configuration etc.
    to_request = function(){
      request_dict = super$to_request()
      if (!is.null(self$cache_config))
        request_dict = modifyList(request_dict, self$cache_config$config)

      if (inherits(self$quality_check_config, "DataQualityCheckConfig")){
        request_dict[["CheckType"]] = .DATA_QUALITY_TYPE
      } else {
        request_dict[["CheckType"]] = .MODEL_QUALITY_TYPE
      }
      request_dict[["ModelPackageGroupName"]] = self$model_package_group_name
      request_dict[["SkipCheck"]] = self$skip_check
      request_dict[["RegisterNewBaseline"]] = self$register_new_baseline
      request_dict[["SuppliedBaselineStatistics"]] = self$supplied_baseline_statistics
      request_dict[["SuppliedBaselineConstraints"]] = self$supplied_baseline_constraints
      return(request_dict)
    }
  ),
  active = list(

    #' @field arguments
    #' The arguments dict that is used to define the QualityCheck step.
    arguments = function(){
      ll = setNames(private$.baselining_processor$.__enclos_env__$private$.normalize_args(
        inputs=private$.baseline_job_inputs,
        outputs=list(private$.baseline_output)),
        c("normalized_inputs", "normalized_outputs")
      )
      process_args = ProcessingJob$private_method$.get_process_args(
        private$.baselining_processor,
        ll$normalized_inputs,
        ll$normalized_outputs,
        experiment_config=list()
      )
      request_dict = do.call(
        private$.baselining_processor$sagemaker_session$.__enclos_env__$private$.get_process_request,
        process_args
      )
      request_dict[["ProcessingJobName"]] = NULL

      return(request_dict)
    },

    #' @field properties
    #' A Properties object representing the output parameters of the QualityCheck step.
    properties = function(){
      return(private$.properties)
    }
  ),
  private = list(

    # Generates a dict with ProcessingInput objects
    # Generates a dict with three ProcessingInput objects: baseline_dataset_input,
    # post_processor_script_input and record_preprocessor_script_input
    # Returns:
    #   dict: with three ProcessingInput objects as baseline job inputs
    .generate_baseline_job_inputs = function(){
      baseline_dataset = self$quality_check_config.baseline_dataset
      baseline_dataset_des = file.path(
          .CONTAINER_BASE_PATH, .CONTAINER_INPUT_PATH, .BASELINE_DATASET_INPUT_NAME,
          fsep="/"
        )
      if (inherits(baseline_dataset, c("ExecutionVariable", "Expression", "Parameter", "Properties"))) {
        baseline_dataset_input = ProcessingInput$new(
          source=self$quality_check_config$baseline_dataset,
          destination=baseline_dataset_des,
          input_name=.BASELINE_DATASET_INPUT_NAME
        )
      } else {
        baseline_dataset_input = (
          private$.model_monitor$.__enclos_env__$private$.upload_and_convert_to_processing_input(
            source=self$quality_check_config$baseline_dataset,
            destination=baseline_dataset_des,
            name=.BASELINE_DATASET_INPUT_NAME
          )
        )
      }
      post_processor_script_input = (
        private$.model_monitor$.__enclos_env__$private$.upload_and_convert_to_processing_input(
          source=self$quality_check_config$post_analytics_processor_script,
          destination=file.path(
            .CONTAINER_BASE_PATH,
            .CONTAINER_INPUT_PATH,
            .POST_ANALYTICS_PROCESSOR_SCRIPT_INPUT_NAME,
            fsep="/"
          ),
          name=.POST_ANALYTICS_PROCESSOR_SCRIPT_INPUT_NAME
        )
      )
      record_preprocessor_script_input = NULL
      if (inherits(self$quality_check_config, "DataQualityCheckConfig"))
        record_preprocessor_script_input = (
          private$.model_monitor$.__enclos_env__$private$.upload_and_convert_to_processing_input(
            source=self$quality_check_config$record_preprocessor_script,
            destination=file.path(
              .CONTAINER_BASE_PATH,
              .CONTAINER_INPUT_PATH,
              .RECORD_PREPROCESSOR_SCRIPT_INPUT_NAME,
              fsep="/"
            ),
            name=.RECORD_PREPROCESSOR_SCRIPT_INPUT_NAME
          )
        )
      return(list(
        baseline_dataset_input=baseline_dataset_input,
        post_processor_script_input=post_processor_script_input,
        record_preprocessor_script_input=record_preprocessor_script_input
        )
      )
    },

    # Generates a ProcessingOutput object
    # Returns:
    #   sagemaker.processing.ProcessingOutput: The normalized ProcessingOutput object
    .generte_baseline_output = function(){
      s3_uri = self$quality_check_config$output_s3_uri %||% s3_path_join(
        "s3://",
        private$.model_monitor$sagemaker_session$default_bucket(),
        .MODEL_MONITOR_S3_PATH,
        .BASELINING_S3_PATH,
        private$.model_monitor$latest_baselining_job_name,
        .RESULTS_S3_PATH
      )
      return(ProcessingOutput$new(
        source=file.path(.CONTAINER_BASE_PATH, .CONTAINER_OUTPUT_PATH, fsep="/"),
        destination=s3_uri,
        output_name=.DEFAULT_OUTPUT_NAME
        )
      )
    },

    # Generates a baseline processor
    # Args:
    #   baseline_dataset_input (ProcessingInput): A ProcessingInput instance for baseline
    # dataset input.
    # baseline_output (ProcessingOutput): A ProcessingOutput instance for baseline
    # dataset output.
    # post_processor_script_input (ProcessingInput): A ProcessingInput instance for
    # post processor script input.
    # record_preprocessor_script_input (ProcessingInput): A ProcessingInput instance for
    # record preprocessor script input.
    # Returns:
    #   sagemaker.processing.Processor: The baseline processor
    .generate_baseline_processor = function(baseline_dataset_input,
                                            baseline_output,
                                            post_processor_script_input=NULL,
                                            record_preprocessor_script_input=NULL){
      quality_check_cfg = self$quality_check_config
      # Unlike other input, dataset must be a directory for the Monitoring image.
      baseline_dataset_container_path = baseline_dataset_input$destination

      post_processor_script_container_path = NULL
      if (!is.null(post_processor_script_input))
        post_processor_script_container_path = file.path(
          post_processor_script_input$destination,
          basename(quality_check_cfg$post_analytics_processor_script),
          fsep="/"
        )

      record_preprocessor_script_container_path = NULL
      if (inherits(quality_check_cfg, "DataQualityCheckConfig")){
        if (!is.null(record_preprocessor_script_input)){
          record_preprocessor_script_container_path = file.path(
            record_preprocessor_script_input.destination,
            basename(quality_check_cfg$record_preprocessor_script),
            fsep = "/"
          )
        }
        normalized_env = ModelMonitor$private_methods$.generate_env_map(
          env=private$.model_monitor$env,
          dataset_format=quality_check_cfg$dataset_format,
          output_path=baseline_output$source,
          enable_cloudwatch_metrics=FALSE,  # Only supported for monitoring schedules
          dataset_source_container_path=baseline_dataset_container_path,
          record_preprocessor_script_container_path=record_preprocessor_script_container_path,
          post_processor_script_container_path=post_processor_script_container_path
        )
      } else {
        inference_attribute = (
          if (!is.null(quality_check_cfg$inference_attribute))
            as.character(quality_check_cfg$inference_attribute)
          else NULL
        )
        probability_attribute = (
          if (!is.null(quality_check_cfg.probability_attribute))
            as.character(quality_check_cfg.probability_attribute)
          else NULL
        )
        ground_truth_attribute = (
          if (!is.null(quality_check_cfg.ground_truth_attribute))
            as.character(quality_check_cfg$ground_truth_attribute)
          else NULL
        )
        probability_threshold_attr = (
          if (!is.null(quality_check_cfg.probability_threshold_attribute))
            as.character(quality_check_cfg$probability_threshold_attribute)
          else NULL
        )
        normalized_env = ModelMonitor$private_method$.generate_env_map(
          env=private$.model_monitor$env,
          dataset_format=quality_check_cfg$dataset_format,
          output_path=baseline_output$source,
          enable_cloudwatch_metrics=FALSE,  # Only supported for monitoring schedules
          dataset_source_container_path=baseline_dataset_container_path,
          post_processor_script_container_path=post_processor_script_container_path,
          analysis_type=.MODEL_QUALITY_TYPE,
          problem_type=quality_check_cfg$problem_type,
          inference_attribute=inference_attribute,
          probability_attribute=probability_attribute,
          ground_truth_attribute=ground_truth_attribute,
          probability_threshold_attribute=probability_threshold_attr
        )
      }
      return(Processor$new(
        role=private$.model_monitor$role,
        image_uri=private$.model_monitor$image_uri,
        instance_count=private$.model_monitor$instance_count,
        instance_type=private$.model_monitor$instance_type,
        entrypoint=private$.model_monitor$entrypoint,
        volume_size_in_gb=private$.model_monitor$volume_size_in_gb,
        volume_kms_key=private$.model_monitor$volume_kms_key,
        output_kms_key=private$.model_monitor$output_kms_key,
        max_runtime_in_seconds=private$.model_monitor$max_runtime_in_seconds,
        base_job_name=private$.model_monitor$base_job_name,
        sagemaker_session=private$.model_monitor$sagemaker_session,
        env=normalized_env,
        tags=private$.model_monitor$tags,
        network_config=private$.model_monitor$network_config)
      )
    }
  )
)
