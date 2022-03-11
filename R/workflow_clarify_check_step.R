# NOTE: This code has been modified from AWS Sagemaker Python:
# https://github.com/aws/sagemaker-python-sdk/blob/master/src/sagemaker/workflow/check_job_config.py

#' @include r_utils.R

#' @import R6
#' @import sagemaker.core
#' @import sagemaker.common
#' @import sagemaker.mlcore
#' @importFrom fs path
#' @importFrom jsonlite write_json
#' @importFrom stats setNames

.DATA_BIAS_TYPE = "DATA_BIAS"
.MODEL_BIAS_TYPE = "MODEL_BIAS"
.MODEL_EXPLAINABILITY_TYPE = "MODEL_EXPLAINABILITY"
.BIAS_MONITORING_CFG_BASE_NAME = "bias-monitoring"
.EXPLAINABILITY_MONITORING_CFG_BASE_NAME = "model-explainability-monitoring"

#' @title Clarify Check Config
#' @export
ClarifyCheckConfig = R6Class("ClarifyCheckConfig",
  public = list(

    #' @field data_config
    #'        Config of the input/output data.
    data_config = NULL,

    #' @field kms_key
    #'        The ARN of the KMS key that is used to encrypt the
    #'        user code file
    kms_key = NULL,

    #' @field monitoring_analysis_config_uri
    #' The uri of monitoring analysis config.
    monitoring_analysis_config_uri = NULL,

    #' @description Initialize ClarifyCheckConfig class
    #' @param data_config (DataConfig): Config of the input/output data.
    #' @param kms_key (str): The ARN of the KMS key that is used to encrypt the
    #'              user code file (default: None).
    #'              This field CANNOT be any of PipelineNonPrimitiveInputTypes.
    #' @param monitoring_analysis_config_uri (str): The uri of monitoring analysis config.
    #'              This field does not take input.
    #'              It will be generated once uploading the created analysis config file.
    initialize = function(data_config,
                          kms_key=NULL,
                          monitoring_analysis_config_uri=NULL){
      stopifnot(
        inherits(data_config, "DataConfig"),
        is.character(kms_key) || is.null(kms_key),
        is.character(monitoring_analysis_config_uri) || is.null(monitoring_analysis_config_uri)
      )

      self$data_config = data_config
      self$kms_key = kms_key
      self$monitoring_analysis_config_uri = monitoring_analysis_config_uri
    },

    #' @description format class
    format = function(){
      format_class(self)
    }
  )
)

#' @title Data Bias Check Config
#' @export
DataBiasCheckConfig = R6Class("DataBiasCheckConfig",
  inherit = ClarifyCheckConfig,
  public = list(

    #' @field data_bias_config
    #' Config of sensitive groups
    data_bias_config = NULL,

    #' @field methods
    #' Selector of a subset of potential metrics
    methods = NULL,

    #' @description Initialize DataBiasCheckConfig class
    #' @param data_bias_config (BiasConfig): Config of sensitive groups.
    #' @param methods (str or list[str]): Selector of a subset of potential metrics:
    #'     \itemize{
    #'         \item{"CI" \url{https://docs.aws.amazon.com/sagemaker/latest/dg/clarify-bias-metric-class-imbalance.html}},
    #'         \item{"DPL" \url{https://docs.aws.amazon.com/sagemaker/latest/dg/clarify-data-bias-metric-true-label-imbalance.html}},
    #'         \item{"KL" \url{https://docs.aws.amazon.com/sagemaker/latest/dg/clarify-data-bias-metric-kl-divergence.html}},
    #'         \item{"JS" \url{https://docs.aws.amazon.com/sagemaker/latest/dg/clarify-data-bias-metric-jensen-shannon-divergence.html}},
    #'         \item{"LP" \url{https://docs.aws.amazon.com/sagemaker/latest/dg/clarify-data-bias-metric-lp-norm.html}},
    #'         \item{"TVD" \url{https://docs.aws.amazon.com/sagemaker/latest/dg/clarify-data-bias-metric-total-variation-distance.html}},
    #'         \item{"KS" \url{https://docs.aws.amazon.com/sagemaker/latest/dg/clarify-data-bias-metric-kolmogorov-smirnov.html}},
    #'         \item{"CDDL" \url{https://docs.aws.amazon.com/sagemaker/latest/dg/clarify-data-bias-metric-cddl.html}}
    #'     }
    #'             Defaults to computing all.
    #'             This field CANNOT be any of PipelineNonPrimitiveInputTypes.
    #' @param ... : Parameters from ClarifyCheckConfig
    initialize = function(data_bias_config,
                          methods="all",
                          ...){
      stopifnot(
        inherits(data_bias_config, "BiasConfig"),
        is.character(methods) || is.list(methods)
      )

      self$data_bias_config = data_bias_config
      self$methods = methods
      do.call(super$initialize, list(...))
    }
  )
)

#' @title Model Bias Check Config
#' @export
ModelBiasCheckConfig = R6Class("ModelBiasCheckConfig",
  inherit = ClarifyCheckConfig,
  public = list(

    #' @field data_bias_config
    #' Config of sensitive groups
    data_bias_config = NULL,

    #' @field model_config
    #' Config of the model and its endpoint to be created
    model_config = NULL,

    #' @field model_predicted_label_config
    #' Config of how to extract the predicted label from the model output
    model_predicted_label_config = NULL,

    #' @field methods
    #' Selector of a subset of potential metrics
    methods = NULL,

    #' @description Initialize DataBiasCheckConfig class
    #' @param data_bias_config (BiasConfig): Config of sensitive groups.
    #' @param model_config (ModelConfig): Config of the model and its endpoint to be created.
    #' @param model_predicted_label_config (ModelPredictedLabelConfig): Config of how to
    #'              extract the predicted label from the model output.
    #' @param methods (str or list[str]): Selector of a subset of potential metrics:
    #'     \itemize{
    #'         \item{"DPPL"\url{https://docs.aws.amazon.com/sagemaker/latest/dg/clarify-post-training-bias-metric-dppl.html}},
    #'         \item{"DI" \url{https://docs.aws.amazon.com/sagemaker/latest/dg/clarify-post-training-bias-metric-di.html}},
    #'         \item{"DCA" \url{https://docs.aws.amazon.com/sagemaker/latest/dg/clarify-post-training-bias-metric-dca.html}},
    #'         \item{"DCR" \url{https://docs.aws.amazon.com/sagemaker/latest/dg/clarify-post-training-bias-metric-dcr.html}},
    #'         \item{"RD" \url{https://docs.aws.amazon.com/sagemaker/latest/dg/clarify-post-training-bias-metric-rd.html}},
    #'         \item{"DAR" \url{https://docs.aws.amazon.com/sagemaker/latest/dg/clarify-post-training-bias-metric-dar.html}},
    #'         \item{"DRR" \url{https://docs.aws.amazon.com/sagemaker/latest/dg/clarify-post-training-bias-metric-drr.html}},
    #'         \item{"AD" \url{https://docs.aws.amazon.com/sagemaker/latest/dg/clarify-post-training-bias-metric-ad.html}},
    #'         \item{"CDDPL" \url{https://docs.aws.amazon.com/sagemaker/latest/dg/clarify-post-training-bias-metric-cddpl.html}},
    #'         \item{"TE" \url{https://docs.aws.amazon.com/sagemaker/latest/dg/clarify-post-training-bias-metric-te.html}},
    #'         \item{"FT" \url{https://docs.aws.amazon.com/sagemaker/latest/dg/clarify-post-training-bias-metric-ft.html}}
    #'     }
    #'             Defaults to computing all.
    #'             This field CANNOT be any of PipelineNonPrimitiveInputTypes.
    #' @param ... : Parameters from ClarifyCheckConfig
    initialize = function(data_bias_config,
                          model_config,
                          model_predicted_label_config,
                          methods = "all",
                          ...){
      stopifnot(
        inherits(data_bias_config, "BiasConfig"),
        inherits(model_config, "ModelConfig"),
        inherits(model_predicted_label_config, "ModelPredictedLabelConfig"),
        is.character(methods) || is.list(methods)
      )
      self$data_bias_config = data_bias_config
      self$model_config = model_config
      self$model_predicted_label_config = model_predicted_label_config
      self$methods = methods
      do.call(super$initialize, list(...))
    }
  )
)

#' @title Model Explainability Check Config
#' @export
ModelExplainabilityCheckConfig = R6Class("ModelExplainabilityCheckConfig",
  inherit = ClarifyCheckConfig,
  public = list(

    #' @field model_config
    #' Config of the model and its endpoint to be created
    model_config = NULL,

    #' @field explainability_config
    #' Config of the specific explainability method
    explainability_config = NULL,

    #' @field model_scores
    #' Index or JSONPath location in the model output
    model_scores = NULL,

    #' @description Initialize ModelExplainabilityCheckConfig class
    #' @param model_config (ModelConfig): Config of the model and its endpoint to be created.
    #' @param explainability_config (SHAPConfig): Config of the specific explainability method.
    #'              Currently, only SHAP is supported.
    #' @param model_scores (str or int or ModelPredictedLabelConfig): Index or JSONPath location
    #'              in the model output for the predicted scores to be explained (default: None).
    #'              This is not required if the model output is a single score. Alternatively,
    #'              an instance of ModelPredictedLabelConfig can be provided
    #'              but this field CANNOT be any of PipelineNonPrimitiveInputTypes.
    #' @param ... : Parameters from ClarifyCheckConfig
    initialize = function(model_config,
                          explainability_config,
                          model_scores = NULL,
                          ...){
      stopifnot(
        inherits(model_config, "ModelConfig"),
        inherits(explainability_config, "SHAPConfig"),
        (
          is.character(model_scores) ||
          is.numeric(model_scores) ||
          inherits(model_scores, "ModelPredictedLabelConfig") ||
          is.null(model_scores)
        )
      )
      self$model_config =  model_config
      self$explainability_config = explainability_config
      self$model_scores = model_scores
      do.call(super$initialize, list(...))
    }
  )
)

#' @title ClarifyCheckStep step for workflow.
#' @export
ClarifyCheckStep = R6Class("ClarifyCheckStep",
  inherit = Step,
  public = list(

    #' @description Constructs a ClarifyCheckStep.
    #' @param name (str): The name of the ClarifyCheckStep step.
    #' @param clarify_check_config (ClarifyCheckConfig): A ClarifyCheckConfig instance.
    #' @param check_job_config (CheckJobConfig): A CheckJobConfig instance.
    #' @param skip_check (bool or PipelineNonPrimitiveInputTypes): Whether the check
    #'              should be skipped (default: False).
    #' @param register_new_baseline (bool or PipelineNonPrimitiveInputTypes): Whether
    #'              the new baseline should be registered (default: False).
    #' @param model_package_group_name (str or PipelineNonPrimitiveInputTypes): The name of a
    #'              registered model package group, among which the baseline will be fetched
    #'              from the latest approved model (default: None).
    #' @param supplied_baseline_constraints (str or PipelineNonPrimitiveInputTypes): The S3 path
    #'              to the supplied constraints object representing the constraints JSON file
    #'              which will be used for drift to check (default: None).
    #' @param display_name (str): The display name of the ClarifyCheckStep step (default: None).
    #' @param description (str): The description of the ClarifyCheckStep step (default: None).
    #' @param cache_config (CacheConfig):  A `sagemaker.workflow.steps.CacheConfig` instance
    #'              (default: None).
    #' @param depends_on (List[str] or List[Step]): A list of step names or step instances
    #'              this `sagemaker.workflow.steps.ClarifyCheckStep` depends on (default: None).
    initialize = function(name,
                          clarify_check_config,
                          check_job_config,
                          skip_check = FALSE,
                          register_new_baseline = FALSE,
                          model_package_group_name = NULL,
                          supplied_baseline_constraints = NULL,
                          display_name = NULL,
                          description = NULL,
                          cache_config = NULL,
                          depends_on = NULL){
      if (
        !inherits(clarify_check_config, "DataBiasCheckConfig")
        & !inherits(clarify_check_config, "ModelBiasCheckConfig")
        & !inherits(clarify_check_config, "ModelExplainabilityCheckConfig")
      ){
        RuntimeError$new(
          "The clarify_check_config can only be object of ",
          "DataBiasCheckConfig, ModelBiasCheckConfig or ModelExplainabilityCheckConfig"
        )
      }
      if (inherits(
        clarify_check_config$data_config$s3_analysis_config_output_path,
        c("ExecutionVariable", "Expression", "Parameter", "Properties"))
      ){
        RuntimeError$new(
          "s3_analysis_config_output_path cannot be of type ",
          "ExecutionVariable/Expression/Parameter/Properties"
        )
      }
      if (empty(clarify_check_config$data_config$s3_analysis_config_output_path) &
          inherits(clarify_check_config$data_config$s3_output_path,
            c("ExecutionVariable", "Expression", "Parameter", "Properties"))
      ){
        RuntimeError$new(
          "`s3_output_path` cannot be of type ExecutionVariable/Expression/Parameter",
          "/Properties if `s3_analysis_config_output_path` is NULL or empty "
        )
      }
      super$initialize(
        name, display_name, description, StepTypeEnum$CLARIFY_CHECK, depends_on
      )
      self$skip_check = skip_check
      self$register_new_baseline = register_new_baseline
      self$clarify_check_config = clarify_check_config
      self$check_job_config = check_job_config
      self$model_package_group_name = model_package_group_name
      self$supplied_baseline_constraints = supplied_baseline_constraints
      self$cache_config = cache_config

      if (inherits(self$clarify_check_config, "ModelExplainabilityCheckConfig")) {
        private$.model_monitor = self$check_job_config$.generate_model_monitor(
          "ModelExplainabilityMonitor"
        )
      } else {
        private$.model_monitor = self$check_job_config$.generate_model_monitor("ModelBiasMonitor")
      }
      self$clarify_check_config$monitoring_analysis_config_uri = (
        private$.upload_monitoring_analysis_config()
      )
      private$.baselining_processor = private$.model_monitor$.__enclos_env__$private$.create_baselining_processor()
      private$.processing_params = private$.generate_processing_job_parameters(
        private$.generate_processing_job_analysis_config(), private$.baselining_processor
      )

      root_path = sprintf("Steps.%s", name)
      root_prop = Properties$new(path=root_path)
      root_prop[["CalculatedBaselineConstraints"]] = Properties$new(
        sprintf("%s.CalculatedBaselineConstraints", root_path)
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

      if (inherits(self$clarify_check_config, "DataBiasCheckConfig")) {
        request_dict[["CheckType"]] = .DATA_BIAS_TYPE
      } else if (inherits(self$clarify_check_config, "ModelBiasCheckConfig")) {
        request_dict[["CheckType"]] = .MODEL_BIAS_TYPE
      } else {
        request_dict[["CheckType"]] = .MODEL_EXPLAINABILITY_TYPE
      }
      request_dict[["ModelPackageGroupName"]] = self$model_package_group_name
      request_dict[["SkipCheck"]] = self$skip_check
      request_dict[["RegisterNewBaseline"]] = self$register_new_baseline
      request_dict[["SuppliedBaselineConstraints"]] = self$supplied_baseline_constraints
      if (inherits(
        self$clarify_check_config, c("ModelBiasCheckConfig", "ModelExplainabilityCheckConfig"))
      ) {
        request_dict[[
          "ModelName"
        ]] = self$clarify_check_config$model_config$get_predictor_config()[["model_name"]]
      }
      return(request_dict)
    }
  ),

  active = list(

    #' @field arguments
    #'        The arguments dict that is used to define the ClarifyCheck step.
    arguments = function(){
      ll = setNames(
        private$.baselining_processor$.__enclos_env__$private$.normalize_args(
          inputs=list(private$.processing_params[["config_input"]], private$.processing_params[["data_input"]]),
          outputs=list(private$.processing_params[["result_output"]])
          ), c("normalized_inputs", "normalized_outputs")
        )
      process_args = sagemaker.common::ProcessingJob$private_methods$.get_process_args(
        private$.baselining_processor,
        ll$normalized_inputs,
        ll$normalized_outputs,
        experiment_config=list()
      )

      request_dict = do.call(
        private$.baselining_processor$sagemaker_session$.__enclos_env__$private$.get_process_request,
        process_args
      )

      request_dict$ProcessingJobName = NULL

      return(request_dict)
    },

    #' @field properties
    #'        A Properties object representing the output parameters of the ClarifyCheck step.
    properties = function(){
      return(private$.properties)
    }
  ),
  private = list(

    # Generate the clarify processing job analysis config
    # Returns:
    #   dict: processing job analysis config dictionary.
    .generate_processing_job_analysis_config = function(){
      analysis_config = self$clarify_check_config$data_config$get_config()
      if (inherits(self$clarify_check_config, "DataBiasCheckConfig")){
        analysis_config = modifyList(analysis_config, self$clarify_check_config$data_bias_config$get_config())
        analysis_config[["methods"]] = list(
          "pre_training_bias"=list("methods"=self$clarify_check_config$methods)
        )
      } else if (inherits(self$clarify_check_config, "ModelBiasCheckConfig")) {
        analysis_config =  modifyList(
          analysis_config, self$clarify_check_config$data_bias_config$get_config()
        )
        ll = setNames(
          self$clarify_check_config$model_predicted_label_config$get_predictor_config(),
          c("probability_threshold", "predictor_config")
        )
        ll$predictor_config = modifyList(
          ll$predictor_config, self$clarify_check_config$model_config$get_predictor_config())
        ll[["predictor_config"]][["model_name"]] = NULL
        analysis_config[["methods"]] = list(
          "post_training_bias"=list("methods"=self$clarify_check_config$methods)
        )
        analysis_config[["predictor"]] = ll$predictor_config
        .set(ll$probability_threshold, "probability_threshold", analysis_config)
      } else {
        predictor_config = self$clarify_check_config$model_config$get_predictor_config()
        predictor_config[["model_name"]] = NULL
        model_scores = self$clarify_check_config$model_scores
        if (inherits(model_scores, "ModelPredictedLabelConfig")){
           ll = setNames(model_scores$get_predictor_config(),
             c("probability_threshold", "predicted_label_config")
           )
          .set(ll$probability_threshold, "probability_threshold", analysis_config)
          predictor_config = modifyList(ll$predictor_config, ll$predicted_label_config)
        } else {
          .set(model_scores, "label", predictor_config)
        }
        analysis_config[[
          "methods"
        ]] = self$clarify_check_config$explainability_config$get_explainability_config()
        analysis_config[["predictor"]] = predictor_config
      }
      return(analysis_config)
    },

    # Generates input and output parameters for the clarify processing job
    # Args:
    #   analysis_config (dict): A clarify processing job analysis config
    # baselining_processor (SageMakerClarifyProcessor): A SageMakerClarifyProcessor instance
    # Returns:
    #   dict: with two ProcessingInput objects as the clarify processing job inputs and
    # a ProcessingOutput object as the clarify processing job output parameter
    .generate_processing_job_parameters = function(analysis_config,
                                                   baselining_processor){
      data_config = self$clarify_check_config$data_config
      analysis_config[["methods"]][["report"]] = list(
        "name"="report", "title"="Analysis Report"
      )

      tmpdirname = tempdir()
      analysis_config_file = fs::path(tmpdirname, "analysis_config.json")
      jsonlite::write_json(analysis_config, analysis_config_file, auto_ubox = TRUE)

      s3_analysis_config_file = .upload_analysis_config(
        analysis_config_file,
        data_config$s3_analysis_config_output_path %||% data_config$s3_output_path,
        baselining_processor$sagemaker_session,
        self$clarify_check_config$kms_key
      )
      config_input = sagemaker.common::ProcessingInput$new(
        input_name="analysis_config",
        source=s3_analysis_config_file,
        destination=sagemaker.common::SageMakerClarifyProcessor$private_fields$.CLARIFY_CONFIG_INPUT,
        s3_data_type="S3Prefix",
        s3_input_mode="File",
        s3_compression_type="None"
      )
      data_input = sagemaker.common::ProcessingInput$new(
        input_name="dataset",
        source=data_config$s3_data_input_path,
        destination=sagemaker.common::SageMakerClarifyProcessor$private_fields$.CLARIFY_DATA_INPUT,
        s3_data_type="S3Prefix",
        s3_input_mode="File",
        s3_data_distribution_type=data_config$s3_data_distribution_type,
        s3_compression_type=data_config$s3_compression_type
      )
      result_output = sagemaker.common::ProcessingOutput$new(
        source=sagemaker.common::SageMakerClarifyProcessor$private_fields$.CLARIFY_OUTPUT,
        destination=data_config$s3_output_path,
        output_name="analysis_result",
        s3_upload_mode="EndOfJob"
      )
      return(list(config_input=config_input, data_input=data_input, result_output=result_output))
    },

    # Generate and upload monitoring schedule analysis config to s3
    # Returns:
    #   str: The S3 uri of the uploaded monitoring schedule analysis config
    .upload_monitoring_analysis_config = function(){
      output_s3_uri = private$.get_s3_base_uri_for_monitoring_analysis_config()

      if (inherits(self$clarify_check_config, "ModelExplainabilityCheckConfig")){
        # Explainability analysis doesn't need label
        headers = self$clarify_check_config$data_config$headers
        if (!is.null(headers) && (self$clarify_check_config$data_config$label) %in% names(headers))
          headers[self$clarify_check_config$data_config$label] = NULL
        explainability_analysis_config = sagemaker.mlcore::ExplainabilityAnalysisConfig$new(
          explainability_config=self$clarify_check_config$explainability_config,
          model_config=self$clarify_check_config$model_config,
          headers=headers
        )
        analysis_config = explainability_analysis_config$to_list()
        analysis_config[["predictor"]][["model_name"]] = NULL
        job_definition_name = sagemaker.core::name_from_base(
          sprintf("%s-config", .EXPLAINABILITY_MONITORING_CFG_BASE_NAME)
        )
      } else {
        bias_analysis_config = sagemaker.mlcore::BiasAnalysisConfig$new(
          bias_config=self$clarify_check_config$data_bias_config,
          headers=self$clarify_check_config$data_config.headers,
          label=self$clarify_check_config$data_config$label
        )
        analysis_config = bias_analysis_config$to_list()
        job_definition_name = sagemaker.core::name_from_base(
          sprintf("%s-config", .BIAS_MONITORING_CFG_BASE_NAME)
        )
      }
      return (private$.model_monitor$.__enclos_env__$private$.upload_analysis_config(
        analysis_config, output_s3_uri, job_definition_name)
      )
    },

    # Generate s3 base uri for monitoring schedule analysis config
    # Returns:
    #   str: The S3 base uri of the monitoring schedule analysis config
    .get_s3_base_uri_for_monitoring_analysis_config = function(){
      s3_analysis_config_output_path = (
        self$clarify_check_config$data_config$s3_analysis_config_output_path
      )
      monitoring_cfg_base_name = sprintf("%s-configuration", .BIAS_MONITORING_CFG_BASE_NAME)
      if (inherits(self$clarify_check_config, "ModelExplainabilityCheckConfig"))
        monitoring_cfg_base_name = sprintf(
          "%s-configuration", .EXPLAINABILITY_MONITORING_CFG_BASE_NAME
        )
      if (!is.null(s3_analysis_config_output_path))
        return(sagemaker.core::s3_path_join(
          s3_analysis_config_output_path,
          monitoring_cfg_base_name)
        )
      return(sagemaker.core::s3_path_join(
        "s3://",
        private$.model_monitor$sagemaker_session$default_bucket(),
        .MODEL_MONITOR_S3_PATH,
        monitoring_cfg_base_name)
      )
    }
  ),
  lock_objects = F
)
