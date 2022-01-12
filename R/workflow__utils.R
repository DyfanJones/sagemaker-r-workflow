# NOTE: This code has been modified from AWS Sagemaker Python:
# https://github.com/aws/sagemaker-python-sdk/blob/master/src/sagemaker/workflow/_utils.py

#' @include workflow_entities.R
#' @include workflow_properties.R
#' @include workflow_steps.R
#' @include r_utils.R

#' @import fs
#' @import R6
#' @import sagemaker.common
#' @import sagemaker.mlcore
#' @import sagemaker.mlframework

FRAMEWORK_VERSION = "0.23-1"
INSTANCE_TYPE = "ml.m5.large"
REPACK_SCRIPT = "_repack_model.py"

#' @title Workflow .RepackModelStep class
#' @description Repacks model artifacts with inference entry point.
#' @keywords internal
.RepackModelStep = R6Class(".RepackModelStep",
  inherit = TrainingStep,
  public = list(

    #' @field name
    #' The name of the training step.
    name = NULL,

    #' @field step_type
    #' The type of the step with value `StepTypeEnum.Training`.
    step_type = NULL,

    #' @field estimator
    #' A `sagemaker.estimator.EstimatorBase` instance.
    estimator = NULL,

    #' @field inputs
    #' A `sagemaker.inputs.TrainingInput` instance. Defaults to `None`.
    inputs = NULL,

    #' @field sagemaker_session
    #' `sagemaker.common::Session` class
    sagemaker_session = NULL,

    #' @field role
    #' execution role
    role = NULL,

    #' @description Constructs a TrainingStep, given an `EstimatorBase` instance.
    #'              In addition to the estimator instance, the other arguments are those that are supplied to
    #'              the `fit` method of the `sagemaker.estimator.Estimator`.
    #' @param name (str): The name of the training step.
    #' @param sagemaker_session (EstimatorBase): A `sagemaker.estimator.EstimatorBase` instance.
    #' @param role (str) : placeholder
    #' @param model_data : placeholder
    #' @param entry_point : placeholder
    #' @param source_dir : placeholder
    #' @param dependencies : placeholder
    #' @param depends_on : placeholder
    #' @param ... : additional parameters passed `sagemaker.mlframework::SKLearn` class
    initialize = function(name,
                          sagemaker_session,
                          role,
                          model_data,
                          entry_point,
                          source_dir=NULL,
                          dependencies=NULL,
                          depends_on=NULL,
                          ...){
      # yeah, go ahead and save the originals for now
      private$.model_data = model_data
      self$sagemaker_session = sagemaker_session
      self$role = role
      if (inherits(model_data, "Properties")){
        private$.model_prefix = model_data
        private$.model_archive = "model.tar.gz"
      } else {
        private$.model_prefix = paste(split_str(private$.model_data, "/"), collapse = "/")
        mod_arc = split_str(private$.model_data, "/")
        private$.model_archive = mod_arc[length(mod_arc)]
      }
      private$.entry_point = entry_point
      private$.entry_point_basename = basename(private$.entry_point)
      private$.source_dir = source_dir
      private$.dependencies = dependencies

      # the real estimator and inputs
      repacker = sagemaker.mlframework::SKLearn$new(
        framework_version=FRAMEWORK_VERSION,
        instance_type=INSTANCE_TYPE,
        entry_point=REPACK_SCRIPT,
        source_dir=private$.ource_dir,
        dependencies=private$.dependencies,
        sagemaker_session=self$sagemaker_session,
        role=self$role,
        hyperparameters=list(
          "inference_script"=private$.entry_point_basename,
          "model_archive"=private$.model_archive),
        ...
      )
      repacker$disable_profiler = TRUE
      inputs = sagemaker.common::TrainingInput$new(private$.model_prefix)

      super$initialize(
        name=name, depends_on=depends_on, estimator=repacker, inputs=inputs
      )
    }
  ),
  private = list(
    .model_data = NULL,
    .model_prefix = NULL,
    .model_archive = NULL,
    .entry_point = NULL,
    .entry_point_basename = NULL,
    .source_dir = NULL,
    .dependencies = NULL,

    # Prepares the source for the estimator.
    .prepare_for_repacking = function(){
      if (!is.null(private$.source_dir))
        private$.establish_source_dir()

      private$.inject_repack_script()
    },

    # If the source_dir is None, creates it for the repacking job.
    # It performs the following:
    #   1) creates a source directory
    #   2) copies the inference_entry_point inside it
    #   3) copies the repack_model.py inside it
    #   4) sets the source dir for the repacking estimator
    .establish_source_dir = function(){
      private$.source_dir = tempfile()
      self.estimator.source_dir = self._source_dir

      fs::file_copy(private$.entry_point, fs::path(private$.source_dir, private$.entry_point_basename))
      private$.entry_point = private$.entry_point_basename
    },

    # Injects the _repack_model.py script where it belongs.
    # If the source_dir is an S3 path:
    #   1) downloads the source_dir tar.gz
    #   2) copies the _repack_model.py script where it belongs
    #   3) uploads the mutated source_dir
    # If the source_dir is a local path:
    #   1) copies the _repack_model.py script into the source dir
    .inject_repack_script = function(){

    }
  )
)

#' @title Workflow .RegisterModelStep class
#' @description Register model step in workflow that creates a model package.
#' @keywords interal
.RegisterModelStep = R6Class(".RegisterModelStep",
  inherit = Step,
  public = list(

    #' @description Constructor of a register model step.
    #' @param name (str): The name of the training step.
    #' @param step_type (StepTypeEnum): The type of the step with value `StepTypeEnum.Training`.
    #' @param estimator (EstimatorBase): A `sagemaker.estimator.EstimatorBase` instance.
    #' @param model_data : the S3 URI to the model data from training.
    #' @param content_types (list): The supported MIME types for the input data (default: None).
    #' @param response_types (list): The supported MIME types for the output data (default: None).
    #' @param inference_instances (list): A list of the instance types that are used to
    #'              generate inferences in real-time (default: None).
    #' @param transform_instances (list): A list of the instance types on which a transformation
    #'              job can be run or on which an endpoint can be deployed (default: None).
    #' @param model_package_group_name (str): Model Package Group name, exclusive to
    #'              `model_package_name`, using `model_package_group_name` makes the Model Package
    #'              versioned (default: None).
    #' @param model_metrics (ModelMetrics): ModelMetrics object (default: None).
    #' @param metadata_properties (MetadataProperties): MetadataProperties object (default: None).
    #' @param approval_status (str): Model Approval Status, values can be "Approved", "Rejected",
    #'              or "PendingManualApproval" (default: "PendingManualApproval").
    #' @param image_uri (str): The container image uri for Model Package, if not specified,
    #'              Estimator's training container image will be used (default: None).
    #' @param compile_model_family (str): Instance family for compiled model, if specified, a compiled
    #'              model will be used (default: None).
    #' @param description (str): Model Package description (default: None).
    #' @param depends_on (List[str]): A list of step names this `sagemaker.workflow.steps.TrainingStep`
    #'              depends on
    #' @param tags : Placeholder
    #' @param container_def_list (list): A list of containers.
    #' @param ... : additional arguments to `create_model`.
    initialize = function(name,
                          content_types,
                          response_types,
                          inference_instances,
                          transform_instances,
                          estimator=NULL,
                          model_data=NULL,
                          model_package_group_name=NULL,
                          model_metrics=NULL,
                          metadata_properties=NULL,
                          approval_status="PendingManualApproval",
                          image_uri=NULL,
                          compile_model_family=NULL,
                          description=NULL,
                          depends_on=NULL,
                          tags=NULL,
                          container_def_list=NULL,
                          ...){
      kwargs = list(...)
      super$initialize(name, StepTypeEnum$REGISTER_MODEL, depends_on)
      self$estimator = estimator
      self$model_data = model_data
      self$content_types = content_types
      self$response_types = response_types
      self$inference_instances = inference_instances
      self$transform_instances = transform_instances
      self$model_package_group_name = model_package_group_name
      self$model_metrics = model_metrics
      self$metadata_properties = metadata_properties
      self$approval_status = approval_status
      self$image_uri = image_uri
      self$compile_model_family = compile_model_family
      self$description = description
      self$tags = tags
      self$kwargs = kwargs
      self$container_def_list = container_def_list

      private$.properties = Properties$new(
        path=sprintf("Steps.%s",name), shape_name="DescribeModelPackageResponse"
      )
    }
  ),
  active = list(

    #' @field arguments
    #' The arguments dict that are used to call `create_model_package`.
    arguments = function(){
      model_name = self$name

      if (is.null(self$container_def_list)){
        if (!is.null(self$compile_model_family)){
          model = self$estimator$.compiled_models[[self$compile_model_family]]
          self$model_data = model$model_data
        } else {
          # create_model wants the estimator to have a model_data attribute...
          self$estimator$.current_job_name = model_name

          # placeholder. replaced with model_data later
          output_path = self$estimator$output_path
          self$estimator$output_path = "/tmp"

          # create the model, but custom funky framework stuff going on in some places
          if (!is.null(self$image_uri)){
            image_uri=self$image_uri
            kwargs = c(image_uri=self$image_uri ,self$kwargs)
            model = do.call(self$estimator$create_model, kwargs)
          } else {
            model = do.call(self$estimator.create_model, self$kwargs)
            self$image_uri = model$image_uri
          }
          # reset placeholder
          self.estimator.output_path = output_path

          # yeah, there is some framework stuff going on that we need to pull in here
          if (is.null(self$image_uri)){
            region_name = self$estimator$sagemaker_session$paws_region_name
            self$image_uri = ImageUris$new()$retrieve(
              attr(model, "_framework_name"),
              region_name,
              version=model$framework_version,
              py_version= model$py_version,
              instance_type=self$kwargs[["instance_type"]] %||% self$estimator$instance_type,
              accelerator_type=self$kwargs[["accelerator_type"]],
              image_scope="inference")
            model$name = model_name
            model$model_data = self$model_data
          }
        }
      }
      model_package_args = get_model_package_args(
        content_types=self$content_types,
        response_types=self$response_types,
        inference_instances=self$inference_instances,
        transform_instances=self$transform_instances,
        model_package_group_name=self$model_package_group_name,
        model_data=self$model_data,
        image_uri=self$image_uri,
        model_metrics=self$model_metrics,
        metadata_properties=self$metadata_properties,
        approval_status=self$approval_status,
        description=self$description,
        tags=self$tags,
        container_def_list=self$container_def_list
      )
      request_dict = do.call(
        Session$private_methods$.get_create_model_package_request, model_package_args
      )
      # these are not available in the workflow service and will cause rejection
      if ("CertifyForMarketplace" %in% names(request_dict))
        request_dict[["CertifyForMarketplace"]] = NULL
      if ("Description" %in% names(request_dict))
        request_dict[["Description"]] = NULL

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
  ),
  lock_objects = F
)
