# NOTE: This code has been modified from AWS Sagemaker Python:
# https://github.com/aws/sagemaker-python-sdk/blob/master/src/sagemaker/workflow/check_job_config.py

#' @import R6
#' @import sagemaker.core
#' @import sagemaker.mlcore

#' @title CheckJobConfig class
#' @description Check job config for QualityCheckStep and ClarifyCheckStep
#' @export
CheckJobConfig = R6Class("CheckJobConfig",
  public = list(

    #' @description Constructs a CheckJobConfig instance.
    #' @param role (str): An AWS IAM role. The Amazon SageMaker jobs use this role.
    #' @param instance_count (int): The number of instances to run the jobs with (default: 1).
    #' @param instance_type (str): Type of EC2 instance to use for the job
    #'              (default: 'ml.m5.xlarge').
    #' @param volume_size_in_gb (int): Size in GB of the EBS volume
    #'              to use for storing data during processing (default: 30).
    #' @param volume_kms_key (str): A KMS key for the processing volume (default: None).
    #' @param output_kms_key (str): The KMS key id for the job's outputs (default: None).
    #' @param max_runtime_in_seconds (int): Timeout in seconds. After this amount of
    #'              time, Amazon SageMaker terminates the job regardless of its current status.
    #'              Default: 3600 if not specified
    #' @param base_job_name (str): Prefix for the job name. If not specified,
    #'              a default name is generated based on the training image name and
    #'              current timestamp (default: None).
    #' @param sagemaker_session (sagemaker.session.Session): Session object which
    #'              manages interactions with Amazon SageMaker APIs and any other
    #'              AWS services needed (default: None). If not specified, one is
    #'              created using the default AWS configuration chain.
    #' @param env (dict): Environment variables to be passed to the job (default: None).
    #' @param tags ([dict]): List of tags to be passed to the job (default: None).
    #' @param network_config (sagemaker.network.NetworkConfig): A NetworkConfig
    #'              object that configures network isolation, encryption of
    #'              inter-container traffic, security group IDs, and subnets (default: None).
    initialize = function(role,
                          instance_count=1,
                          instance_type="ml.m5.xlarge",
                          volume_size_in_gb=30,
                          volume_kms_key=NULL,
                          output_kms_key=NULL,
                          max_runtime_in_seconds=NULL,
                          base_job_name=NULL,
                          sagemaker_session=NULL,
                          env=NULL,
                          tags=NULL,
                          network_config=NULL){
      self$role = role
      self$instance_count = instance_count
      self$instance_type = instance_type
      self$volume_size_in_gb = volume_size_in_gb
      self$volume_kms_key = volume_kms_key
      self$output_kms_key = output_kms_key
      self$max_runtime_in_seconds = max_runtime_in_seconds
      self$base_job_name = base_job_name
      self$sagemaker_session = sagemaker_session %||% sagemaker.core::Session$new()
      self$env = env
      self$tags = tags
      self$network_config = network_config
    },

    #' @description Generates a ModelMonitor object
    #'              Generates a ModelMonitor object with required config attributes for
    #'              QualityCheckStep and ClarifyCheckStep
    #' @param mm_type (str): The subclass type of ModelMonitor object.
    #'              A valid mm_type should be one of the following: "DefaultModelMonitor",
    #'              "ModelQualityMonitor", "ModelBiasMonitor", "ModelExplainabilityMonitor"
    #' @return sagemaker.model_monitor.ModelMonitor or None if the mm_type is not valid
    .generate_model_monitor = function(mm_type){
      stopifnot(is.character(mm_type))

      if (mm_type == "DefaultModelMonitor") {
        monitor = sagemaker.mlcore::DefaultModelMonitor$new(
          role=self$role,
          instance_count=self$instance_count,
          instance_type=self$instance_type,
          volume_size_in_gb=self$volume_size_in_gb,
          volume_kms_key=self$volume_kms_key,
          output_kms_key=self$output_kms_key,
          max_runtime_in_seconds=self$max_runtime_in_seconds,
          base_job_name=self$base_job_name,
          sagemaker_session=self$sagemaker_session,
          env=self$env,
          tags=self$tags,
          network_config=self$network_config
        )
      } else if (mm_type == "ModelQualityMonitor") {
        monitor = sagemaker.mlcore::ModelQualityMonitor$new(
          role=self$role,
          instance_count=self$instance_count,
          instance_type=self$instance_type,
          volume_size_in_gb=self$volume_size_in_gb,
          volume_kms_key=self$volume_kms_key,
          output_kms_key=self$output_kms_key,
          max_runtime_in_seconds=self$max_runtime_in_seconds,
          base_job_name=self$base_job_name,
          sagemaker_session=self$sagemaker_session,
          env=self$env,
          tags=self$tags,
          network_config=self$network_config
        )
      } else if (mm_type == "ModelBiasMonitor") {
        monitor = sagemaker.mlcore::ModelBiasMonitor$new(
          role=self$role,
          instance_count=self$instance_count,
          instance_type=self$instance_type,
          volume_size_in_gb=self$volume_size_in_gb,
          volume_kms_key=self$volume_kms_key,
          output_kms_key=self$output_kms_key,
          max_runtime_in_seconds=self$max_runtime_in_seconds,
          base_job_name=self$base_job_name,
          sagemaker_session=self$sagemaker_session,
          env=self$env,
          tags=self$tags,
          network_config=self$network_config
        )
      } else if (mm_type == "ModelExplainabilityMonitor") {
        monitor = sagemaker.mlcore::ModelExplainabilityMonitor$new(
          role=self$role,
          instance_count=self$instance_count,
          instance_type=self$instance_type,
          volume_size_in_gb=self$volume_size_in_gb,
          volume_kms_key=self$volume_kms_key,
          output_kms_key=self$output_kms_key,
          max_runtime_in_seconds=self$max_runtime_in_seconds,
          base_job_name=self$base_job_name,
          sagemaker_session=self$sagemaker_session,
          env=self$env,
          tags=self$tags,
          network_config=self$network_config
        )
      } else {
        LOGGER$warn(paste(
          'Expected model monitor types: "DefaultModelMonitor", "ModelQualityMonitor",',
          '"ModelBiasMonitor", "ModelExplainabilityMonitor"'
          )
        )
        return(NULL)
      }
      return(monitor)
    },

    #' @description Format class
    format = function(){
      format_cls(self)
    }
  ),
  lock_objects = F
)
