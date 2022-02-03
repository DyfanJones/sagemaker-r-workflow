# NOTE: This code has been modified from AWS Sagemaker Python:
# https://github.com/aws/sagemaker-python-sdk/blob/master/src/sagemaker/wrangler/ingestion.py

#' @import R6
#' @import sagemaker.core
#' @import sagemaker.common

#' @title DataWranglerProcessor class
#' @description Handles Amazon SageMaker DataWrangler tasks
#' @export
DataWranglerProcessor = R6Class(
  inherit = sagemaker.common::Processor,
  public = list(

    #' @description Initializes a ``Processor`` instance.
    #'              The ``Processor`` handles Amazon SageMaker Processing tasks.
    #' @param role (str): An AWS IAM role name or ARN. Amazon SageMaker Processing
    #'              uses this role to access AWS resources, such as
    #'              data stored in Amazon S3.
    #' @param data_wrangler_flow_source (str): The source of the DaraWrangler flow which will be
    #'              used for the DataWrangler job. If a local path is provided, it will automatically
    #'              be uploaded to S3 under:
    #'              "s3://<default-bucket-name>/<job-name>/input/<input-name>".
    #' @param instance_count (int): The number of instances to run
    #'              a processing job with.
    #' @param instance_type (str): The type of EC2 instance to use for
    #'              processing, for example, 'ml.c4.xlarge'.
    #' @param volume_size_in_gb (int): Size in GB of the EBS volume
    #'              to use for storing data during processing (default: 30).
    #' @param volume_kms_key (str): A KMS key for the processing
    #'              volume (default: None).
    #' @param output_kms_key (str): The KMS key ID for processing job outputs (default: None).
    #' @param max_runtime_in_seconds (int): Timeout in seconds (default: None).
    #'              After this amount of time, Amazon SageMaker terminates the job,
    #'              regardless of its current status. If `max_runtime_in_seconds` is not
    #'              specified, the default value is 24 hours.
    #' @param base_job_name (str): Prefix for processing job name. If not specified,
    #'              the processor generates a default job name, based on the
    #'              processing image name and current timestamp.
    #' @param sagemaker_session (:class:`~sagemaker.session.Session`):
    #'              Session object which manages interactions with Amazon SageMaker and
    #'              any other AWS services needed. If not specified, the processor creates
    #'              one using the default AWS configuration chain.
    #' @param env (dict[str, str]): Environment variables to be passed to
    #'              the processing jobs (default: None).
    #' @param tags (list[dict]): List of tags to be passed to the processing job
    #'              (default: None). For more, see
    #'              \url{https://docs.aws.amazon.com/sagemaker/latest/dg/API_Tag.html}.
    #' @param network_config (:class:`~sagemaker.network.NetworkConfig`):
    #'              A :class:`~sagemaker.network.NetworkConfig`
    #'              object that configures network isolation, encryption of
    #'              inter-container traffic, security group IDs, and subnets.
    initialize = function(role,
                          data_wrangler_flow_source,
                          instance_count,
                          instance_type,
                          volume_size_in_gb=30L,
                          volume_kms_key=NULL,
                          output_kms_key=NULL,
                          max_runtime_in_seconds=NULL,
                          base_job_name=NULL,
                          sagemaker_session=NULL,
                          env=NULL,
                          tags=NULL,
                          network_config=NULL){
      stopifnot(is.character(role),
                is.character(data_wrangler_flow_source),
                is.integer(instance_count) || is.numeric(instance_count),
                is.character(instance_type),
                is.integer(volume_size_in_gb) || is.numeric(volume_size_in_gb),
                is.character(volume_kms_key) || is.null(volume_kms_key),
                is.character(output_kms_key) || is.null(output_kms_key),
                is.character(max_runtime_in_seconds) || is.numeric(max_runtime_in_seconds) || is.null(max_runtime_in_seconds),
                inherits(sagemaker_session, "Session") || is.null(sagemaker_session),
                is.list(env) || is.null(env),
                is.list(tags) || is.null(tags),
                inherits(network_config, "NetworkConfig") || is.null(network_config)
      )

      self$data_wrangler_flow_source = data_wrangler_flow_source
      self$sagemaker_session = sagemaker_session %||% sagemaker.core::Session$new()
      image_uri = sagemaker.common::ImageUris$new()$retrieve(
        "data-wrangler", region=self$sagemaker_session$paws_region_name
      )
      super$intialize(
        role,
        image_uri,
        instance_count,
        instance_type,
        volume_size_in_gb=volume_size_in_gb,
        volume_kms_key=volume_kms_key,
        output_kms_key=output_kms_key,
        max_runtime_in_seconds=max_runtime_in_seconds,
        base_job_name=base_job_name,
        sagemaker_session=sagemaker_session,
        env=env,
        tags=tags,
        network_config=network_config
      )
    }
  ),
  private = list(

    # Normalizes the arguments so that they can be passed to the job run
    # Args:
    #   job_name (str): Name of the processing job to be created. If not specified, one
    # is generated, using the base name given to the constructor, if applicable
    # (default: None).
    # arguments (list[str]): A list of string arguments to be passed to a
    # processing job (default: None).
    # inputs (list[:class:`~sagemaker.processing.ProcessingInput`]): Input files for
    # the processing job. These must be provided as
    # :class:`~sagemaker.processing.ProcessingInput` objects (default: None).
    # outputs (list[:class:`~sagemaker.processing.ProcessingOutput`]): Outputs for
    # the processing job. These can be specified as either path strings or
    # :class:`~sagemaker.processing.ProcessingOutput` objects (default: None).
    # code (str): This can be an S3 URI or a local path to a file with the framework
    # script to run (default: None). A no op in the base class.
    # kms_key (str): The ARN of the KMS key that is used to encrypt the
    # user code file (default: None).
    .normalize_args = function(job_name=NULL,
                               arguments=NULL,
                               inputs=NULL,
                               outputs=NULL,
                               code=NULL,
                               kms_key=NULL){
      inputs = inputs %||% list()
      found = any(sapply(inputs, function(element) element$input_name == "flow"))
      if (!found)
        inputs = c(inputs, private$.get_recipe_input())
      return (super$.normalize_args(job_name, arguments, inputs, outputs, code, kms_key))
    },

    # Creates a ProcessingInput with Data Wrangler recipe uri and appends it to inputs
    .get_recipe_input = function(){
      return(sagemaker.common::ProcessingInput$new(
        source=self$data_wrangler_flow_source,
        destination="/opt/ml/processing/flow",
        input_name="flow",
        s3_data_type="S3Prefix",
        s3_input_mode="File",
        s3_data_distribution_type="FullyReplicated")
      )
    }
  ),
  lock_objects=F
)
