# NOTE: This code has been modified from AWS Sagemaker Python:
# https://github.com/aws/sagemaker-python-sdk/blob/master/tests/unit/sagemaker/workflow/test_quality_check_step.py

library(sagemaker.core)
library(sagemaker.common)
library(sagemaker.mlcore)
library(sagemaker.mlframework)

.REGION = "us-west-2"
.ROLE = "DummyRole"
.BUCKET = "my-bucket"

paws_session = function(region=.REGION){
  paws_mock = Mock$new(
    name = "PawsSession",
    region_name = region
  )

  s3_client = Mock$new()
  cl_client = Mock$new()
  sm_client = Mock$new()
  smt_client = Mock$new()
  iam_client = Mock$new()
  athena_client = Mock$new()

  s3_client$.call_args("put_object")

  iam_client$.call_args("get_role", list(Role = list(Arn = .ROLE)))

  paws_mock$.call_args("client", side_effect = function(service_name, ...){
    switch(service_name,
     "s3" = s3_client,
     "cloudwatchlogs" = cl_client,
     "sagemaker"= sm_client,
     "sagemakerruntime" = smt_client,
     "iam" = iam_client,
     "athena" = athena_client
    )
  })
  return(paws_mock)
}

sagemaker_session = function(paws_session){
  return(Session$new(
    paws_session=paws_session,
    default_bucket=.BUCKET)
  )
}

.expected_data_quality_dsl = list(
  "Name"="DataQualityCheckStep",
  "Type"="QualityCheck",
  "Arguments"=list(
    "ProcessingResources"=list(
      "ClusterConfig"=list(
        "InstanceType"="ml.m5.xlarge",
        "InstanceCount"=1,
        "VolumeSizeInGB"=60
      )
    ),
    "AppSpecification"=list(
      "ImageUri"="159807026194.dkr.ecr.us-west-2.amazonaws.com/sagemaker-model-monitor-analyzer"
    ),
    "RoleArn"="DummyRole",
    "ProcessingInputs"=list(
      list(
        "InputName"="baseline_dataset_input",
        "AppManaged"=FALSE,
        "S3Input"=list(
          "S3Uri"=list("Get"="Parameters.BaselineDataset"),
          "LocalPath"="/opt/ml/processing/input/baseline_dataset_input",
          "S3DataType"="S3Prefix",
          "S3InputMode"="File",
          "S3DataDistributionType"="FullyReplicated",
          "S3CompressionType"="None"
        )
      ),
      list(
        "InputName"="post_analytics_processor_script_input",
        "AppManaged"=FALSE,
        "S3Input"=list(
          "LocalPath"="/opt/ml/processing/input/post_analytics_processor_script_input",
          "S3DataType"="S3Prefix",
          "S3InputMode"="File",
          "S3DataDistributionType"="FullyReplicated",
          "S3CompressionType"="None"
        )
      ),
      list(
        "InputName"="record_preprocessor_script_input",
        "AppManaged"=FALSE,
        "S3Input"=list(
          "LocalPath"="/opt/ml/processing/input/record_preprocessor_script_input",
          "S3DataType"="S3Prefix",
          "S3InputMode"="File",
          "S3DataDistributionType"="FullyReplicated",
          "S3CompressionType"="None"
        )
      )
    ),
    "ProcessingOutputConfig"=list(
      "Outputs"=list(
        list(
          "OutputName"="quality_check_output",
          "AppManaged"=FALSE,
          "S3Output"=list(
            "S3Uri"="s3://...",
            "LocalPath"="/opt/ml/processing/output",
            "S3UploadMode"="EndOfJob"
          )
        )
      )
    ),
    "Environment"=list(
      "output_path"="/opt/ml/processing/output",
      "publish_cloudwatch_metrics"="Disabled",
      "dataset_format"='{"csv"=list("header"=true, "output_columns_position"="START"}}',
      "record_preprocessor_script"="/opt/ml/processing/input/record_preprocessor_script_input/preprocessor.py",
      "post_analytics_processor_script"=paste0("/opt/ml/processing/input/",
        "post_analytics_processor_script_input/postprocessor.py"),
      "dataset_source"="/opt/ml/processing/input/baseline_dataset_input"
    ),
    "StoppingCondition"=list("MaxRuntimeInSeconds"=1800)
  ),
  "CheckType"="DATA_QUALITY",
  "ModelPackageGroupName"=list("Get"="Parameters.MyModelPackageGroup"),
  "SkipCheck"=FALSE,
  "RegisterNewBaseline"=FALSE,
  "SuppliedBaselineStatistics"=list("Get"="Parameters.SuppliedBaselineStatisticsUri"),
  "SuppliedBaselineConstraints"=list("Get"="Parameters.SuppliedBaselineConstraintsUri"),
  "CacheConfig"=list("Enabled"=TRUE, "ExpireAfter"="PT1H")
)

.expected_model_quality_dsl = list(
  "Name"="ModelQualityCheckStep",
  "Type"="QualityCheck",
  "Arguments"=list(
    "ProcessingResources"=list(
      "ClusterConfig"=list(
        "InstanceType"="ml.m5.xlarge",
        "InstanceCount"=1,
        "VolumeSizeInGB"=60
      )
    ),
    "AppSpecification"=list(
      "ImageUri"="159807026194.dkr.ecr.us-west-2.amazonaws.com/sagemaker-model-monitor-analyzer"
    ),
    "RoleArn"="DummyRole",
    "ProcessingInputs"=list(
      list(
        "InputName"="baseline_dataset_input",
        "AppManaged"=FALSE,
        "S3Input"=list(
          "LocalPath"="/opt/ml/processing/input/baseline_dataset_input",
          "S3DataType"="S3Prefix",
          "S3InputMode"="File",
          "S3DataDistributionType"="FullyReplicated",
          "S3CompressionType"="None"
        )
      ),
      list(
        "InputName"="post_analytics_processor_script_input",
        "AppManaged"=FALSE,
        "S3Input"=list(
          "LocalPath"="/opt/ml/processing/input/post_analytics_processor_script_input",
          "S3DataType"="S3Prefix",
          "S3InputMode"="File",
          "S3DataDistributionType"="FullyReplicated",
          "S3CompressionType"="None"
        )
      )
    ),
    "ProcessingOutputConfig"=list(
      "Outputs"=list(
        list(
          "OutputName"="quality_check_output",
          "AppManaged"=FALSE,
          "S3Output"=list(
            "LocalPath"="/opt/ml/processing/output",
            "S3UploadMode"="EndOfJob"
          )
        )
      )
    ),
    "Environment"=list(
      "output_path"="/opt/ml/processing/output",
      "publish_cloudwatch_metrics"="Disabled",
      "dataset_format"='{"csv"=list("header"=true, "output_columns_position"="START"}}',
      "post_analytics_processor_script"=paste0("/opt/ml/processing/input/post_analytics_processor_script_input/",
        "postprocessor.py"),
      "dataset_source"="/opt/ml/processing/input/baseline_dataset_input",
      "analysis_type"="MODEL_QUALITY",
      "problem_type"="BinaryClassification",
      "probability_attribute"="0",
      "probability_threshold_attribute"="0.5"
    ),
    "StoppingCondition"=list("MaxRuntimeInSeconds"=1800)
  ),
  "CheckType"="MODEL_QUALITY",
  "ModelPackageGroupName"=list("Get"="Parameters.MyModelPackageGroup"),
  "SkipCheck"=FALSE,
  "RegisterNewBaseline"=FALSE,
  "SuppliedBaselineStatistics"=list("Get"="Parameters.SuppliedBaselineStatisticsUri"),
  "SuppliedBaselineConstraints"=list("Get"="Parameters.SuppliedBaselineConstraintsUri")
)

model_package_group_name = function(){
  return(ParameterString$new(name="MyModelPackageGroup", default_value=""))
}

supplied_baseline_statistics_uri = function(){
  return(ParameterString$new(name="SuppliedBaselineStatisticsUri", default_value=""))
}

supplied_baseline_constraints_uri = function(){
  return(ParameterString$new(name="SuppliedBaselineConstraintsUri", default_value=""))
}

check_job_config = function(sms = sagemaker_session(paws_session())){
  return(CheckJobConfig$new(
    role=.ROLE,
    instance_count=1,
    instance_type="ml.m5.xlarge",
    volume_size_in_gb=60,
    max_runtime_in_seconds=1800,
    sagemaker_session=sms
  ))
}

test_that("test_data_quality_check_step", {
  paws_sess = paws_session()
  sms = sagemaker_session(paws_sess)

  data_quality_check_config = DataQualityCheckConfig$new(
    baseline_dataset=ParameterString$new(name="BaselineDataset"),
    dataset_format=DatasetFormat$new()$csv(header=T),
    output_s3_uri="s3://...",
    record_preprocessor_script="s3://my_bucket/data_quality/preprocessor.py",
    post_analytics_processor_script="s3://my_bucket/data_quality/postprocessor.py"
  )
  data_quality_check_step = QualityCheckStep$new(
    name="DataQualityCheckStep",
    skip_check=FALSE,
    register_new_baseline=FALSE,
    quality_check_config=data_quality_check_config,
    check_job_config=check_job_config(sms),
    model_package_group_name=model_package_group_name(),
    supplied_baseline_statistics=supplied_baseline_statistics_uri(),
    supplied_baseline_constraints=supplied_baseline_constraints_uri(),
    cache_config=CacheConfig$new(enable_caching=TRUE, expire_after="PT1H")
  )

})

check_job_config$.generate_model_monitor(
  "DefaultModelMonitor"
)


