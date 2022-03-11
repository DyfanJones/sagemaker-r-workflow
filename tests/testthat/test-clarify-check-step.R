# NOTE: This code has been modified from AWS Sagemaker Python:
# https://github.com/aws/sagemaker-python-sdk/blob/dev/tests/unit/sagemaker/workflow/test_callback_step.py

library(sagemaker.core)
library(sagemaker.common)
library(sagemaker.mlcore)
library(sagemaker.mlframework)


.REGION = "us-west-2"
.ROLE = "DummyRole"
.DEFAULT_BUCKET = "my-bucket"
.S3_INPUT_PATH = "s3://my_bucket/input"
.S3_OUTPUT_PATH = "s3://my_bucket/output"
.S3_ANALYSIS_CONFIG_OUTPUT_PATH = "s3://my_bucket/analysis_cfg_output"

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
    default_bucket=.DEFAULT_BUCKET)
  )
}

.expected_data_bias_dsl = list(
  "Name"="DataBiasCheckStep",
  "Type"="ClarifyCheck",
  "Arguments"=list(
    "ProcessingResources"=list(
      "ClusterConfig"=list(
        "InstanceType"="ml.m5.xlarge",
        "InstanceCount"=1,
        "VolumeSizeInGB"=30
      )
    ),
    "AppSpecification"=list(
      "ImageUri"="306415355426.dkr.ecr.us-west-2.amazonaws.com/sagemaker-clarify-processing:1.0"
    ),
    "RoleArn"="DummyRole",
    "ProcessingInputs"=list(
      list(
        "InputName"="analysis_config",
        "AppManaged"=FALSE,
        "S3Input"=list(
          "S3Uri"=sprintf("%s/analysis_config.json", .S3_ANALYSIS_CONFIG_OUTPUT_PATH),
          "LocalPath"="/opt/ml/processing/input/config",
          "S3DataType"="S3Prefix",
          "S3InputMode"="File",
          "S3DataDistributionType"="FullyReplicated",
          "S3CompressionType"="None"
        )
      ),
      list(
        "InputName"="dataset",
        "AppManaged"=FALSE,
        "S3Input"=list(
          "S3Uri"=.S3_INPUT_PATH,
          "LocalPath"="/opt/ml/processing/input/data",
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
          "OutputName"="analysis_result",
          "AppManaged"=FALSE,
          "S3Output"=list(
            "S3Uri"=.S3_OUTPUT_PATH,
            "LocalPath"="/opt/ml/processing/output",
            "S3UploadMode"="EndOfJob"
          )
        )
      ),
      "KmsKeyId"="output_kms_key"
    )
  ),
  "CheckType"="DATA_BIAS",
  "ModelPackageGroupName"=list("Get"="Parameters.MyModelPackageGroup"),
  "SkipCheck"=FALSE,
  "RegisterNewBaseline"=FALSE,
  "SuppliedBaselineConstraints"="supplied_baseline_constraints",
  "CacheConfig"=list("Enabled"=TRUE, "ExpireAfter"="PT1H")
)

.expected_model_bias_dsl = list(
  "Name"="ModelBiasCheckStep",
  "Type"="ClarifyCheck",
  "Arguments"=list(
    "ProcessingResources"=list(
      "ClusterConfig"=list(
        "InstanceType"="ml.m5.xlarge",
        "InstanceCount"=1,
        "VolumeSizeInGB"=30
      )
    ),
    "AppSpecification"=list(
      "ImageUri"="306415355426.dkr.ecr.us-west-2.amazonaws.com/sagemaker-clarify-processing:1.0"
    ),
    "RoleArn"="DummyRole",
    "ProcessingInputs"=list(
      list(
        "InputName"="analysis_config",
        "AppManaged"=FALSE,
        "S3Input"=list(
          "S3Uri"=sprintf("%s/analysis_config.json", .S3_OUTPUT_PATH),
          "LocalPath"="/opt/ml/processing/input/config",
          "S3DataType"="S3Prefix",
          "S3InputMode"="File",
          "S3DataDistributionType"="FullyReplicated",
          "S3CompressionType"="None"
        )
      ),
      list(
        "InputName"="dataset",
        "AppManaged"=FALSE,
        "S3Input"=list(
          "S3Uri"=.S3_INPUT_PATH,
          "LocalPath"="/opt/ml/processing/input/data",
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
          "OutputName"="analysis_result",
          "AppManaged"=FALSE,
          "S3Output"=list(
            "S3Uri"=.S3_OUTPUT_PATH,
            "LocalPath"="/opt/ml/processing/output",
            "S3UploadMode"="EndOfJob"
          )
        )
      ),
      "KmsKeyId"="output_kms_key"
    )
  ),
  "CheckType"="MODEL_BIAS",
  "ModelPackageGroupName"=list("Get"="Parameters.MyModelPackageGroup"),
  "SkipCheck"=FALSE,
  "RegisterNewBaseline"=FALSE,
  "SuppliedBaselineConstraints"="supplied_baseline_constraints",
  "ModelName"="model_name"
)

.expected_model_explainability_dsl = list(
  "Name"="ModelExplainabilityCheckStep",
  "Type"="ClarifyCheck",
  "Arguments"=list(
    "ProcessingResources"=list(
      "ClusterConfig"=list(
        "InstanceType"="ml.m5.xlarge",
        "InstanceCount"=1,
        "VolumeSizeInGB"=30
      )
    ),
    "AppSpecification"=list(
      "ImageUri"="306415355426.dkr.ecr.us-west-2.amazonaws.com/sagemaker-clarify-processing:1.0"
    ),
    "RoleArn"="DummyRole",
    "ProcessingInputs"=list(
      list(
        "InputName"="analysis_config",
        "AppManaged"=FALSE,
        "S3Input"=list(
          "S3Uri"=sprintf("%s/analysis_config.json", .S3_OUTPUT_PATH),
          "LocalPath"="/opt/ml/processing/input/config",
          "S3DataType"="S3Prefix",
          "S3InputMode"="File",
          "S3DataDistributionType"="FullyReplicated",
          "S3CompressionType"="None"
        )
      ),
      list(
        "InputName"="dataset",
        "AppManaged"=FALSE,
        "S3Input"=list(
          "S3Uri"=.S3_INPUT_PATH,
          "LocalPath"="/opt/ml/processing/input/data",
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
          "OutputName"="analysis_result",
          "AppManaged"=FALSE,
          "S3Output"=list(
            "S3Uri"=.S3_OUTPUT_PATH,
            "LocalPath"="/opt/ml/processing/output",
            "S3UploadMode"="EndOfJob"
          )
        )
      ),
      "KmsKeyId"="output_kms_key"
    )
  ),
  "CheckType"="MODEL_EXPLAINABILITY",
  "ModelPackageGroupName"=list("Get"="Parameters.MyModelPackageGroup"),
  "SkipCheck"=FALSE,
  "RegisterNewBaseline"=FALSE,
  "SuppliedBaselineConstraints"="supplied_baseline_constraints",
  "ModelName"="model_name"
)

model_package_group_name=function(){
  return(ParameterString$new(name="MyModelPackageGroup", default_value=""))
}

check_job_config = function(sagemaker_session){
  return (CheckJobConfig$new(
    role=.ROLE,
    instance_type="ml.m5.xlarge",
    instance_count=1,
    sagemaker_session=sagemaker_session,
    output_kms_key="output_kms_key")
  )
}

data_config = function(){
  return (DataConfig$new(
    s3_data_input_path=.S3_INPUT_PATH,
    s3_output_path=.S3_OUTPUT_PATH,
    label="fraud",
    dataset_type="text/csv")
  )
}

bias_config = function(){
  return (BiasConfig$new(
    label_values_or_threshold=0,
    facet_name="customer_gender_female",
    facet_values_or_threshold=1)
  )
}

model_config = function(){
  return (ModelConfig$new(
    model_name="model_name",
    instance_type="ml.m5.xlarge",
    instance_count=1,
    accept_type="text/csv",
    content_type="text/csv")
  )
}

predictions_config = function(){
  return (ModelPredictedLabelConfig$new(probability_threshold=0.8))
}

shap_config = function(){
  return (SHAPConfig$new(
    baseline=list(),
    num_samples=15,
    agg_method="mean_abs",
    save_local_shap_values=TRUE)
  )
}

test_that("test_data_bias_check_step", {
  data_bias_data_config = DataConfig$new(
    s3_data_input_path=.S3_INPUT_PATH,
    s3_output_path=.S3_OUTPUT_PATH,
    s3_analysis_config_output_path=.S3_ANALYSIS_CONFIG_OUTPUT_PATH,
    label="fraud",
    dataset_type="text/csv"
  )

  data_bias_check_config = DataBiasCheckConfig$new(
    data_config=data_bias_data_config,
    data_bias_config=bias_config(),
    methods="all",
    kms_key="kms_key"
  )

  sms = sagemaker_session(paws_session())
  data_bias_check_step = ClarifyCheckStep$new(
    name="DataBiasCheckStep",
    clarify_check_config=data_bias_check_config,
    check_job_config=check_job_config(sms),
    skip_check=FALSE,
    register_new_baseline=FALSE,
    model_package_group_name=model_package_group_name(),
    supplied_baseline_constraints="supplied_baseline_constraints",
    cache_config=CacheConfig$new(enable_caching=TRUE, expire_after="PT1H")
  )

  pipeline = Pipeline$new(
    name="MyPipeline",
    parameters=list(model_package_group_name()),
    steps=list(data_bias_check_step),
    sagemaker_session=sms
  )

  actual = jsonlite::parse_json(pipeline$definition())$Steps[[1]]
  expect_equal(actual[sort(names(actual))], .expected_data_bias_dsl[sort(names(.expected_data_bias_dsl))])
  expect_true(grepl(sprintf(
        "%s/%s-configuration/%s-config.*/.*/analysis_config.json",
        .S3_ANALYSIS_CONFIG_OUTPUT_PATH, .BIAS_MONITORING_CFG_BASE_NAME,
        .BIAS_MONITORING_CFG_BASE_NAME
      ),
      data_bias_check_config$monitoring_analysis_config_uri
    )
  )
})

test_that("test_model_bias_check_step", {
  sms = sagemaker_session(paws_session())
  model_bias_check_config = ModelBiasCheckConfig$new(
    data_config=data_config(),
    data_bias_config=bias_config(),
    model_config=model_config(),
    model_predicted_label_config=predictions_config(),
    methods="all"
  )
  model_bias_check_step = ClarifyCheckStep$new(
    name="ModelBiasCheckStep",
    clarify_check_config=model_bias_check_config,
    check_job_config=check_job_config(sms),
    skip_check=FALSE,
    register_new_baseline=FALSE,
    model_package_group_name=model_package_group_name(),
    supplied_baseline_constraints="supplied_baseline_constraints",
  )
  pipeline = Pipeline$new(
    name="MyPipeline",
    parameters=list(model_package_group_name),
    steps=list(model_bias_check_step),
    sagemaker_session=sms
  )

  actual = jsonlite::parse_json(pipeline$definition())$Steps[[1]]
  expect_equal(actual[sort(names(actual))], .expected_model_bias_dsl[sort(names(.expected_model_bias_dsl))])
  expect_true(grepl(sprintf(
        "s3://%s/%s/%s-configuration/%s-config.*/.*/analysis_config.json",
        .DEFAULT_BUCKET, .MODEL_MONITOR_S3_PATH,
        .BIAS_MONITORING_CFG_BASE_NAME, .BIAS_MONITORING_CFG_BASE_NAME
      ),
      model_bias_check_config$monitoring_analysis_config_uri
    )
  )
})

test_that("test_model_explainability_check_step", {
  sms = sagemaker_session(paws_session())
  model_explainability_check_config = ModelExplainabilityCheckConfig$new(
    data_config=data_config(),
    model_config=model_config(),
    explainability_config=shap_config()
  )
  model_explainability_check_step = ClarifyCheckStep$new(
    name="ModelExplainabilityCheckStep",
    clarify_check_config=model_explainability_check_config,
    check_job_config=check_job_config(sms),
    skip_check=FALSE,
    register_new_baseline=FALSE,
    model_package_group_name=model_package_group_name(),
    supplied_baseline_constraints="supplied_baseline_constraints"
  )
  pipeline = Pipeline$new(
    name="MyPipeline",
    parameters=list(model_package_group_name),
    steps=list(model_explainability_check_step),
    sagemaker_session=sms
  )

  actual = jsonlite::parse_json(pipeline$definition())$Steps[[1]]
  expect_equal(
    actual[sort(names(actual))],
    .expected_model_explainability_dsl[sort(names(.expected_model_explainability_dsl))]
  )
  expect_true(grepl(sprintf(
        "s3://%s/%s/%s-configuration/%s-config.*/.*/analysis_config.json",
        .DEFAULT_BUCKET, .MODEL_MONITOR_S3_PATH,
        .EXPLAINABILITY_MONITORING_CFG_BASE_NAME, .EXPLAINABILITY_MONITORING_CFG_BASE_NAME
      ),
      model_explainability_check_config$monitoring_analysis_config_uri
    )
  )
})

test_that("test_clarify_check_step_properties", {
  sms = sagemaker_session(paws_session())
  model_explainability_check_config = ModelExplainabilityCheckConfig$new(
    data_config=data_config(),
    model_config=model_config(),
    explainability_config=shap_config()
  )
  model_explainability_check_step = ClarifyCheckStep$new(
    name="ModelExplainabilityCheckStep",
    clarify_check_config=model_explainability_check_config,
    check_job_config=check_job_config(sms),
    skip_check=FALSE,
    register_new_baseline=FALSE,
    model_package_group_name=model_package_group_name(),
    supplied_baseline_constraints="supplied_baseline_constraints"
  )

  expect_equal(model_explainability_check_step$properties$CalculatedBaselineConstraints$expr, list(
    "Get"="Steps.ModelExplainabilityCheckStep.CalculatedBaselineConstraints")
  )
  expect_equal(model_explainability_check_step$properties$BaselineUsedForDriftCheckConstraints$expr, list(
    "Get"="Steps.ModelExplainabilityCheckStep.BaselineUsedForDriftCheckConstraints")
  )
})

test_that("test_clarify_check_step_invalid_config", {
  sms = sagemaker_session(paws_session())
  clarify_check_config = ClarifyCheckConfig$new(data_config=data_config())

  expect_error(
    ClarifyCheckStep$new(
      name="ClarifyCheckStep",
      clarify_check_config=clarify_check_config,
      check_job_config=check_job_config(sms),
      skip_check=FALSE,
      register_new_baseline=FALSE,
      model_package_group_name=model_package_group_name(),
      supplied_baseline_constraints="supplied_baseline_constraints"
    ),
    class = "RuntimeError",
    paste(
      "The clarify_check_config can only be object of DataBiasCheckConfig, ModelBiasCheckConfig",
      "or ModelExplainabilityCheckConfig"
    )
  )
})

test_that("test_clarify_check_step_with_none_or_invalid_s3_analysis_config_output_uri", {
  sms = sagemaker_session(paws_session())
  # s3_analysis_config_output is None and s3_output_path is valid s3 path str
  data_config_cls = DataConfig$new(
    s3_data_input_path=.S3_INPUT_PATH,
    s3_output_path=.S3_OUTPUT_PATH,
    label="fraud",
    dataset_type="text/csv"
  )
  clarify_check_config = DataBiasCheckConfig$new(
    data_config=data_config_cls,
    data_bias_config=bias_config()
  )

  ClarifyCheckStep$new(
    name="ClarifyCheckStep",
    clarify_check_config=clarify_check_config,
    check_job_config=check_job_config(sms),
    skip_check=FALSE,
    register_new_baseline=FALSE,
    model_package_group_name=model_package_group_name(),
    supplied_baseline_constraints="supplied_baseline_constraints"
  )

  # s3_analysis_config_output is empty but s3_output_path is Parameter
  data_config_cls = DataConfig$new(
    s3_data_input_path=.S3_INPUT_PATH,
    s3_output_path=ParameterString$new(name="S3OutputPath", default_value=.S3_OUTPUT_PATH),
    s3_analysis_config_output_path="",
    label="fraud",
    dataset_type="text/csv"
  )
  clarify_check_config = DataBiasCheckConfig$new(
    data_config=data_config_cls,
    data_bias_config=bias_config()
  )

  expect_error(
    ClarifyCheckStep$new(
      name="ClarifyCheckStep",
      clarify_check_config=clarify_check_config,
      check_job_config=check_job_config(sms),
      skip_check=FALSE,
      register_new_baseline=FALSE,
      model_package_group_name=model_package_group_name(),
      supplied_baseline_constraints="supplied_baseline_constraints"
    ),
    class = "RuntimeError",
    paste(
      "`s3_output_path` cannot be of type ExecutionVariable/Expression/Parameter/Properties",
      "if `s3_analysis_config_output_path` is NULL or empty "
    )
  )

  # s3_analysis_config_output is invalid
  data_config_cls = DataConfig$new(
    s3_data_input_path=.S3_INPUT_PATH,
    s3_output_path=ParameterString$new(name="S3OutputPath", default_value=.S3_OUTPUT_PATH),
    s3_analysis_config_output_path=ParameterString$new(name="S3OAnalysisCfgOutput"),
    label="fraud",
    dataset_type="text/csv"
  )
  clarify_check_config = DataBiasCheckConfig$new(
    data_config=data_config_cls,
    data_bias_config=bias_config()
  )

  expect_error(
    ClarifyCheckStep$new(
      name="ClarifyCheckStep",
      clarify_check_config=clarify_check_config,
      check_job_config=check_job_config,
      skip_check=FALSE,
      register_new_baseline=FALSE,
      model_package_group_name=model_package_group_name(),
      supplied_baseline_constraints="supplied_baseline_constraints"
    ),
    class = "RuntimeError",
    paste(
      "s3_analysis_config_output_path cannot be of type",
      "ExecutionVariable/Expression/Parameter/Properties"
    )
  )
})

test_that("test_get_s3_base_uri_for_monitoring_analysis_config", {
  sms = sagemaker_session(paws_session())
  # ModelExplainabilityCheckStep without specifying s3_analysis_config_output_path
  model_explainability_check_config_1 = ModelExplainabilityCheckConfig$new(
    data_config=data_config(),
    model_config=model_config(),
    explainability_config=shap_config()
  )
  model_explainability_check_step_1 = ClarifyCheckStep$new(
    name="ModelExplainabilityCheckStep",
    clarify_check_config=model_explainability_check_config_1,
    check_job_config=check_job_config(sms)
  )

  expect_equal(
    sprintf(
      "s3://%s/%s/%s-configuration",
      .DEFAULT_BUCKET, .MODEL_MONITOR_S3_PATH,
      .EXPLAINABILITY_MONITORING_CFG_BASE_NAME
    ),
    model_explainability_check_step_1$.__enclos_env__$private$.get_s3_base_uri_for_monitoring_analysis_config()
  )

  # ModelExplainabilityCheckStep with specifying s3_analysis_config_output_path
  model_explainability_data_config = DataConfig$new(
    s3_data_input_path=.S3_INPUT_PATH,
    s3_output_path=ParameterString$new(name="S3OutputPath", default_value=.S3_OUTPUT_PATH),
    s3_analysis_config_output_path=.S3_ANALYSIS_CONFIG_OUTPUT_PATH
  )
  model_explainability_check_config_2 = ModelExplainabilityCheckConfig$new(
    data_config=model_explainability_data_config,
    model_config=model_config(),
    explainability_config=shap_config()
  )
  model_explainability_check_step_2 = ClarifyCheckStep$new(
    name="ModelExplainabilityCheckStep",
    clarify_check_config=model_explainability_check_config_2,
    check_job_config=check_job_config(sms)
  )

  expect_equal(
    sprintf(
      "%s/%s-configuration",
      .S3_ANALYSIS_CONFIG_OUTPUT_PATH, .EXPLAINABILITY_MONITORING_CFG_BASE_NAME
    ),
    model_explainability_check_step_2$.__enclos_env__$private$.get_s3_base_uri_for_monitoring_analysis_config()
  )

  # ModelBiasCheckStep with specifying s3_analysis_config_output_path
  model_bias_data_config = DataConfig$new(
    s3_data_input_path=.S3_INPUT_PATH,
    s3_output_path=.S3_OUTPUT_PATH,
    s3_analysis_config_output_path=.S3_ANALYSIS_CONFIG_OUTPUT_PATH
  )
  model_bias_check_config = ModelBiasCheckConfig$new(
    data_config=model_bias_data_config,
    data_bias_config=bias_config(),
    model_config=model_config(),
    model_predicted_label_config=predictions_config()
  )
  model_bias_check_step = ClarifyCheckStep$new(
    name="ModelBiasCheckStep",
    clarify_check_config=model_bias_check_config,
    check_job_config=check_job_config(sms)
  )

  expect_equal(
    sprintf(
      "%s/%s-configuration",
      .S3_ANALYSIS_CONFIG_OUTPUT_PATH, .BIAS_MONITORING_CFG_BASE_NAME
    ),
    model_bias_check_step$.__enclos_env__$private$.get_s3_base_uri_for_monitoring_analysis_config()
  )

  # DataBiasCheckStep without specifying s3_analysis_config_output_path
  data_bias_check_config = DataBiasCheckConfig$new(
    data_config=data_config(),
    data_bias_config=bias_config()
  )
  data_bias_check_step = ClarifyCheckStep$new(
    name="DataBiasCheckStep",
    clarify_check_config=data_bias_check_config,
    check_job_config=check_job_config(sms)
  )
  expect_equal(
    sprintf("s3://%s/%s/%s-configuration",
    .DEFAULT_BUCKET, .MODEL_MONITOR_S3_PATH,
    .BIAS_MONITORING_CFG_BASE_NAME
    ),
    data_bias_check_step$.__enclos_env__$private$.get_s3_base_uri_for_monitoring_analysis_config()
  )
})
