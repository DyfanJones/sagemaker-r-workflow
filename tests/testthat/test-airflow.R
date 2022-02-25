# NOTE: This code has been modified from AWS Sagemaker Python:
# https://github.com/aws/sagemaker-python-sdk/blob/dev/tests/unit/test_airflow.py

library(sagemaker.core)
library(sagemaker.common)
library(sagemaker.mlcore)
library(sagemaker.mlframework)

REGION = "us-west-2"
BUCKET_NAME = "output"
TIME_STAMP = "1111"


sagemaker_session = function(region=REGION){
  paws_mock = Mock$new(
    name = "PawsSession",
    region_name = region
  )

  sms = Mock$new(
    name="Session",
    paws_session=paws_mock,
    paws_region_name=region,
    config=NULL,
    local_mode=FALSE,
    s3=NULL
  )

  sagemaker = Mock$new()
  s3_client = Mock$new()

  sms$.call_args("default_bucket", return_value=BUCKET_NAME)
  sms$.__enclos_env__$private$.default_bucket = BUCKET_NAME

  sms$sagemaker = sagemaker
  sms$s3 = s3_client
  return(sms)
}

test_that("test_byo_training_config_required_args", {
  sms = sagemaker_session()
  byo = Estimator$new(
    image_uri="byo",
    role="{{ role }}",
    instance_count="{{ instance_count }}",
    instance_type="ml.c4.2xlarge",
    sagemaker_session=sms
  )

  byo$set_hyperparameters(epochs=32, feature_dim=1024, mini_batch_size=256)

  data = list("train"="{{ training_data }}")

  with_mock(
    `sagemaker.core::sagemaker_timestamp` = mock_fun(TIME_STAMP),{
      config = training_config(byo, data)
  })
  expected_config = list(
    "AlgorithmSpecification"=list("TrainingImage"="byo", "TrainingInputMode"="File"),
    "OutputDataConfig"=list("S3OutputPath"="s3://output/"),
    "TrainingJobName"=sprintf("byo-%s", TIME_STAMP),
    "StoppingCondition"=list("MaxRuntimeInSeconds"=86400),
    "ResourceConfig"=list(
      "InstanceCount"="{{ instance_count }}",
      "InstanceType"="ml.c4.2xlarge",
      "VolumeSizeInGB"=30
    ),
    "RoleArn"="{{ role }}",
    "InputDataConfig"=list(
      list(
        "DataSource"=list(
          "S3DataSource"=list(
            "S3DataType"="S3Prefix",
            "S3Uri"="{{ training_data }}",
            "S3DataDistributionType"="FullyReplicated"
          )
        ),
        "ChannelName"="train"
      )
    ),
    "HyperParameters"=list("epochs"="32", "feature_dim"="1024", "mini_batch_size"="256")
  )
  expect_equal(config[sort(names(config))], expected_config[sort(names(expected_config))])
})

test_that("test_byo_training_config_all_args", {
  sms = sagemaker_session()
  byo = Estimator$new(
    image_uri="byo",
    role="{{ role }}",
    instance_count="{{ instance_count }}",
    instance_type="ml.c4.2xlarge",
    volume_size="{{ volume_size }}",
    volume_kms_key="{{ volume_kms_key }}",
    max_run="{{ max_run }}",
    input_mode="Pipe",
    output_path="{{ output_path }}",
    output_kms_key="{{ output_volume_kms_key }}",
    base_job_name="{{ base_job_name }}",
    tags=list(list("{{ key }}"="{{ value }}")),
    subnets=list("{{ subnet }}"),
    security_group_ids=list("{{ security_group_ids }}"),
    model_uri="{{ model_uri }}",
    model_channel_name="{{ model_chanel }}",
    sagemaker_session=sms,
    use_spot_instances=TRUE
  )

  byo$set_hyperparameters(epochs=32, feature_dim=1024, mini_batch_size=256)

  data = list("train"="{{ training_data }}")
  with_mock(
    `sagemaker.core::sagemaker_timestamp` = mock_fun(TIME_STAMP),{
      config = training_config(byo, data)
  })
  expected_config = list(
    "AlgorithmSpecification"=list("TrainingImage"="byo", "TrainingInputMode"="Pipe"),
    "OutputDataConfig"=list(
      "S3OutputPath"="{{ output_path }}",
      "KmsKeyId"="{{ output_volume_kms_key }}"
    ),
    "TrainingJobName"=sprintf("{{ base_job_name }}-%s", TIME_STAMP),
    "StoppingCondition"=list("MaxRuntimeInSeconds"="{{ max_run }}"),
    "ResourceConfig"=list(
      "InstanceCount"="{{ instance_count }}",
      "InstanceType"="ml.c4.2xlarge",
      "VolumeSizeInGB"="{{ volume_size }}",
      "VolumeKmsKeyId"="{{ volume_kms_key }}"
    ),
    "RoleArn"="{{ role }}",
    "InputDataConfig"=list(
      list(
        "DataSource"=list(
          "S3DataSource"=list(
            "S3DataType"="S3Prefix",
            "S3Uri"="{{ training_data }}",
            "S3DataDistributionType"="FullyReplicated"
          )
        ),
        "ChannelName"="train"
      ),
      list(
        "DataSource"=list(
          "S3DataSource"=list(
            "S3DataType"="S3Prefix",
            "S3Uri"="{{ model_uri }}",
            "S3DataDistributionType"="FullyReplicated"
          )
        ),
        "ContentType"="application/x-sagemaker-model",
        "InputMode"="File",
        "ChannelName"="{{ model_chanel }}"
      )
    ),
    "VpcConfig"=list(
      "Subnets"=list("{{ subnet }}"),
      "SecurityGroupIds"=list("{{ security_group_ids }}")
    ),
    "EnableManagedSpotTraining"=TRUE,
    "HyperParameters"=list("epochs"="32", "feature_dim"="1024", "mini_batch_size"="256"),
    "Tags"=list(list("{{ key }}"="{{ value }}"))
  )
  expect_equal(config[sort(names(config))], expected_config[sort(names(expected_config))])
})

test_that("test_framework_training_config_required_args", {
  sms = sagemaker_session()
  tf = TensorFlow$new(
    entry_point="/some/script.py",
    framework_version="1.15.2",
    py_version="py3",
    role="{{ role }}",
    instance_count="{{ instance_count }}",
    instance_type="ml.c4.2xlarge",
    sagemaker_session=sms
  )

  data = "{{ training_data }}"
  with_mock(
    `fs::is_file` = mock_fun(TRUE),
    `sagemaker.core::tar_and_upload_dir` = mock_fun(),
    `sagemaker.core::sagemaker_timestamp` = mock_fun(TIME_STAMP),
    `sagemaker.core::parse_s3_url` = mock_fun(list(bucket = "{{ output_path }}", key = "{{ output_path }}")), {
     config = training_config(tf, data)
  })
  expected_config = list(
    "AlgorithmSpecification"=list(
      "TrainingImage"="763104351884.dkr.ecr.us-west-2.amazonaws.com/tensorflow-training:1.15.2-cpu-py3",
      "TrainingInputMode"="File"
    ),
    "OutputDataConfig"=list("S3OutputPath"="s3://output/"),
    "TrainingJobName"=sprintf("tensorflow-training-%s", TIME_STAMP),
    "StoppingCondition"=list("MaxRuntimeInSeconds"=86400),
    "ResourceConfig"=list(
      "InstanceCount"="{{ instance_count }}",
      "InstanceType"="ml.c4.2xlarge",
      "VolumeSizeInGB"=30
    ),
    "RoleArn"="{{ role }}",
    "InputDataConfig"=list(
      list(
        "DataSource"=list(
          "S3DataSource"=list(
            "S3DataType"="S3Prefix",
            "S3Uri"="{{ training_data }}",
            "S3DataDistributionType"="FullyReplicated"
          )
        ),
        "ChannelName"="training"
      )
    ),
    "HyperParameters"=list(
      "sagemaker_container_log_level"="20",
      "sagemaker_job_name"=sprintf('tensorflow-training-%s', TIME_STAMP),
      "sagemaker_region"='us-west-2',
      "sagemaker_submit_directory"=sprintf(
        's3://output/tensorflow-training-%s/source/sourcedir.tar.gz', TIME_STAMP),
      "sagemaker_program"='script.py',
      "model_dir"=sprintf('s3://output/tensorflow-training-%s/model', TIME_STAMP)
    ),
    "S3Operations"=list(
      "S3Upload"=list(
        list(
          "Path"="/some/script.py",
          "Bucket"="output",
          "Key"=sprintf("tensorflow-training-%s/source/sourcedir.tar.gz",TIME_STAMP),
          "Tar"=TRUE
        )
      )
    )
  )
  expect_equal(config[sort(names(config))], expected_config[sort(names(expected_config))])
})

test_that("test_framework_training_config_all_args", {
  sms = sagemaker_session()
  tf = TensorFlow$new(
    entry_point="{{ entry_point }}",
    source_dir="{{ source_dir }}",
    container_log_level="{{ log_level }}",
    code_location="s3://{{ bucket_name }}/{{ prefix }}",
    hyperparameters=list("epochs"=1),
    py_version="py3",
    framework_version="1.15.2",
    role="{{ role }}",
    instance_count=1,
    instance_type="ml.c4.2xlarge",
    volume_size="{{ volume_size }}",
    volume_kms_key="{{ volume_kms_key }}",
    max_run="{{ max_run }}",
    input_mode="Pipe",
    output_path="{{ output_path }}",
    output_kms_key="{{ output_volume_kms_key }}",
    base_job_name="{{ base_job_name }}",
    tags=list(list("{{ key }}"="{{ value }}")),
    subnets=list("{{ subnet }}"),
    security_group_ids=list("{{ security_group_ids }}"),
    metric_definitions=list(list("Name"="{{ name }}", "Regex"="{{ regex }}")),
    sagemaker_session=sms,
    checkpoint_local_path="{{ checkpoint_local_path }}",
    checkpoint_s3_uri="{{ checkpoint_s3_uri }}"
  )
  data = "{{ training_data }}"
  with_mock(
    `fs::is_file` = mock_fun(TRUE),
    `sagemaker.core::tar_and_upload_dir` = mock_fun(),
    `sagemaker.core::sagemaker_timestamp` = mock_fun(TIME_STAMP),
    `sagemaker.core::parse_s3_url` = mock_fun(list(bucket = "{{ output_path }}", key = "{{ output_path }}")), {
      config = training_config(tf, data)
  })
  expected_config = list(
    "AlgorithmSpecification"=list(
      "TrainingImage"="763104351884.dkr.ecr.us-west-2.amazonaws.com/tensorflow-training:1.15.2-cpu-py3",
      "TrainingInputMode"="Pipe",
      "MetricDefinitions"=list(list("Name"="{{ name }}", "Regex"="{{ regex }}"))
    ),
    "OutputDataConfig"=list(
      "S3OutputPath"="{{ output_path }}",
      "KmsKeyId"="{{ output_volume_kms_key }}"
    ),
    "TrainingJobName"=sprintf("{{ base_job_name }}-%s", TIME_STAMP),
    "StoppingCondition"=list("MaxRuntimeInSeconds"="{{ max_run }}"),
    "ResourceConfig"=list(
      "InstanceCount"=1,
      "InstanceType"="ml.c4.2xlarge",
      "VolumeSizeInGB"="{{ volume_size }}",
      "VolumeKmsKeyId"="{{ volume_kms_key }}"
    ),
    "RoleArn"="{{ role }}",
    "InputDataConfig"=list(
      list(
        "DataSource"=list(
          "S3DataSource"=list(
            "S3DataType"="S3Prefix",
            "S3Uri"="{{ training_data }}",
            "S3DataDistributionType"="FullyReplicated"
          )
        ),
        "ChannelName"="training"
      )
    ),
    "VpcConfig"=list(
      "Subnets"=list("{{ subnet }}"),
      "SecurityGroupIds"=list("{{ security_group_ids }}")
    ),
    "HyperParameters"=list(
      "epochs"="1",
      "sagemaker_container_log_level"='{{ log_level }}',
      "sagemaker_job_name"=sprintf('{{ base_job_name }}-%s', TIME_STAMP),
      "sagemaker_region"='us-west-2',
      "sagemaker_submit_directory"=sprintf(
        's3://{{ bucket_name }}/{{ prefix }}/{{ base_job_name }}-%s/source/sourcedir.tar.gz', TIME_STAMP),
      "sagemaker_program"='{{ entry_point }}',
      "model_dir"= sprintf('{{ output_path }}/{{ base_job_name }}-%s/model', TIME_STAMP)
    ),
    "Tags"=list(list("{{ key }}"="{{ value }}")),
    "S3Operations"=list(
      "S3Upload"=list(
        list(
          "Path"="{{ source_dir }}",
          "Bucket"="{{ bucket_name }}",
          "Key"=sprintf("{{ prefix }}/{{ base_job_name }}-%s/source/sourcedir.tar.gz", TIME_STAMP),
          "Tar"=TRUE
        )
      )
    ),
    "CheckpointConfig"=list(
      "LocalPath"="{{ checkpoint_local_path }}",
      "S3Uri"="{{ checkpoint_s3_uri }}"
    )
  )
  expect_equal(config[sort(names(config))], expected_config[sort(names(expected_config))])
})

test_that("test_amazon_alg_training_config_required_args", {
  sms = sagemaker_session()
  ntm_estimator = NTM$new(
    role="{{ role }}",
    num_topics=10,
    instance_count="{{ instance_count }}",
    instance_type="ml.c4.2xlarge",
    sagemaker_session=sms
  )

  ntm_estimator$epochs = 32

  record = RecordSet$new("{{ record }}", 10000, 100, "S3Prefix")

  with_mock(
    `sagemaker.core::sagemaker_timestamp` = mock_fun(TIME_STAMP), {
    config = training_config(ntm_estimator, record, mini_batch_size=256)
  })

  expected_config = list(
    "AlgorithmSpecification"=list(
      "TrainingImage"="174872318107.dkr.ecr.us-west-2.amazonaws.com/ntm:1",
      "TrainingInputMode"="File"
    ),
    "OutputDataConfig"=list("S3OutputPath"="s3://output/"),
    "TrainingJobName"=sprintf("ntm-%s", TIME_STAMP),
    "StoppingCondition"=list("MaxRuntimeInSeconds"=86400),
    "ResourceConfig"=list(
      "InstanceCount"="{{ instance_count }}",
      "InstanceType"="ml.c4.2xlarge",
      "VolumeSizeInGB"=30
    ),
    "RoleArn"="{{ role }}",
    "InputDataConfig"=list(
      list(
        "DataSource"=list(
          "S3DataSource"=list(
            "S3DataType"="S3Prefix",
            "S3Uri"="{{ record }}",
            "S3DataDistributionType"="ShardedByS3Key"
          )
        ),
        "ChannelName"="train"
      )
    ),
    "HyperParameters"=list(
      "num_topics"="10",
      "epochs"="32",
      "feature_dim"="100",
      "mini_batch_size"="256"
    )
  )
  expect_equal(config[sort(names(config))], expected_config[sort(names(expected_config))])
})

test_that("test_amazon_alg_training_config_all_args", {
  sms = sagemaker_session()
  ntm_estimator = NTM$new(
    role="{{ role }}",
    num_topics=10,
    instance_count="{{ instance_count }}",
    instance_type="ml.c4.2xlarge",
    volume_size="{{ volume_size }}",
    volume_kms_key="{{ volume_kms_key }}",
    max_run="{{ max_run }}",
    input_mode="Pipe",
    output_path="{{ output_path }}",
    output_kms_key="{{ output_volume_kms_key }}",
    base_job_name="{{ base_job_name }}",
    tags=list(list("{{ key }}"="{{ value }}")),
    subnets=list("{{ subnet }}"),
    security_group_ids=list("{{ security_group_ids }}"),
    sagemaker_session=sms
  )

  ntm_estimator$epochs = 32

  record = RecordSet$new("{{ record }}", 10000, 100, "S3Prefix")

  with_mock(
    `sagemaker.core::sagemaker_timestamp` = mock_fun(TIME_STAMP), {
      config = training_config(ntm_estimator, record, mini_batch_size=256)
  })
  expected_config = list(
    "AlgorithmSpecification"=list(
      "TrainingImage"="174872318107.dkr.ecr.us-west-2.amazonaws.com/ntm:1",
      "TrainingInputMode"="Pipe"
    ),
    "OutputDataConfig"=list(
      "S3OutputPath"="{{ output_path }}",
      "KmsKeyId"="{{ output_volume_kms_key }}"
    ),
    "TrainingJobName"=sprintf("{{ base_job_name }}-%s", TIME_STAMP),
    "StoppingCondition"=list("MaxRuntimeInSeconds"="{{ max_run }}"),
    "ResourceConfig"=list(
      "InstanceCount"="{{ instance_count }}",
      "InstanceType"="ml.c4.2xlarge",
      "VolumeSizeInGB"="{{ volume_size }}",
      "VolumeKmsKeyId"="{{ volume_kms_key }}"
    ),
    "RoleArn"="{{ role }}",
    "InputDataConfig"=list(
      list(
        "DataSource"=list(
          "S3DataSource"=list(
            "S3DataType"="S3Prefix",
            "S3Uri"="{{ record }}",
            "S3DataDistributionType"="ShardedByS3Key"
          )
        ),
        "ChannelName"="train"
      )
    ),
    "VpcConfig"=list(
      "Subnets"=list("{{ subnet }}"),
      "SecurityGroupIds"=list("{{ security_group_ids }}")
    ),
    "HyperParameters"=list(
      "num_topics"="10",
      "epochs"="32",
      "feature_dim"="100",
      "mini_batch_size"="256"
    ),
    "Tags"=list(list("{{ key }}"="{{ value }}"))
  )
  expect_equal(config[sort(names(config))], expected_config[sort(names(expected_config))])
})

test_that("test_framework_tuning_config", {
  sms = sagemaker_session()
  mxnet_estimator = MXNet$new(
    entry_point="{{ entry_point }}",
    source_dir="{{ source_dir }}",
    py_version="py3",
    framework_version="1.3.0",
    role="{{ role }}",
    instance_count=1,
    instance_type="ml.m4.xlarge",
    sagemaker_session=sms,
    base_job_name="{{ base_job_name }}",
    hyperparameters=list("batch_size"=100)
  )
  hyperparameter_ranges = list(
    "optimizer"=CategoricalParameter$new(list("sgd", "Adam")),
    "learning_rate"=ContinuousParameter$new(0.01, 0.2),
    "num_epoch"=IntegerParameter$new(10, 50)
  )
  objective_metric_name = "Validation-accuracy"
  metric_definitions = list(
   list("Name"="Validation-accuracy", "Regex"="Validation-accuracy=([0-9\\.]+)")
  )
  mxnet_tuner = HyperparameterTuner$new(
    estimator=mxnet_estimator,
    objective_metric_name=objective_metric_name,
    hyperparameter_ranges=hyperparameter_ranges,
    metric_definitions=metric_definitions,
    strategy="Bayesian",
    objective_type="Maximize",
    max_jobs="{{ max_job }}",
    max_parallel_jobs="{{ max_parallel_job }}",
    tags=list(list("{{ key }}"="{{ value }}")),
    base_tuning_job_name="{{ base_job_name }}"
  )
  data = "{{ training_data }}"

  with_mock(
    `sagemaker.core::sagemaker_timestamp` = mock_fun(TIME_STAMP),
    `sagemaker.core::sagemaker_short_timestamp` = mock_fun(TIME_STAMP),
    `fs::is_file` = mock_fun(TRUE),
    `sagemaker.core::tar_and_upload_dir` = mock_fun(),
    `sagemaker.core::parse_s3_url` = mock_fun(
      list(bucket = "output", key = sprintf("{{{{ base_job_name }}}}-%s/source/sourcedir.tar.gz", TIME_STAMP))), {
    config = tuning_config(mxnet_tuner, data)
  })
  expected_config = list(
    "HyperParameterTuningJobName"=sprintf("{{ base_job_name }}-%s", TIME_STAMP),
    "HyperParameterTuningJobConfig"=list(
      "Strategy"="Bayesian",
      "ResourceLimits"=list(
        "MaxNumberOfTrainingJobs"="{{ max_job }}",
        "MaxParallelTrainingJobs"="{{ max_parallel_job }}"
      ),
      "TrainingJobEarlyStoppingType"="Off",
      "HyperParameterTuningJobObjective"=list(
        "Type"="Maximize",
        "MetricName"="Validation-accuracy"
      ),
      "ParameterRanges"=list(
        "ContinuousParameterRanges"=list(
          list(
            "Name"="learning_rate",
            "MinValue"="0.01",
            "MaxValue"="0.2",
            "ScalingType"="Auto"
          )
        ),
        "CategoricalParameterRanges"=list(
          list("Name"="optimizer", "Values"=list('"sgd"', '"Adam"'))
        ),
        "IntegerParameterRanges"=list(
          list("Name"="num_epoch", "MinValue"="10", "MaxValue"="50", "ScalingType"="Auto")
        )
      )
    ),
    "TrainingJobDefinition"=list(
      "AlgorithmSpecification"=list(
        "TrainingImage"="520713654638.dkr.ecr.us-west-2.amazonaws.com/sagemaker-mxnet:1.3.0-cpu-py3",
        "TrainingInputMode"="File",
        "MetricDefinitions"=list(
          list("Name"="Validation-accuracy", "Regex"="Validation-accuracy=([0-9\\.]+)")
        )
      ),
      "OutputDataConfig"=list("S3OutputPath"="s3://output/"),
      "StoppingCondition"=list("MaxRuntimeInSeconds"=86400),
      "ResourceConfig"=list(
        "InstanceCount"=1,
        "InstanceType"="ml.m4.xlarge",
        "VolumeSizeInGB"=30
      ),
      "RoleArn"="{{ role }}",
      "InputDataConfig"=list(
        list(
          "DataSource"=list(
            "S3DataSource"=list(
              "S3DataType"="S3Prefix",
              "S3Uri"="{{ training_data }}",
              "S3DataDistributionType"="FullyReplicated"
            )
          ),
          "ChannelName"="training"
        )
      ),
      "StaticHyperParameters"=list(
        "batch_size"="100",
        "sagemaker_container_log_level"="20",
        "sagemaker_job_name"=sprintf('{{ base_job_name }}-%s', TIME_STAMP),
        "sagemaker_region"='us-west-2',
        "sagemaker_submit_directory"=sprintf(
          's3://output/{{ base_job_name }}-%s/source/sourcedir.tar.gz', TIME_STAMP),
        "sagemaker_program"='{{ entry_point }}',
        "sagemaker_estimator_class_name"='MXNet',
        "sagemaker_estimator_module"='sagemaker.mxnet.estimator'
      )
    ),
    "Tags"=list(list("{{ key }}"="{{ value }}")),
    "S3Operations"=list(
      "S3Upload"=list(
        list(
          "Path"="{{ source_dir }}",
          "Bucket"="output",
          "Key"=sprintf("{{ base_job_name }}-%s/source/sourcedir.tar.gz", TIME_STAMP),
          "Tar"=TRUE
        )
      )
    )
  )
  expect_equal(config[sort(names(config))], expected_config[sort(names(expected_config))])
})

test_that("test_multi_estimator_tuning_config", {
  estimator_dict = list()
  hyperparameter_ranges_dict = list()
  objective_metric_name_dict = list()
  metric_definitions_dict = list()

  sms = sagemaker_session()
  mxnet_estimator_name = "mxnet"
  estimator_dict[[mxnet_estimator_name]] = MXNet$new(
    entry_point="{{ entry_point }}",
    source_dir="{{ source_dir }}",
    py_version="py3",
    framework_version="1.3.0",
    role="{{ role }}",
    instance_count=1,
    instance_type="ml.m4.xlarge",
    sagemaker_session=sms,
    base_job_name="{{ base_job_name }}",
    hyperparameters=list("batch_size"=100)
  )
  hyperparameter_ranges_dict[[mxnet_estimator_name]] = list(
    "optimizer"=CategoricalParameter$new(list("sgd", "Adam")),
    "learning_rate"=ContinuousParameter$new(0.01, 0.2),
    "num_epoch"=IntegerParameter$new(10, 50)
  )
  objective_metric_name_dict[[mxnet_estimator_name]] = "Validation-accuracy"
  metric_definitions_dict[[mxnet_estimator_name]] = list(
    list("Name"="Validation-accuracy", "Regex"="Validation-accuracy=([0-9\\.]+)")
  )

  ll_estimator_name = "linear_learner"
  estimator_dict[[ll_estimator_name]] = LinearLearner$new(
    predictor_type="binary_classifier",
    role="{{ role }}",
    instance_count=1,
    instance_type="ml.c4.2xlarge",
    sagemaker_session=sms
  )
  hyperparameter_ranges_dict[[ll_estimator_name]] = list(
    "learning_rate"=ContinuousParameter$new(0.2, 0.5),
    "use_bias"=CategoricalParameter$new(list(TRUE, FALSE))
  )
  objective_metric_name_dict[[ll_estimator_name]] = "validation:binary_classification_accuracy"

  multi_estimator_tuner = HyperparameterTuner$public_methods$create(
    estimator_list=estimator_dict,
    objective_metric_name_list=objective_metric_name_dict,
    hyperparameter_ranges_list=hyperparameter_ranges_dict,
    metric_definitions_list=metric_definitions_dict,
    strategy="Bayesian",
    objective_type="Maximize",
    max_jobs="{{ max_job }}",
    max_parallel_jobs="{{ max_parallel_job }}",
    tags=list(list("{{ key }}"="{{ value }}")),
    base_tuning_job_name="{{ base_job_name }}"
  )
  data = setNames(
    list("{{ training_data_mxnet }}", RecordSet$new("{{ record }}", 10000, 100, "S3Prefix")),
    c(mxnet_estimator_name, ll_estimator_name)
  )
  with_mock(
    `sagemaker.core::sagemaker_timestamp` = mock_fun(TIME_STAMP),
    `sagemaker.core::sagemaker_short_timestamp` = mock_fun(TIME_STAMP),
    `fs::is_file` = mock_fun(TRUE),
    `sagemaker.core::tar_and_upload_dir` = mock_fun(),
    `sagemaker.core::parse_s3_url` = mock_fun(
      list(bucket = "output", key = sprintf("{{{{ base_job_name }}}}-%s/source/sourcedir.tar.gz", TIME_STAMP))), {
        config = tuning_config(multi_estimator_tuner, inputs=data, include_cls_metadata=list())
  })
  expected_config = list(
    "HyperParameterTuningJobName"=sprintf("{{ base_job_name }}-%s", TIME_STAMP),
    "HyperParameterTuningJobConfig"=list(
      "Strategy"="Bayesian",
      "ResourceLimits"=list(
        "MaxNumberOfTrainingJobs"="{{ max_job }}",
        "MaxParallelTrainingJobs"="{{ max_parallel_job }}"
      ),
      "TrainingJobEarlyStoppingType"="Off"
    ),
    "TrainingJobDefinitions"=list(
      list(
        "AlgorithmSpecification"=list(
          "TrainingImage"="174872318107.dkr.ecr.us-west-2.amazonaws.com/linear-learner:1",
          "TrainingInputMode"="File"
        ),
        "OutputDataConfig"=list("S3OutputPath"="s3://output/"),
        "StoppingCondition"=list("MaxRuntimeInSeconds"=86400),
        "ResourceConfig"=list(
          "InstanceCount"=1,
          "InstanceType"="ml.c4.2xlarge",
          "VolumeSizeInGB"=30
        ),
        "RoleArn"="{{ role }}",
        "InputDataConfig"=list(
          list(
            "DataSource"=list(
              "S3DataSource"=list(
                "S3DataType"="S3Prefix",
                "S3Uri"="{{ record }}",
                "S3DataDistributionType"="ShardedByS3Key"
              )
            ),
            "ChannelName"="train"
          )
        ),
        "StaticHyperParameters"=list(
          "predictor_type"="binary_classifier",
          "feature_dim"="100"
        ),
        "DefinitionName"="linear_learner",
        "TuningObjective"=list(
          "Type"="Maximize",
          "MetricName"="validation:binary_classification_accuracy" # Missing
        ),
        "HyperParameterRanges"=list(
          "ContinuousParameterRanges"=list(
            list(
              "Name"="learning_rate",
              "MinValue"="0.2",
              "MaxValue"="0.5",
              "ScalingType"="Auto"
            )
          ),
          "CategoricalParameterRanges"=list(
            list("Name"="use_bias", "Values"=list("TRUE", "FALSE"))
          ),
          "IntegerParameterRanges"=list()
        )
      ),
      list(
        "AlgorithmSpecification"=list(
          "TrainingImage"="520713654638.dkr.ecr.us-west-2.amazonaws.com/sagemaker-mxnet:1.3.0-cpu-py3",
          "TrainingInputMode"="File",
          "MetricDefinitions"=list(
            list("Name"="Validation-accuracy", "Regex"="Validation-accuracy=([0-9\\.]+)")
          )
        ),
        "OutputDataConfig"=list("S3OutputPath"="s3://output/"),
        "StoppingCondition"=list("MaxRuntimeInSeconds"=86400),
        "ResourceConfig"=list(
          "InstanceCount"=1,
          "InstanceType"="ml.m4.xlarge",
          "VolumeSizeInGB"=30
        ),
        "RoleArn"="{{ role }}",
        "InputDataConfig"=list(
          list(
            "DataSource"=list(
              "S3DataSource"=list(
                "S3DataType"="S3Prefix",
                "S3Uri"="{{ training_data_mxnet }}",
                "S3DataDistributionType"="FullyReplicated"
              )
            ),
            "ChannelName"="training"
          )
        ),
        "StaticHyperParameters"=list(
          "batch_size"="100",
          "sagemaker_container_log_level"="20",
          "sagemaker_job_name"=sprintf('{{ base_job_name }}-%s', TIME_STAMP),
          "sagemaker_region"='us-west-2',
          "sagemaker_submit_directory"=sprintf(
            's3://output/{{ base_job_name }}-%s/source/sourcedir.tar.gz', TIME_STAMP),
          "sagemaker_program"='{{ entry_point }}',
          "sagemaker_estimator_class_name"='MXNet',
          "sagemaker_estimator_module"='sagemaker.mxnet.estimator'
        ),
        "DefinitionName"="mxnet",
        "TuningObjective"=list("Type"="Maximize", "MetricName"="Validation-accuracy"),
        "HyperParameterRanges"=list(
          "ContinuousParameterRanges"=list(
            list(
              "Name"="learning_rate",
              "MinValue"="0.01",
              "MaxValue"="0.2",
              "ScalingType"="Auto"
            )
          ),
          "CategoricalParameterRanges"=list(
            list("Name"="optimizer", "Values"=list('"sgd"', '"Adam"'))
          ),
          "IntegerParameterRanges"=list(
            list(
              "Name"="num_epoch",
              "MinValue"="10",
              "MaxValue"="50",
              "ScalingType"="Auto"
            )
          )
        )
      )
    ),
    "S3Operations"=list(
      "S3Upload"=list(
        list(
          "Path"="{{ source_dir }}",
          "Bucket"="output",
          "Key"=sprintf("{{ base_job_name }}-%s/source/sourcedir.tar.gz", TIME_STAMP),
          "Tar"=TRUE
        )
      )
    ),
    "Tags"=list(list("{{ key }}"="{{ value }}"))
  )
  expect_equal(config[sort(names(config))], expected_config[sort(names(expected_config))])
})

test_that("test_merge_s3_operations", {
  s3_operations_list = list(
    list(
      "S3Upload"=list(
        list(
          "Bucket"="output",
          "Key"="base_job_name-111/source/sourcedir.tar.gz",
          "Path"="source_dir",
          "Tar"=TRUE
        )
      )
    ),
    list(
      "S3Upload"=list(
        list(
          "Bucket"="output",
          "Key"="base_job_name-111/source/sourcedir.tar.gz",
          "Path"="source_dir",
          "Tar"=TRUE
        )
      ),
      "S3CreateBucket"=list(list("Bucket"="output"))
    ),
    list(
      "S3Upload"=list(
        list(
          "Bucket"="output_2",
          "Key"="base_job_name-111/source/sourcedir_2.tar.gz",
          "Path"="source_dir_2",
          "Tar"=TRUE
        )
      )
    ),
    list("S3CreateBucket"=list(list("Bucket"="output_2"))),
    list()
  )

  expected_result = list(
    "S3Upload"=list(
      list(
        "Bucket"="output",
        "Key"="base_job_name-111/source/sourcedir.tar.gz",
        "Path"="source_dir",
        "Tar"=TRUE
      ),
      list(
        "Bucket"="output_2",
        "Key"="base_job_name-111/source/sourcedir_2.tar.gz",
        "Path"="source_dir_2",
        "Tar"=TRUE
      )
    ),
    "S3CreateBucket"=list(list("Bucket"="output"), list("Bucket"="output_2"))
  )

  sagemaker.workflow:::.merge_s3_operations(s3_operations_list)
  expect_equal(
    sagemaker.workflow:::.merge_s3_operations(s3_operations_list),
    expected_result
  )
})

test_that("test_byo_model_config", {
  sms = sagemaker_session()
  byo_model = Model$new(
    model_data="{{ model_data }}",
    image_uri="{{ image_uri }}",
    role="{{ role }}",
    env=list("{{ key }}"="{{ value }}"),
    name="model",
    sagemaker_session=sms
  )
  config = model_config(model=byo_model)
  expected_config = list(
    "ModelName"="model",
    "PrimaryContainer"=list(
      "Image"="{{ image_uri }}",
      "Environment"=list("{{ key }}"="{{ value }}"),
      "ModelDataUrl"="{{ model_data }}"
    ),
    "ExecutionRoleArn"="{{ role }}"
  )
  expect_equal(config, expected_config)
})

test_that("test_byo_framework_model_config", {
  sms = sagemaker_session()
  byo_model = FrameworkModel$new(
    model_data="{{ model_data }}",
    image_uri="{{ image_uri }}",
    role="{{ role }}",
    entry_point="{{ entry_point }}",
    source_dir="{{ source_dir }}",
    env=list("{{ key }}"="{{ value }}"),
    name="model",
    sagemaker_session=sms
  )
  config = model_config(model=byo_model, instance_type="ml.c4.xlarge")
  expected_config = list(
    "ModelName"="model",
    "PrimaryContainer"=list(
      "Image"="{{ image_uri }}",
      "Environment"=list(
        "{{ key }}"="{{ value }}",
        "SAGEMAKER_PROGRAM"="{{ entry_point }}",
        "SAGEMAKER_SUBMIT_DIRECTORY"="s3://output/model/source/sourcedir.tar.gz",
        "SAGEMAKER_CONTAINER_LOG_LEVEL"="20",
        "SAGEMAKER_REGION"="us-west-2"
      ),
      "ModelDataUrl"="{{ model_data }}"
    ),
    "ExecutionRoleArn"="{{ role }}",
    "S3Operations"=list(
      "S3Upload"=list(
        list(
          "Path"="{{ source_dir }}",
          "Bucket"="output",
          "Key"="model/source/sourcedir.tar.gz",
          "Tar"=TRUE
        )
      )
    )
  )
  expect_equal(config, expected_config)
})

test_that("test_framework_model_config",{
  sms = sagemaker_session()
  chainer_model = ChainerModel$new(
    model_data="{{ model_data }}",
    role="{{ role }}",
    entry_point="{{ entry_point }}",
    source_dir="{{ source_dir }}",
    image_uri=NULL,
    py_version="py3",
    framework_version="5.0.0",
    model_server_workers="{{ model_server_worker }}",
    sagemaker_session=sms
  )
  with_mock(
    `sagemaker.core::sagemaker_timestamp` = mock_fun(TIME_STAMP), {
      config = model_config(model=chainer_model, instance_type="ml.c4.xlarge")
  })
  expected_config = list(
    "ModelName"=sprintf("sagemaker-chainer-%s", TIME_STAMP),
    "PrimaryContainer"=list(
      "Image"="520713654638.dkr.ecr.us-west-2.amazonaws.com/sagemaker-chainer:5.0.0-cpu-py3",
      "Environment"=list(
        "SAGEMAKER_PROGRAM"="{{ entry_point }}",
        "SAGEMAKER_SUBMIT_DIRECTORY"=sprintf(
          "s3://output/sagemaker-chainer-%s/source/sourcedir.tar.gz", TIME_STAMP),
        "SAGEMAKER_CONTAINER_LOG_LEVEL"="20",
        "SAGEMAKER_REGION"="us-west-2",
        "SAGEMAKER_MODEL_SERVER_WORKERS"="{{ model_server_worker }}"
      ),
      "ModelDataUrl"="{{ model_data }}"
    ),
    "ExecutionRoleArn"="{{ role }}",
    "S3Operations"=list(
      "S3Upload"=list(
        list(
          "Path"="{{ source_dir }}",
          "Bucket"="output",
          "Key"=sprintf("sagemaker-chainer-%s/source/sourcedir.tar.gz", TIME_STAMP),
          "Tar"=TRUE
        )
      )
    )
  )
  expect_equal(config[sort(names(config))], expected_config[sort(names(expected_config))])
})

test_that("test_amazon_alg_model_config", {
  sms = sagemaker_session()
  pca_model = PCAModel$new(
    model_data="{{ model_data }}", role="{{ role }}", sagemaker_session=sms
  )
  with_mock(
    `sagemaker.core::sagemaker_timestamp` = mock_fun(TIME_STAMP), {
      config = model_config(model=pca_model)
  })
  expected_config = list(
    "ModelName"=sprintf("pca-%s", TIME_STAMP),
    "PrimaryContainer"=list(
      "Image"="174872318107.dkr.ecr.us-west-2.amazonaws.com/pca:1",
      "Environment"=list(),
      "ModelDataUrl"="{{ model_data }}"
    ),
    "ExecutionRoleArn"="{{ role }}"
  )
  expect_equal(config[sort(names(config))], expected_config[sort(names(expected_config))])
})

test_that("test_model_config_from_framework_estimator", {
  sms = sagemaker_session()
  mxnet_estimator = MXNet$new(
    entry_point="{{ entry_point }}",
    source_dir="{{ source_dir }}",
    py_version="py3",
    framework_version="1.6.0",
    role="{{ role }}",
    instance_count=1,
    instance_type="ml.m4.xlarge",
    sagemaker_session=sms,
    base_job_name="{{ base_job_name }}",
    hyperparameters=list("batch_size"=100)
  )

  data = "{{ training_data }}"

  # simulate training
  with_mock(
    `sagemaker.core::sagemaker_timestamp` = mock_fun(TIME_STAMP),
    `fs::is_file` = mock_fun(TRUE),
    `sagemaker.core::tar_and_upload_dir` = mock_fun(),
    `sagemaker.core::parse_s3_url` = mock_fun(list(
      bucket = "output",key=sprintf("{{{{ base_job_name }}}}-%s/source/sourcedir.tar.gz", TIME_STAMP))
    ), {
      training_config(mxnet_estimator, data)
      config = model_config_from_estimator(
        estimator=mxnet_estimator,
        task_id="task_id",
        task_type="training",
        instance_type="ml.c4.xlarge"
      )
  })
  expected_config = list(
    "ModelName"=sprintf("mxnet-inference-%s", TIME_STAMP),
    "PrimaryContainer"=list(
      "Image"="763104351884.dkr.ecr.us-west-2.amazonaws.com/mxnet-inference:1.6.0-cpu-py3",
      "Environment"=list(
        "SAGEMAKER_PROGRAM"="{{ entry_point }}",
        "SAGEMAKER_SUBMIT_DIRECTORY"=paste0(
          "s3://output/{{ ti.xcom_pull(task_ids='task_id')['Training']",
          "['TrainingJobName'] }}/source/sourcedir.tar.gz"),
        "SAGEMAKER_CONTAINER_LOG_LEVEL"="20",
        "SAGEMAKER_REGION"="us-west-2"
      ),
      "ModelDataUrl"=paste0(
        "s3://output/{{ ti.xcom_pull(task_ids='task_id')['Training']['TrainingJobName'] }}",
        "/output/model.tar.gz")
    ),
    "ExecutionRoleArn"="{{ role }}"
  )
  expect_equal(config[sort(names(config))], expected_config[sort(names(expected_config))])
})

test_that("test_model_config_from_amazon_alg_estimator", {
  sms = sagemaker_session()
  knn_estimator = KNN$new(
    role="{{ role }}",
    instance_count="{{ instance_count }}",
    instance_type="ml.m4.xlarge",
    k=16,
    sample_size=128,
    predictor_type="regressor",
    sagemaker_session=sms
  )

  record = RecordSet$new("{{ record }}", 10000, 100, "S3Prefix")

  with_mock(
    `sagemaker.core::sagemaker_timestamp` = mock_fun(TIME_STAMP), {
      # simulate training
      training_config(knn_estimator, record, mini_batch_size=256)
      config = model_config_from_estimator(
        estimator=knn_estimator, task_id="task_id", task_type="tuning"
      )
  })
  expected_config = list(
    "ModelName"=sprintf("knn-%s", TIME_STAMP),
    "PrimaryContainer"=list(
      "Image"="174872318107.dkr.ecr.us-west-2.amazonaws.com/knn:1",
      "Environment"=list(),
      "ModelDataUrl"=paste0(
        "s3://output/{{ ti.xcom_pull(task_ids='task_id')['Tuning']['BestTrainingJob']",
        "['TrainingJobName'] }}/output/model.tar.gz"
      )
    ),
    "ExecutionRoleArn"="{{ role }}"
  )
  expect_equal(config[sort(names(config))], expected_config[sort(names(expected_config))])
})

test_that("test_transform_config", {
  sms = sagemaker_session()
  tf_transformer = Transformer$new(
    model_name="tensorflow-model",
    instance_count="{{ instance_count }}",
    instance_type="ml.p2.xlarge",
    strategy="SingleRecord",
    assemble_with="Line",
    output_path="{{ output_path }}",
    output_kms_key="{{ kms_key }}",
    accept="{{ accept }}",
    max_concurrent_transforms="{{ max_parallel_job }}",
    max_payload="{{ max_payload }}",
    tags=list(list("{{ key }}"="{{ value }}")),
    env=list("{{ key }}"="{{ value }}"),
    base_transform_job_name="tensorflow-transform",
    sagemaker_session=sms,
    volume_kms_key="{{ kms_key }}"
  )

  data = "{{ transform_data }}"

  with_mock(
    `sagemaker.core::sagemaker_timestamp` = mock_fun(TIME_STAMP), {
      config = transform_config(
        tf_transformer,
        data,
        data_type="S3Prefix",
        content_type="{{ content_type }}",
        compression_type="{{ compression_type }}",
        split_type="{{ split_type }}",
        input_filter="{{ input_filter }}",
        output_filter="{{ output_filter }}",
        join_source="{{ join_source }}"
      )
  })
  expected_config = list(
    "TransformJobName"=sprintf("tensorflow-transform-%s", TIME_STAMP),
    "ModelName"="tensorflow-model",
    "TransformInput"=list(
      "DataSource"=list(
        "S3DataSource"=list("S3DataType"="S3Prefix", "S3Uri"="{{ transform_data }}")
      ),
      "ContentType"="{{ content_type }}",
      "CompressionType"="{{ compression_type }}",
      "SplitType"="{{ split_type }}"
    ),
    "TransformOutput"=list(
      "S3OutputPath"="{{ output_path }}",
      "KmsKeyId"="{{ kms_key }}",
      "AssembleWith"="Line",
      "Accept"="{{ accept }}"
    ),
    "TransformResources"=list(
      "InstanceCount"="{{ instance_count }}",
      "InstanceType"="ml.p2.xlarge",
      "VolumeKmsKeyId"="{{ kms_key }}"
    ),
    "BatchStrategy"="SingleRecord",
    "MaxConcurrentTransforms"="{{ max_parallel_job }}",
    "MaxPayloadInMB"="{{ max_payload }}",
    "Environment"=list("{{ key }}"="{{ value }}"),
    "Tags"=list(list("{{ key }}"="{{ value }}")),
    "DataProcessing"=list(
      "InputFilter"="{{ input_filter }}",
      "OutputFilter"="{{ output_filter }}",
      "JoinSource"="{{ join_source }}"
    )
  )
  expect_equal(config[sort(names(config))], expected_config[sort(names(expected_config))])
})

test_that("test_transform_config_from_framework_estimator", {
  sms = sagemaker_session()
  mxnet_estimator = MXNet$new(
    entry_point="{{ entry_point }}",
    source_dir="{{ source_dir }}",
    py_version="py3",
    framework_version="1.6.0",
    role="{{ role }}",
    instance_count=1,
    instance_type="ml.m4.xlarge",
    sagemaker_session=sms,
    base_job_name="{{ base_job_name }}",
    hyperparameters=list("batch_size"=100)
  )

  train_data = "{{ train_data }}"
  transform_data = "{{ transform_data }}"

  with_mock(
    `sagemaker.core::sagemaker_timestamp` = mock_fun(TIME_STAMP),
    `fs::is_file` = mock_fun(TRUE),
    `sagemaker.core::tar_and_upload_dir` = mock_fun(),
    `sagemaker.core::parse_s3_url` = mock_fun(
      list(bucket = "output", key = sprintf("{{{{ base_job_name }}}}-%s/source/sourcedir.tar.gz", TIME_STAMP))), {
      # simulate training
      training_config(mxnet_estimator, train_data)

      config = transform_config_from_estimator(
        estimator=mxnet_estimator,
        task_id="task_id",
        task_type="training",
        instance_count="{{ instance_count }}",
        instance_type="ml.p2.xlarge",
        data=transform_data,
        input_filter="{{ input_filter }}",
        output_filter="{{ output_filter }}",
        join_source="{{ join_source }}"
      )
  })
  expected_config = list(
    "Model"=list(
      "ModelName"=sprintf("mxnet-inference-%s", TIME_STAMP),
      "PrimaryContainer"=list(
        "Image"="763104351884.dkr.ecr.us-west-2.amazonaws.com/mxnet-inference:1.6.0-gpu-py3",
        "Environment"=list(
          "SAGEMAKER_PROGRAM"="{{ entry_point }}",
          "SAGEMAKER_SUBMIT_DIRECTORY"=paste0(
            "s3://output/{{ ti.xcom_pull(task_ids='task_id')",
            "['Training']['TrainingJobName'] }}",
            "/source/sourcedir.tar.gz"),
          "SAGEMAKER_CONTAINER_LOG_LEVEL"="20",
          "SAGEMAKER_REGION"="us-west-2"
        ),
        "ModelDataUrl"=paste0(
          "s3://output/{{ ti.xcom_pull(task_ids='task_id')['Training']['TrainingJobName'] }}",
          "/output/model.tar.gz")
      ),
      "ExecutionRoleArn"="{{ role }}"
    ),
    "Transform"=list(
      "TransformJobName"=sprintf("{{ base_job_name }}-%s", TIME_STAMP),
      "ModelName"=sprintf("mxnet-inference-%s", TIME_STAMP),
      "TransformInput"=list(
        "DataSource"=list(
          "S3DataSource"=list("S3DataType"="S3Prefix", "S3Uri"="{{ transform_data }}")
        )
      ),
      "TransformOutput"=list("S3OutputPath"=sprintf("s3://output/{{ base_job_name }}-%s", TIME_STAMP)),
      "TransformResources"=list(
        "InstanceCount"="{{ instance_count }}",
        "InstanceType"="ml.p2.xlarge"
      ),
      "DataProcessing"=list(
        "InputFilter"="{{ input_filter }}",
        "OutputFilter"="{{ output_filter }}",
        "JoinSource"="{{ join_source }}"
      ),
      "Environment"=list()
    )
  )
  expect_equal(config[sort(names(config))], expected_config[sort(names(expected_config))])
})

test_that("test_transform_config_from_amazon_alg_estimator", {
  sms = sagemaker_session()
  knn_estimator = KNN$new(
    role="{{ role }}",
    instance_count="{{ instance_count }}",
    instance_type="ml.m4.xlarge",
    k=16,
    sample_size=128,
    predictor_type="regressor",
    sagemaker_session=sms
  )
  record = RecordSet$new("{{ record }}", 10000, 100, "S3Prefix")
  transform_data = "{{ transform_data }}"

  with_mock(
    `sagemaker.core::sagemaker_timestamp` = mock_fun(TIME_STAMP), {
      # simulate training
      training_config(knn_estimator, record, mini_batch_size=256)
      config = transform_config_from_estimator(
        estimator=knn_estimator,
        task_id="task_id",
        task_type="training",
        instance_count="{{ instance_count }}",
        instance_type="ml.p2.xlarge",
        data=transform_data
      )
  })
  expected_config = list(
    "Model"=list(
      "ModelName"=sprintf("knn-%s", TIME_STAMP),
      "PrimaryContainer"=list(
        "Image"="174872318107.dkr.ecr.us-west-2.amazonaws.com/knn:1",
        "Environment"=list(),
        "ModelDataUrl"=paste0(
          "s3://output/{{ ti.xcom_pull(task_ids='task_id')['Training']['TrainingJobName'] }}",
          "/output/model.tar.gz")
      ),
      "ExecutionRoleArn"="{{ role }}"
    ),
    "Transform"=list(
      "TransformJobName"=sprintf("knn-%s", TIME_STAMP),
      "ModelName"=sprintf("knn-%s", TIME_STAMP),
      "TransformInput"=list(
        "DataSource"=list(
          "S3DataSource"=list("S3DataType"="S3Prefix", "S3Uri"="{{ transform_data }}")
        )
      ),
      "TransformOutput"=list("S3OutputPath"=sprintf("s3://output/knn-%s", TIME_STAMP)),
      "TransformResources"=list(
        "InstanceCount"="{{ instance_count }}",
        "InstanceType"="ml.p2.xlarge"
      )
    )
  )
  expect_equal(config[sort(names(config))], expected_config[sort(names(expected_config))])
})

test_that("test_deploy_framework_model_config", {
  sms = sagemaker_session()
  chainer_model = ChainerModel$new(
    model_data="{{ model_data }}",
    role="{{ role }}",
    entry_point="{{ entry_point }}",
    source_dir="{{ source_dir }}",
    image_uri=NULL,
    py_version="py3",
    framework_version="5.0.0",
    model_server_workers="{{ model_server_worker }}",
    sagemaker_session=sms
  )
  with_mock(
    `sagemaker.core::sagemaker_timestamp` = mock_fun(TIME_STAMP),{
      config = deploy_config(
        chainer_model, initial_instance_count="{{ instance_count }}", instance_type="ml.m4.xlarge"
      )
  })
  expected_config = list(
    "Model"=list(
      "ModelName"=sprintf("sagemaker-chainer-%s", TIME_STAMP),
      "PrimaryContainer"=list(
        "Image"="520713654638.dkr.ecr.us-west-2.amazonaws.com/sagemaker-chainer:5.0.0-cpu-py3",
        "Environment"=list(
          "SAGEMAKER_PROGRAM"="{{ entry_point }}",
          "SAGEMAKER_SUBMIT_DIRECTORY"=sprintf(
            "s3://output/sagemaker-chainer-%s/source/sourcedir.tar.gz", TIME_STAMP),
          "SAGEMAKER_CONTAINER_LOG_LEVEL"="20",
          "SAGEMAKER_REGION"="us-west-2",
          "SAGEMAKER_MODEL_SERVER_WORKERS"="{{ model_server_worker }}"
        ),
        "ModelDataUrl"="{{ model_data }}"
      ),
      "ExecutionRoleArn"="{{ role }}"
    ),
    "EndpointConfig"=list(
      "EndpointConfigName"=sprintf("sagemaker-chainer-%s", TIME_STAMP),
      "ProductionVariants"=list(
        list(
          "ModelName"=sprintf("sagemaker-chainer-%s", TIME_STAMP),
          "VariantName"="AllTraffic",
          "InitialVariantWeight"=1,
          "InitialInstanceCount"="{{ instance_count }}",
          "InstanceType"="ml.m4.xlarge"
        )
      )
    ),
    "Endpoint"=list(
      "EndpointName"=sprintf("sagemaker-chainer-%s", TIME_STAMP),
      "EndpointConfigName"=sprintf("sagemaker-chainer-%s", TIME_STAMP)
    ),
    "S3Operations"=list(
      "S3Upload"=list(
        list(
          "Path"="{{ source_dir }}",
          "Bucket"="output",
          "Key"=sprintf("sagemaker-chainer-%s/source/sourcedir.tar.gz", TIME_STAMP),
          "Tar"=TRUE
        )
      )
    )
  )
  expect_equal(config[sort(names(config))], expected_config[sort(names(expected_config))])
})

test_that("test_deploy_amazon_alg_model_config", {
  sms = sagemaker_session()
  pca_model = PCAModel$new(
    model_data="{{ model_data }}", role="{{ role }}", sagemaker_session=sms
  )
  with_mock(
    `sagemaker.core::sagemaker_timestamp` = mock_fun(TIME_STAMP),{
      config = deploy_config(
        pca_model, initial_instance_count="{{ instance_count }}", instance_type="ml.c4.xlarge"
      )
  })
  expected_config = list(
    "Model"=list(
      "ModelName"=sprintf("pca-%s", TIME_STAMP),
      "PrimaryContainer"=list(
        "Image"="174872318107.dkr.ecr.us-west-2.amazonaws.com/pca:1",
        "Environment"=list(),
        "ModelDataUrl"="{{ model_data }}"
      ),
      "ExecutionRoleArn"="{{ role }}"
    ),
    "EndpointConfig"=list(
      "EndpointConfigName"=sprintf("pca-%s", TIME_STAMP),
      "ProductionVariants"=list(
        list(
          "ModelName"=sprintf("pca-%s", TIME_STAMP),
          "VariantName"="AllTraffic",
          "InitialVariantWeight"=1,
          "InitialInstanceCount"="{{ instance_count }}",
          "InstanceType"="ml.c4.xlarge"
        )
      )
    ),
    "Endpoint"=list(
      "EndpointName"=sprintf("pca-%s", TIME_STAMP),
      "EndpointConfigName"=sprintf("pca-%s", TIME_STAMP)
    )
  )
  expect_equal(config[sort(names(config))], expected_config[sort(names(expected_config))])
})

test_that("test_deploy_config_from_framework_estimator", {
  sms = sagemaker_session()
  mxnet_estimator = MXNet$new(
    entry_point="{{ entry_point }}",
    source_dir="{{ source_dir }}",
    py_version="py3",
    framework_version="1.6.0",
    role="{{ role }}",
    instance_count=1,
    instance_type="ml.m4.xlarge",
    sagemaker_session=sms,
    base_job_name="{{ base_job_name }}",
    hyperparameters=list("batch_size"=100)
  )

  train_data = "{{ train_data }}"

  with_mock(
    `sagemaker.core::sagemaker_timestamp` = mock_fun(TIME_STAMP),
    `fs::is_file` = mock_fun(TRUE),
    `sagemaker.core::tar_and_upload_dir` = mock_fun(),
    `sagemaker.core::parse_s3_url` = mock_fun(
      list(
        bucket = "output",
        key = sprintf("{{{{ base_job_name }}}}-%s/source/sourcedir.tar.gz", TIME_STAMP)
      )
    ), {
      # simulate training
      training_config(mxnet_estimator, train_data)

      config = deploy_config_from_estimator(
        estimator=mxnet_estimator,
        task_id="task_id",
        task_type="training",
        initial_instance_count="{{ instance_count}}",
        instance_type="ml.c4.large",
        endpoint_name="mxnet-endpoint"
      )
  })
  expected_config = list(
    "Model"=list(
      "ModelName"=sprintf("mxnet-inference-%s", TIME_STAMP),
      "PrimaryContainer"=list(
        "Image"="763104351884.dkr.ecr.us-west-2.amazonaws.com/mxnet-inference:1.6.0-cpu-py3",
        "Environment"=list(
          "SAGEMAKER_PROGRAM"="{{ entry_point }}",
          "SAGEMAKER_SUBMIT_DIRECTORY"=paste0(
            "s3://output/{{ ti.xcom_pull(task_ids='task_id')['Training']",
            "['TrainingJobName'] }}/source/sourcedir.tar.gz"),
          "SAGEMAKER_CONTAINER_LOG_LEVEL"="20",
          "SAGEMAKER_REGION"="us-west-2"
        ),
        "ModelDataUrl"=paste0(
          "s3://output/{{ ti.xcom_pull(task_ids='task_id')['Training']['TrainingJobName'] }}",
          "/output/model.tar.gz")
      ),
      "ExecutionRoleArn"="{{ role }}"
    ),
    "EndpointConfig"=list(
      "EndpointConfigName"=sprintf("mxnet-inference-%s", TIME_STAMP),
      "ProductionVariants"=list(
        list(
          "ModelName"=sprintf("mxnet-inference-%s", TIME_STAMP),
          "VariantName"="AllTraffic",
          "InitialVariantWeight"=1,
          "InitialInstanceCount"="{{ instance_count}}",
          "InstanceType"="ml.c4.large"
        )
      )
    ),
    "Endpoint"=list(
      "EndpointName"="mxnet-endpoint",
      "EndpointConfigName"=sprintf("mxnet-inference-%s", TIME_STAMP)
    )
  )
  expect_equal(config[sort(names(config))], expected_config[sort(names(expected_config))])
})

test_that("test_deploy_config_from_amazon_alg_estimator", {
  sms = sagemaker_session()
  knn_estimator = KNN$new(
    role="{{ role }}",
    instance_count="{{ instance_count }}",
    instance_type="ml.m4.xlarge",
    k=16,
    sample_size=128,
    predictor_type="regressor",
    sagemaker_session=sms
  )

  record = RecordSet$new("{{ record }}", 10000, 100, "S3Prefix")

  with_mock(
    `sagemaker.core::sagemaker_timestamp` = mock_fun(TIME_STAMP), {
      # simulate training
      training_config(knn_estimator, record, mini_batch_size=256)

      config = deploy_config_from_estimator(
        estimator=knn_estimator,
        task_id="task_id",
        task_type="tuning",
        initial_instance_count="{{ instance_count }}",
        instance_type="ml.p2.xlarge"
      )
  })
  expected_config = list(
    "Model"=list(
      "ModelName"=sprintf("knn-%s", TIME_STAMP),
      "PrimaryContainer"=list(
        "Image"="174872318107.dkr.ecr.us-west-2.amazonaws.com/knn:1",
        "Environment"=list(),
        "ModelDataUrl"=paste0(
          "s3://output/{{ ti.xcom_pull(task_ids='task_id')['Tuning']['BestTrainingJob']",
          "['TrainingJobName'] }}/output/model.tar.gz")
      ),
      "ExecutionRoleArn"="{{ role }}"
    ),
    "EndpointConfig"=list(
      "EndpointConfigName"=sprintf("knn-%s", TIME_STAMP),
      "ProductionVariants"=list(
        list(
          "ModelName"=sprintf("knn-%s", TIME_STAMP),
          "VariantName"="AllTraffic",
          "InitialVariantWeight"=1,
          "InitialInstanceCount"="{{ instance_count }}",
          "InstanceType"="ml.p2.xlarge"
        )
      )
    ),
    "Endpoint"=list(
      "EndpointName"=sprintf("knn-%s", TIME_STAMP),
      "EndpointConfigName"=sprintf("knn-%s", TIME_STAMP)
    )
  )
  expect_equal(config[sort(names(config))], expected_config[sort(names(expected_config))])
})

test_that("test_processing_config", {
  sms = sagemaker_session()
  network_config = NetworkConfig$new(
    encrypt_inter_container_traffic=FALSE,
    enable_network_isolation=TRUE,
    security_group_ids=list("sg1"),
    subnets=list("subnet1")
  )

  processor = Processor$new(
    role="arn:aws:iam::0122345678910:role/SageMakerPowerUser",
    image_uri="{{ image_uri }}",
    instance_count=2,
    instance_type="ml.p2.xlarge",
    entrypoint="{{ entrypoint }}",
    volume_size_in_gb=30,
    volume_kms_key="{{ kms_key }}",
    output_kms_key="{{ kms_key }}",
    max_runtime_in_seconds=3600,
    base_job_name="processing_base_job_name",
    sagemaker_session=sms,
    tags=list(list("{{ key }}"="{{ value }}")),
    env=list("{{ key }}"="{{ value }}"),
    network_config=network_config
  )

  outputs = list(
    ProcessingOutput$new(
      output_name="AnalyticsOutputName",
      source="{{ Local Path }}",
      destination="{{ S3Uri }}",
      s3_upload_mode="EndOfJob"
    )
  )
  inputs = list(
    ProcessingInput$new(
      input_name="AnalyticsInputName",
      source="{{ S3Uri }}",
      destination="{{ Local Path }}",
      s3_data_type="S3Prefix",
      s3_input_mode="File",
      s3_data_distribution_type="FullyReplicated",
      s3_compression_type="None"
    )
  )
  experiment_config = list()
  experiment_config[["ExperimentName"]] = "ExperimentName"
  experiment_config[["TrialName"]] = "TrialName"
  experiment_config[["TrialComponentDisplayName"]] = "TrialComponentDisplayName"

  with_mock(
    `sagemaker.core::sagemaker_timestamp` = mock_fun(TIME_STAMP), {
    config = processing_config(
      processor,
      inputs=inputs,
      outputs=outputs,
      job_name="ProcessingJobName",
      container_arguments=list("container_arg"),
      container_entrypoint=list("container_entrypoint"),
      kms_key_id="KmsKeyID",
      experiment_config=experiment_config
    )
  })
  expected_config = list(
    "AppSpecification"=list(
      "ImageUri"="{{ image_uri }}",
      "ContainerArguments"=list("container_arg"),
      "ContainerEntrypoint"=list("container_entrypoint")
    ),
    "Environment"=list("{{ key }}"="{{ value }}"),
    "ExperimentConfig"=list(
      "ExperimentName"="ExperimentName",
      "TrialName"="TrialName",
      "TrialComponentDisplayName"="TrialComponentDisplayName"
    ),
    "ProcessingInputs"=list(
      list(
        "InputName"="AnalyticsInputName",
        "AppManaged"=FALSE,
        "S3Input"=list(
          "S3Uri"="{{ S3Uri }}",
          "LocalPath"="{{ Local Path }}",
          "S3DataType"="S3Prefix",
          "S3InputMode"="File",
          "S3DataDistributionType"="FullyReplicated",
          "S3CompressionType"="None"
        )
      )
    ),
    "ProcessingJobName"="ProcessingJobName",
    "ProcessingOutputConfig"=list(
      "Outputs"=list(
        list(
          "OutputName"="AnalyticsOutputName",
          "AppManaged"=FALSE,
          "S3Output"=list(
            "S3Uri"="{{ S3Uri }}",
            "LocalPath"="{{ Local Path }}",
            "S3UploadMode"="EndOfJob"
          )
        )
      ),
      "KmsKeyId"="KmsKeyID"
    ),
    "ProcessingResources"=list(
      "ClusterConfig"=list(
        "InstanceCount"=2,
        "InstanceType"="ml.p2.xlarge",
        "VolumeSizeInGB"=30,
        "VolumeKmsKeyId"="{{ kms_key }}"
      )
    ),
    "RoleArn"="arn:aws:iam::0122345678910:role/SageMakerPowerUser",
    "StoppingCondition"=list("MaxRuntimeInSeconds"=3600),
    "Tags"=list(list("{{ key }}"="{{ value }}")),
    "NetworkConfig"=list(
      "EnableNetworkIsolation"=TRUE,
      "EnableInterContainerTrafficEncryption"=FALSE,
      "VpcConfig"=list("SecurityGroupIds"=list("sg1"), "Subnets"=list("subnet1"))
    )
  )
  expect_equal(config[sort(names(config))], expected_config[sort(names(expected_config))])
})
