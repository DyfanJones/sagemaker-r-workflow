# NOTE: This code has been modified from AWS Sagemaker Python:
# https://github.com/aws/sagemaker-python-sdk/blob/dev/tests/unit/sagemaker/workflow/test_pipeline.py

library(sagemaker.core)
library(sagemaker.common)
library(sagemaker.mlcore)
library(sagemaker.mlframework)


CustomStep = R6::R6Class("CustomStep",
  inherit = Step,
  public = list(
    initialize = function(name, input_data, display_name=NULL, description=NULL){
      self$input_data = input_data
      super$initialize(name, display_name, description, StepTypeEnum$TRAINING)
      path = sprintf("Steps.%s", name)
      prop = Properties$new(path=path)
      prop$S3Uri = Properties$new(sprintf("%s.S3Uri", path))
      private$.properties = prop
    }
  ),
  active = list(
    arguments = function(){
      return(list("input_data"=self$input_data))
    },

    properties = function(){
      return(private$.properties)
    }
  ),
  lock_objects = F
)

role_arn = "arn:role"

sagemaker_session_mock = function(){
  paws_mock = Mock$new(
    name = "PawsSession"
  )

  sms = Mock$new(
    name="Session",
    paws_session=paws_mock,
    config=NULL,
    local_mode=FALSE,
    s3=NULL
  )

  sagemaker = Mock$new()
  sagemaker$.call_args("create_pipeline")
  sagemaker$.call_args("update_pipeline")
  sagemaker$.call_args("delete_pipeline")
  sagemaker$.call_args("describe_pipeline")
  sagemaker$.call_args("describe_pipeline_execution")
  sagemaker$.call_args("start_pipeline_execution")
  sagemaker$.call_args("stop_pipeline_execution")
  sagemaker$.call_args("list_tags")
  sagemaker$.call_args("add_tags")
  s3_client = Mock$new()

  sms$.call_args("upload_string_as_file_body")
  sms$.call_args("default_bucket", return_value="s3_bucket")
  sms$.__enclos_env__$private$.default_bucket = "s3_bucket"

  sms$sagemaker = sagemaker
  sms$s3 = s3_client
  return(sms)
}

test_that("test_pipeline_create", {
  sms = sagemaker_session_mock()
  pipeline = Pipeline$new(
    name="MyPipeline",
    parameters=list(),
    steps=list(),
    sagemaker_session=sms
  )
  pipeline$create(role_arn=role_arn)
  expect_equal(
    sms$sagemaker$create_pipeline(..return_value = T), list(
      PipelineName="MyPipeline", RoleArn=role_arn, PipelineDefinition=pipeline$definition()
  ))
})

test_that("test_pipeline_create_with_parallelism_config", {
  sms = sagemaker_session_mock()
  pipeline = Pipeline$new(
    name="MyPipeline",
    parameters=list(),
    steps=list(),
    pipeline_experiment_config=ParallelismConfiguration$new(max_parallel_execution_steps=10),
    sagemaker_session=sms
  )
  pipeline$create(role_arn=role_arn)
  expect_equal(
    sms$sagemaker$create_pipeline(..return_value = T), list(
      PipelineName="MyPipeline",
      RoleArn=role_arn,
      PipelineDefinition=pipeline$definition()
  ))
})

test_that("test_large_pipeline_create", {
  sms = sagemaker_session_mock()
  parameter = ParameterString$new("MyStr")
  pipeline = Pipeline$new(
    name="MyPipeline",
    parameters=list(parameter),
    steps=rep(list(CustomStep$new(name="MyStep", input_data=parameter)),  2000),
    sagemaker_session=sms,
  )
  pipeline$create(role_arn=role_arn)
  expect_equal(sms$upload_string_as_file_body(..return_value = T), list(
    body=pipeline$definition(), bucket="s3_bucket", key="MyPipeline", kms_key=NULL
  ))
  expect_equal(sms$sagemaker$create_pipeline(..return_value = T), list(
    PipelineName="MyPipeline",
    RoleArn=role_arn,
    PipelineDefinitionS3Location=list("Bucket"="s3_bucket", "ObjectKey"="MyPipeline")
  ))
})

test_that("test_pipeline_update", {
  sms = sagemaker_session_mock()
  pipeline = Pipeline$new(
    name="MyPipeline",
    parameters=list(),
    steps=list(),
    sagemaker_session=sms
  )
  pipeline$update(role_arn=role_arn)
  expect_equal(sms$sagemaker$update_pipeline(..return_value = T), list(
    PipelineName="MyPipeline", RoleArn=role_arn, PipelineDefinition=pipeline$definition()
  ))
})

test_that("test_pipeline_update_with_parallelism_config", {
  sms = sagemaker_session_mock()
  pipeline = Pipeline$new(
    name="MyPipeline",
    parameters=list(),
    steps=list(),
    pipeline_experiment_config=ParallelismConfiguration$new(max_parallel_execution_steps=10),
    sagemaker_session=sms
  )
  pipeline$create(role_arn=role_arn)
  expect_equal(sms$sagemaker$create_pipeline(..return_value = T), list(
    PipelineName="MyPipeline",
    RoleArn=role_arn,
    PipelineDefinition=pipeline$definition()
    # ParallelismConfiguration=list("MaxParallelExecutionSteps"=10)
  ))
})

test_that("test_large_pipeline_update", {
  sms = sagemaker_session_mock()
  parameter = ParameterString$new("MyStr")
  pipeline = Pipeline$new(
    name="MyPipeline",
    parameters=list(parameter),
    steps=rep(list(CustomStep$new(name="MyStep", input_data=parameter)), 2000),
    sagemaker_session=sms
  )
  pipeline$update(role_arn=role_arn)
  expect_equal(sms$upload_string_as_file_body(..return_value = T), list(
    body=pipeline$definition(), bucket="s3_bucket", key="MyPipeline", kms_key=NULL
  ))
  expect_equal(sms$sagemaker$update_pipeline(..return_value = T), list(
    PipelineName="MyPipeline",
    RoleArn=role_arn,
    PipelineDefinitionS3Location=list("Bucket"="s3_bucket", "ObjectKey"="MyPipeline")
  ))
})

test_that("test_pipeline_upsert", {
  sms = sagemaker_session_mock()
  sms$sagemaker$.call_args(
    "create_pipeline",
    side_effect = function(...){
      .Data = list(
      message = "Pipeline names must be unique within ...",
      error_response = list(Code="ValidationException")
      )
      stop(structure(.Data, class = c("error", "condition")))
  })
  sms$sagemaker$.call_args("update_pipeline", return_value = list("PipelineArn"="mock_pipeline_arn"))
  sms$sagemaker$.call_args("list_tags", return_value = list(Tags=list(list("Key"="dummy", "Value"="dummy_tag"))))

  pipeline = Pipeline$new(
    name="MyPipeline",
    parameters=list(),
    steps=list(),
    sagemaker_session=sms
  )
  tags = list(
    list("Key"="foo", "Value"="abc"),
    list("Key"="bar", "Value"="xyz")
  )
  pipeline$upsert(role_arn=role_arn, tags=tags)
  expect_equal(sms$sagemaker$create_pipeline(..return_value = T), list(
    PipelineName="MyPipeline", RoleArn=role_arn, PipelineDefinition=pipeline$definition(), Tags=tags
  ))
  expect_equal(sms$sagemaker$update_pipeline(..return_value = T), list(
    PipelineName="MyPipeline", RoleArn=role_arn, PipelineDefinition=pipeline$definition()
  ))
  expect_equal(sms$sagemaker$list_tags(..return_value = T), list(
    ResourceArn="mock_pipeline_arn"
  ))
  tags = sagemaker.workflow:::list.append(tags, list("Key"="dummy", "Value"="dummy_tag"))
  expect_equal(sms$sagemaker$add_tags(..return_value = T), list(
    ResourceArn="mock_pipeline_arn", Tags=tags
  ))
})

test_that("test_pipeline_delete", {
  sms = sagemaker_session_mock()
  pipeline = Pipeline$new(
    name="MyPipeline",
    parameters=list(),
    steps=list(),
    sagemaker_session=sms
  )
  pipeline$delete()
  expect_equal(sms$sagemaker$delete_pipeline(..return_value = T), list(
    PipelineName="MyPipeline"
  ))
})

test_that("test_pipeline_describe", {
  sms = sagemaker_session_mock()
  pipeline = Pipeline$new(
    name="MyPipeline",
    parameters=list(),
    steps=list(),
    sagemaker_session=sms
  )
  pipeline$describe()
  expect_equal(sms$sagemaker$describe_pipeline(..return_value = T), list(
    PipelineName="MyPipeline"
  ))
})

test_that("test_pipeline_start", {
  sms = sagemaker_session_mock()
  sms$sagemaker$.call_args("start_pipeline_execution", return_value = list("PipelineExecutionArn"="my:arn"))
  pipeline = Pipeline$new(
    name="MyPipeline",
    parameters=list(ParameterString$new("alpha", "beta"), ParameterString$new("gamma", "delta")),
    steps=list(),
    sagemaker_session=sms
  )
  pipeline$start()
  expect_equal(sms$sagemaker$start_pipeline_execution(..return_value = T), list(
    PipelineName="MyPipeline"
  ))
  pipeline$start(execution_display_name="pipeline-execution")
  expect_equal(sms$sagemaker$start_pipeline_execution(..return_value = T), list(
    PipelineName="MyPipeline", PipelineExecutionDisplayName="pipeline-execution"
  ))
  pipeline$start(parameters=list(alpha="epsilon"))
  expect_equal(sms$sagemaker$start_pipeline_execution(..return_value = T), list(
    PipelineName="MyPipeline", PipelineParameters=list(list("Name"="alpha", "Value"="epsilon"))
  ))
})

test_that("test_pipeline_start_before_creation", {
  sms = sagemaker_session_mock()
  sms$sagemaker$.call_args("describe_pipeline", side_effect = function(...) stop("bar"))
  pipeline = Pipeline$new(
    name="MyPipeline",
    parameters=list(ParameterString$new("alpha", "beta"), ParameterString$new("gamma", "delta")),
    steps=list(),
    sagemaker_session=sms
  )
  expect_error(
    pipeline$start(),
    class = "ValueError"
  )
})

test_that("test_pipeline_basic", {
  sms = sagemaker_session_mock()
  parameter = ParameterString$new("MyStr")
  pipeline = Pipeline$new(
    name="MyPipeline",
    parameters=list(parameter),
    steps=list(CustomStep$new(name="MyStep", input_data=parameter)),
    sagemaker_session=sms
  )
  expect_equal(pipeline$to_request(), list(
    "Version"="2020-12-01",
    "Metadata"=list(),
    "Parameters"=list(list("Name"="MyStr", "Type"="String")),
    "PipelineExperimentConfig"=list(
      "ExperimentName"=ExecutionVariables$PIPELINE_NAME,
      "TrialName"=ExecutionVariables$PIPELINE_EXECUTION_ID
    ),
    "Steps"=list(list("Name"="MyStep", "Type"="Training", "Arguments"=list("input_data"=parameter)))
  ))
  expect_equal(jsonlite::parse_json(pipeline$definition()), list(
    "Version"="2020-12-01",
    "Metadata"=list(),
    "Parameters"=list(list("Name"="MyStr", "Type"="String")),
    "PipelineExperimentConfig"=list(
      "ExperimentName"=list("Get"="Execution.PipelineName"),
      "TrialName"=list("Get"="Execution.PipelineExecutionId")
    ),
    "Steps"=list(
      list(
        "Name"="MyStep",
        "Type"="Training",
        "Arguments"=list("input_data"=list("Get"="Parameters.MyStr"))
      )
    )
  ))
})

test_that("test_pipeline_two_step", {
  sms = sagemaker_session_mock()
  parameter = ParameterString$new("MyStr")
  step1 = CustomStep$new(
    name="MyStep1",
    input_data=list(
      parameter,  # parameter reference
      ExecutionVariables$PIPELINE_EXECUTION_ID,  # execution variable
      PipelineExperimentConfigProperties$EXPERIMENT_NAME  # experiment config property
    )
  )
  step2 = CustomStep$new(name="MyStep2", input_data=list(step1$properties$S3Uri))  # step property
  pipeline = Pipeline$new(
    name="MyPipeline",
    parameters=list(parameter),
    steps=list(step1, step2),
    sagemaker_session=sms
  )
  expect_equal(pipeline$to_request(), list(
    "Version"="2020-12-01",
    "Metadata"=list(),
    "Parameters"=list(list("Name"="MyStr", "Type"="String")),
    "PipelineExperimentConfig"=list(
      "ExperimentName"=ExecutionVariables$PIPELINE_NAME,
      "TrialName"=ExecutionVariables$PIPELINE_EXECUTION_ID
    ),
    "Steps"=list(
      list(
        "Name"="MyStep1",
        "Type"="Training",
        "Arguments"=list(
          "input_data"=list(
            parameter,
            ExecutionVariables$PIPELINE_EXECUTION_ID,
            PipelineExperimentConfigProperties$EXPERIMENT_NAME
          )
        )
      ),
      list(
        "Name"="MyStep2",
        "Type"="Training",
        "Arguments"=list("input_data"=list(step1$properties$S3Uri))
      )
    )
  ))
  expect_equal(jsonlite::parse_json(pipeline$definition()), list(
    "Version"="2020-12-01",
    "Metadata"=list(),
    "Parameters"=list(list("Name"="MyStr", "Type"="String")),
    "PipelineExperimentConfig"=list(
      "ExperimentName"=list("Get"="Execution.PipelineName"),
      "TrialName"=list("Get"="Execution.PipelineExecutionId")
    ),
    "Steps"=list(
      list(
        "Name"="MyStep1",
        "Type"="Training",
        "Arguments"=list(
          "input_data"=list(
            list("Get"="Parameters.MyStr"),
            list("Get"="Execution.PipelineExecutionId"),
            list("Get"="PipelineExperimentConfig.ExperimentName")
          )
        )
      ),
      list(
        "Name"="MyStep2",
        "Type"="Training",
        "Arguments"=list("input_data"=list(list("Get"="Steps.MyStep1.S3Uri")))
      )
    )
  ))
})

test_that("test_pipeline_override_experiment_config", {
  sms = sagemaker_session_mock()
  pipeline = Pipeline$new(
    name="MyPipeline",
    pipeline_experiment_config=PipelineExperimentConfig$new("MyExperiment", "MyTrial"),
    steps=list(CustomStep$new(name="MyStep", input_data="input")),
    sagemaker_session=sms
  )
  expect_equal(jsonlite::parse_json(pipeline$definition()), list(
    "Version"="2020-12-01",
    "Metadata"=list(),
    "Parameters"=list(),
    "PipelineExperimentConfig"=list("ExperimentName"="MyExperiment", "TrialName"="MyTrial"),
    "Steps"=list(
      list(
        "Name"="MyStep",
        "Type"="Training",
        "Arguments"=list("input_data"="input")
      )
    )
  ))
})

test_that("test_pipeline_disable_experiment_config", {
  sms = sagemaker_session_mock()
  pipeline = Pipeline$new(
    name="MyPipeline",
    pipeline_experiment_config=NULL,
    steps=list(CustomStep$new(name="MyStep", input_data="input")),
    sagemaker_session=sms
  )
  expect_equal(jsonlite::parse_json(pipeline$definition()), list(
    "Version"="2020-12-01",
    "Metadata"=list(),
    "Parameters"=list(),
    "Steps"=list(
      list(
        "Name"="MyStep",
        "Type"="Training",
        "Arguments"=list("input_data"="input")
      )
    )
  ))
})

test_that("test_pipeline_execution_basics", {
  sms = sagemaker_session_mock()
  sms$sagemaker$.call_args("start_pipeline_execution", return_value = list(
    "PipelineExecutionArn"="my:arn"
  ))
  sms$sagemaker$.call_args("list_pipeline_execution_steps", return_value = list(
    "PipelineExecutionSteps"=list(Mock$new())
  ))
  pipeline = Pipeline$new(
    name="MyPipeline",
    parameters=list(ParameterString$new("alpha", "beta"), ParameterString$new("gamma", "delta")),
    steps=list(),
    sagemaker_session=sms
  )
  execution = pipeline$start()
  execution$stop()
  expect_equal(sms$sagemaker$stop_pipeline_execution(..return_value = T), list(
    PipelineExecutionArn="my:arn"
  ))
  execution$describe()
  expect_equal(sms$sagemaker$describe_pipeline_execution(..return_value = T), list(
    PipelineExecutionArn="my:arn"
  ))
  steps = execution$list_steps()
  expect_equal(sms$sagemaker$list_pipeline_execution_steps(..return_value = T), list(
    PipelineExecutionArn="my:arn"
  ))
  expect_equal(length(steps), 1)
})
