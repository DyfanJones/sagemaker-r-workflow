# NOTE=This code has been modified from AWS Sagemaker Python:
# https://github.com/aws/sagemaker-python-sdk/blob/dev/tests/unit/sagemaker/workflow/test_emr_step.py

library(sagemaker.core)
library(sagemaker.common)
library(sagemaker.mlcore)
library(sagemaker.mlframework)

sagemaker_session = function(region="us-west-2"){
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

  sms$.call_args("default_bucket", return_value="dummy")
  sms$.__enclos_env__$private$.default_bucket = "dummy"

  sms$sagemaker = sagemaker
  sms$s3 = s3_client
  return(sms)
}

test_that("test_emr_step_with_one_step_config", {
  emr_step_config = EMRStepConfig$new(
    jar="s3:/script-runner/script-runner.jar",
    args=list("--arg_0", "arg_0_value"),
    main_class="com.my.main",
    properties=list(list("Key"="Foo", "Value"="Foo_value"), list("Key"="Bar", "Value"="Bar_value"))
  )

  emr_step = EMRStep$new(
    name="MyEMRStep",
    display_name="MyEMRStep",
    description="MyEMRStepDescription",
    cluster_id="MyClusterID",
    step_config=emr_step_config,
    depends_on=list("TestStep"),
    cache_config=CacheConfig$new(enable_caching=TRUE, expire_after="PT1H")
  )
  emr_step$add_depends_on(list("SecondTestStep"))
  expect_equal(emr_step$to_request(), list(
    "Name"="MyEMRStep",
    "Type"="EMR",
    "Arguments"=list(
      "ClusterId"="MyClusterID",
      "StepConfig"=list(
        "HadoopJarStep"=list(
          "Jar"="s3:/script-runner/script-runner.jar",
          "Args"=list("--arg_0", "arg_0_value"),
          "MainClass"="com.my.main",
          "Properties"=list(
            list("Key"="Foo", "Value"="Foo_value"),
            list("Key"="Bar", "Value"="Bar_value")
          )
        )
      )
    ),
    "DependsOn"=list("TestStep", "SecondTestStep"),
    "DisplayName"="MyEMRStep",
    "Description"="MyEMRStepDescription",
    "CacheConfig"=list("Enabled"=TRUE, "ExpireAfter"="PT1H")
  ))

  expect_equal(emr_step$properties$ClusterId, "MyClusterID")
  expect_equal(emr_step$properties$ActionOnFailure$expr, list("Get"="Steps.MyEMRStep.ActionOnFailure"))
  expect_equal(emr_step$properties$Config$Args$expr, list("Get"="Steps.MyEMRStep.Config.Args"))
  expect_equal(emr_step$properties$Config$Jar$expr, list("Get"="Steps.MyEMRStep.Config.Jar"))
  expect_equal(emr_step$properties$Config$MainClass$expr, list("Get"="Steps.MyEMRStep.Config.MainClass"))
  expect_equal(emr_step$properties$Id$expr, list("Get"="Steps.MyEMRStep.Id"))
  expect_equal(emr_step$properties$Name$expr, list("Get"="Steps.MyEMRStep.Name"))
  expect_equal(emr_step$properties$Status$State$expr, list("Get"="Steps.MyEMRStep.Status.State"))
  expect_equal(emr_step$properties$Status$FailureDetails$Reason$expr,
    list("Get"="Steps.MyEMRStep.Status.FailureDetails.Reason")
  )
})

test_that("test_pipeline_interpolates_emr_outputs", {
  sms = sagemaker_session()
  parameter = ParameterString$new("MyStr")

  emr_step_config_1 = EMRStepConfig$new(
    jar="s3:/script-runner/script-runner_1.jar",
    args=list("--arg_0", "arg_0_value"),
    main_class="com.my.main",
    properties=list(list("Key"="Foo", "Value"="Foo_value"), list("Key"="Bar", "Value"="Bar_value"))
  )

  step_emr_1 = EMRStep$new(
    name="emr_step_1",
    cluster_id="MyClusterID",
    display_name="emr_step_1",
    description="MyEMRStepDescription",
    depends_on=list("TestStep"),
    step_config=emr_step_config_1
  )

  emr_step_config_2 = EMRStepConfig$new(jar="s3:/script-runner/script-runner_2.jar")

  step_emr_2 = EMRStep$new(
    name="emr_step_2",
    cluster_id="MyClusterID",
    display_name="emr_step_2",
    description="MyEMRStepDescription",
    depends_on=list("TestStep"),
    step_config=emr_step_config_2
  )

  pipeline = Pipeline$new(
    name="MyPipeline",
    parameters=list(parameter),
    steps=list(step_emr_1, step_emr_2),
    sagemaker_session=sms
  )

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
        "Name"="emr_step_1",
        "Type"="EMR",
        "Arguments"=list(
          "ClusterId"="MyClusterID",
          "StepConfig"=list(
            "HadoopJarStep"=list(
              "Jar"="s3:/script-runner/script-runner_1.jar",
              "Args"=list("--arg_0", "arg_0_value"),
              "MainClass"="com.my.main",
              "Properties"=list(
                list("Key"="Foo", "Value"="Foo_value"),
                list("Key"="Bar", "Value"="Bar_value")
              )
            )
          )
        ),
        "DependsOn"=list("TestStep"),
        "DisplayName"="emr_step_1",
        "Description"="MyEMRStepDescription"
      ),
      list(
        "Name"="emr_step_2",
        "Type"="EMR",
        "Arguments"=list(
          "ClusterId"="MyClusterID",
          "StepConfig"=list(
            "HadoopJarStep"=list("Jar"="s3:/script-runner/script-runner_2.jar")
          )
        ),
        "DependsOn"=list("TestStep"),
        "DisplayName"="emr_step_2",
        "Description"="MyEMRStepDescription"
      )
    )
  ))
})
