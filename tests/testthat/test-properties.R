# NOTE: This code has been modified from AWS Sagemaker Python:
# https://github.com/aws/sagemaker-python-sdk/blob/master/tests/unit/sagemaker/workflow/test_properties.py

library(sagemaker.core)
library(sagemaker.common)
library(sagemaker.mlcore)
library(sagemaker.mlframework)

test_that("test_properties_describe_training_job_response", {
  prop = Properties$new("Steps.MyStep", "DescribeTrainingJobResponse")
  some_prop_names = list("TrainingJobName", "TrainingJobArn", "HyperParameters", "OutputDataConfig")
  expect_true(all(some_prop_names %in% names(prop)))
  expect_equal(prop$CreationTime$expr, list("Get"="Steps.MyStep.CreationTime"))
  expect_equal(prop$HyperParameters$expr, list("Get"="Steps.MyStep.HyperParameters"))
  expect_equal(prop$OutputDataConfig$S3OutputPath$expr, list(
    "Get"="Steps.MyStep.OutputDataConfig.S3OutputPath"
  ))
})

test_that("test_properties_describe_processing_job_response", {
  prop = Properties$new("Steps.MyStep", "DescribeProcessingJobResponse")
  some_prop_names = list("ProcessingInputs", "ProcessingOutputConfig", "ProcessingEndTime")
  expect_true(all(some_prop_names %in% names(prop)))
  expect_equal(prop$ProcessingJobName$expr, list("Get"="Steps.MyStep.ProcessingJobName"))
  expect_equal(prop$ProcessingOutputConfig$Outputs$get_item("MyOutputName")$S3Output$S3Uri$expr, list(
    "Get"="Steps.MyStep.ProcessingOutputConfig.Outputs['MyOutputName'].S3Output.S3Uri"
  ))
})

test_that("test_properties_tuning_job", {
  prop = Properties$new(
    "Steps.MyStep",
    shape_names=c(
      "DescribeHyperParameterTuningJobResponse",
      "ListTrainingJobsForHyperParameterTuningJobResponse"
    )
  )
  some_prop_names = c(
    "BestTrainingJob",
    "HyperParameterTuningJobConfig",
    "ObjectiveStatusCounters",
    "TrainingJobSummaries"
  )
  expect_true(all(some_prop_names %in% names(prop)))
  expect_equal(prop$HyperParameterTuningJobName$expr, list(
    "Get"="Steps.MyStep.HyperParameterTuningJobName"
  ))
  expect_equal(prop$HyperParameterTuningJobConfig$HyperParameterTuningJobObjective$Type$expr, list(
    "Get"="Steps.MyStep.HyperParameterTuningJobConfig.HyperParameterTuningJobObjective.Type"
  ))
  expect_equal(prop$ObjectiveStatusCounters$Succeeded$expr, list(
    "Get"="Steps.MyStep.ObjectiveStatusCounters.Succeeded"
  ))
  expect_equal(prop$TrainingJobSummaries$get_item(1)$TrainingJobName$expr, list(
    "Get"="Steps.MyStep.TrainingJobSummaries[0].TrainingJobName"
  ))
})

test_that("test_properties_emr_step", {
  prop = Properties$new("Steps.MyStep", "Step", service_name="emr")
  some_prop_names = list("Id", "Name", "Config", "ActionOnFailure", "Status")
  expect_true(all(some_prop_names %in% names(prop)))
  expect_equal(prop$Id$expr, list("Get"="Steps.MyStep.Id"))
  expect_equal(prop$Name$expr, list("Get"="Steps.MyStep.Name"))
  expect_equal(prop$ActionOnFailure$expr, list("Get"="Steps.MyStep.ActionOnFailure"))
  expect_equal(prop$Config$Jar$expr, list("Get"="Steps.MyStep.Config.Jar"))
  expect_equal(prop$Status$State$expr, list("Get"="Steps.MyStep.Status.State"))
})

test_that("test_properties_describe_model_package_output", {
  prop = Properties$new("Steps.MyStep", "DescribeModelPackageOutput")
  some_prop_names = c("ModelPackageName", "ModelPackageGroupName", "ModelPackageArn")
  expect_true(all(some_prop_names %in% names(prop)))
  expect_equal(prop$ModelPackageName$expr, list("Get"="Steps.MyStep.ModelPackageName"))
  expect_equal(prop$ValidationSpecification$ValidationRole$expr, list(
    "Get"="Steps.MyStep.ValidationSpecification.ValidationRole"
  ))
})

test_that("test_to_string", {
  prop = Properties$new("Steps.MyStep", "DescribeTrainingJobResponse")
  expect_equal(prop$CreationTime$to_string()$expr, list(
    "Std:Join"=list(
      "On"="",
      "Values"=list(list("Get"="Steps.MyStep.CreationTime"))
    )
  ))
})

test_that("test_string_builtin_funcs_that_return_bool", {
  prop = Properties$new("Steps.MyStep", "DescribeModelPackageOutput")
  # The prop will only be parsed in runtime (Pipeline backend) so not able to tell in SDK
  expect_false(prop$startswith("s3"))
  expect_false(prop$endswith("s3"))
})
