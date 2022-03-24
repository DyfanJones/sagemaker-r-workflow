# NOTE: This code has been modified from AWS Sagemaker Python:
# https://github.com/aws/sagemaker-python-sdk/blob/master/tests/unit/sagemaker/workflow/test_pipeline_experiment_config.py

test_that("test_pipeline_experiment_config", {
  config = PipelineExperimentConfig$new("experiment-name", "trial-name")
  expect_equal(config$to_request(), list(
    "ExperimentName"="experiment-name", "TrialName"="trial-name"
  ))
})

test_that("test_pipeline_experiment_config_property", {
  var = PipelineExperimentConfigProperties$EXPERIMENT_NAME
  expect_equal(var$expr, list(
    "Get"="PipelineExperimentConfig.ExperimentName"
  ))
})
