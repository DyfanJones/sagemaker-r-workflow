# NOTE: This code has been modified from AWS Sagemaker Python:
# https://github.com/aws/sagemaker-python-sdk/blob/master/tests/unit/sagemaker/workflow/test_execution_variables.py

library(sagemaker.core)
library(sagemaker.common)
library(sagemaker.mlcore)
library(sagemaker.mlframework)

test_that("test_execution_variable", {
  expect_equal(ExecutionVariables$START_DATETIME$expr, list("Get"="Execution.StartDateTime"))
})
