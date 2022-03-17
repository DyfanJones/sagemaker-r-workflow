# NOTE: This code has been modified from AWS Sagemaker Python:
# https://github.com/aws/sagemaker-python-sdk/blob/master/tests/unit/sagemaker/workflow/test_fail_step.py

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

test_that("test_execution_variable", {
  expect_equal(ExecutionVariables$START_DATETIME$expr, list("Get"="Execution.StartDateTime"))
})

test_that("test_fail_step", {
  fail_step = FailStep$new(
    name="MyFailStep",
    depends_on=list("TestStep"),
    error_message="Test error message"
  )
  fail_step$add_depends_on(list("SecondTestStep"))
  expect_equal(fail_step$to_request(), list(
    "Name"="MyFailStep",
    "Type"="Fail",
    "Arguments"=list("ErrorMessage"="Test error message"),
    "DependsOn"=list("TestStep", "SecondTestStep")
  ))
})

test_that("test_fail_step_with_no_error_message", {
  fail_step = FailStep$new(
    name="MyFailStep",
    depends_on=list("TestStep")
  )
  fail_step$add_depends_on(list("SecondTestStep"))
  expect_equal(fail_step$to_request(), list(
    "Name"="MyFailStep",
    "Type"="Fail",
    "Arguments"=list("ErrorMessage"=""),
    "DependsOn"=list("TestStep", "SecondTestStep")
  ))
})

test_that("test_fail_step_with_join_fn_in_error_message", {
  param = ParameterInteger$new(name="MyInt", default_value=2)
  cond = ConditionEquals$new(left=param, right=1)
  step_cond = ConditionStep$new(
    name="CondStep",
    conditions=list(cond),
    if_steps=list(),
    else_steps=list()
  )
  step_fail = FailStep$new(
    name="FailStep",
    error_message=Join$new(
      on=": ", values=list("Failed due to xxx == yyy returns", step_cond$properties$Outcome)
    )
  )
  pipeline = Pipeline$new(
    name="MyPipeline",
    steps=list(step_cond, step_fail),
    parameters=list(param),
    sagemaker_session = sagemaker_session()
  )
  .expected_dsl = list(
    list(
      "Name"="CondStep",
      "Type"="Condition",
      "Arguments"=list(
        "Conditions"=list(
          list("Type"="Equals", "LeftValue"=list("Get"="Parameters.MyInt"), "RightValue"=1)
        ),
        "IfSteps"=list(),
        "ElseSteps"=list()
      )
    ),
    list(
      "Name"="FailStep",
      "Type"="Fail",
      "Arguments"=list(
        "ErrorMessage"=list(
          "Std:Join"=list(
            "On"=": ",
            "Values"=list(
              "Failed due to xxx == yyy returns",
              list("Get"="Steps.CondStep.Outcome")
            )
          )
        )
      )
    )
  )

  expect_equal(jsonlite::parse_json(pipeline$definition())$Steps, .expected_dsl)
})

test_that("test_fail_step_with_properties_ref", {
  fail_step = FailStep$new(
    name="MyFailStep",
    error_message="Test error message"
  )
  expect_error(
    fail_step$properties(),
    class = "RuntimeError",
    "FailStep is a terminal step and the Properties object is not available for it."
  )
})
