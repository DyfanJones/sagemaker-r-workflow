# NOTE: This code has been modified from AWS Sagemaker Python:
# https://github.com/aws/sagemaker-python-sdk/blob/dev/tests/unit/sagemaker/workflow/test_callback_step.py

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

test_that("test_callback_step", {
  param = ParameterInteger$new(name = "MyInt")
  outputParam1 = CallbackOutput$new(output_name="output1", output_type=CallbackOutputTypeEnum$String)
  outputParam2 = CallbackOutput$new(output_name="output2", output_type=CallbackOutputTypeEnum$Boolean)
  cb_step = CallbackStep$new(
    name="MyCallbackStep",
    depends_on=list("TestStep"),
    sqs_queue_url="https://sqs.us-east-2.amazonaws.com/123456789012/MyQueue",
    inputs=list("arg1"="foo", "arg2"=5, "arg3"=param),
    outputs=list(outputParam1, outputParam2)
  )
  cb_step$add_depends_on(list("SecondTestStep"))
  expect_equal(cb_step$to_request(), list(
    "Name"="MyCallbackStep",
    "Type"="Callback",
    "Arguments"=list("arg1"="foo", "arg2"=5, "arg3"=param),
    "DependsOn"=list("TestStep", "SecondTestStep"),
    "SqsQueueUrl"="https://sqs.us-east-2.amazonaws.com/123456789012/MyQueue",
    "OutputParameters"=list(
      list("OutputName"="output1", "OutputType"="String"),
      list("OutputName"="output2", "OutputType"="Boolean")
    )
  ))
})

