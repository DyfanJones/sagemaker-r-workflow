# NOTE: This code has been modified from AWS Sagemaker Python:
# https://github.com/aws/sagemaker-python-sdk/blob/master/tests/unit/sagemaker/workflow/test_lambda_step.py

zip_dir = file.path(getwd(), "data", "dummy.zip")

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
    s3=NULL,
    account_id = mock_fun()
  )

  sagemaker = Mock$new()
  lambda_client = Mock$new()
  lambda_client$.call_args("create_function")
  s3_client = Mock$new()
  s3_client$.call_args("put_object")

  sms$.call_args("default_bucket", return_value="dummy")
  sms$.__enclos_env__$private$.default_bucket = "dummy"

  sms$sagemaker = sagemaker
  sms$lambda_client = lambda_client
  sms$s3 = s3_client
  return(sms)
}

sagemaker_session_cn = function(region="cn-north-1"){
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
    s3=NULL,
    account_id = mock_fun("234567890123")
  )

  sagemaker = Mock$new()
  lambda_client = Mock$new()
  lambda_client$.call_args("create_function")
  s3_client = Mock$new()
  s3_client$.call_args("put_object")

  sms$.call_args("default_bucket", return_value="dummy")
  sms$.__enclos_env__$private$.default_bucket = "dummy"

  sms$sagemaker = sagemaker
  sms$lambda_client = lambda_client
  sms$s3 = s3_client
  return(sms)
}

test_that("test_lambda_step", {
  sms = sagemaker_session()

  param = ParameterInteger$new(name="MyInt")
  output_param1 = LambdaOutput$new(output_name="output1", output_type=LambdaOutputTypeEnum$String)
  output_param2 = LambdaOutput$new(output_name="output2", output_type=LambdaOutputTypeEnum$Boolean)
  cache_config = CacheConfig$new(enable_caching=TRUE, expire_after="PT1H")

  lambda_step = LambdaStep$new(
    name="MyLambdaStep",
    depends_on=list("TestStep"),
    lambda_func=Lambda$new(
      function_arn="arn:aws:lambda:us-west-2:123456789012:function:sagemaker_test_lambda",
      session=sms
    ),
    display_name="MyLambdaStep",
    description="MyLambdaStepDescription",
    inputs=list("arg1"="foo", "arg2"=5, "arg3"=param),
    outputs=list(output_param1, output_param2),
    cache_config=cache_config
  )

  lambda_step$add_depends_on(list("SecondTestStep"))
  pipeline = Pipeline$new(
    name="MyPipeline",
    parameters=list(param),
    steps=list(lambda_step),
    sagemaker_session=sms
  )

  expect_equal(jsonlite::parse_json(pipeline$definition())$Steps[[1]], list(
    "Name"="MyLambdaStep",
    "Type"="Lambda",
    "Arguments"=list("arg1"="foo", "arg2"=5, "arg3"=list("Get"="Parameters.MyInt")),
    "DependsOn"=list("TestStep", "SecondTestStep"),
    "DisplayName"="MyLambdaStep",
    "Description"="MyLambdaStepDescription",
    "CacheConfig"=list("Enabled"=TRUE, "ExpireAfter"="PT1H"),
    "FunctionArn"="arn:aws:lambda:us-west-2:123456789012:function:sagemaker_test_lambda",
    "OutputParameters"=list(
      list("OutputName"="output1", "OutputType"="String"),
      list("OutputName"="output2", "OutputType"="Boolean")
    )
  ))
})

test_that("test_lambda_step_output_expr", {
  sms = sagemaker_session()
  param = ParameterInteger$new(name="MyInt")
  outputParam1 = LambdaOutput$new(output_name="output1", output_type=LambdaOutputTypeEnum$String)
  outputParam2 = LambdaOutput$new(output_name="output2", output_type=LambdaOutputTypeEnum$Boolean)
  lambda_step = LambdaStep$new(
    name="MyLambdaStep",
    depends_on=list("TestStep"),
    lambda_func=Lambda$new(
      function_arn="arn:aws:lambda:us-west-2:123456789012:function:sagemaker_test_lambda",
      session=sms
    ),
    inputs=list("arg1"="foo", "arg2"=5, "arg3"=param),
    outputs=list(outputParam1, outputParam2)
  )
  expect_equal(lambda_step$properties$Outputs$output1$expr, list(
    "Get"="Steps.MyLambdaStep.OutputParameters['output1']"
  ))
  expect_equal(lambda_step$properties$Outputs$output2$expr, list(
    "Get"="Steps.MyLambdaStep.OutputParameters['output2']"
  ))
})

test_that("test_pipeline_interpolates_lambda_outputs", {
  sms = sagemaker_session()
  parameter = ParameterString$new("MyStr")
  output_param1 = LambdaOutput$new(output_name="output1", output_type=LambdaOutputTypeEnum$String)
  output_param2 = LambdaOutput$new(output_name="output2", output_type=LambdaOutputTypeEnum$String)
  lambda_step1 = LambdaStep$new(
    name="MyLambdaStep1",
    depends_on=list("TestStep"),
    lambda_func=Lambda$new(
      function_arn="arn:aws:lambda:us-west-2:123456789012:function:sagemaker_test_lambda",
      session=sms
    ),
    inputs=list("arg1"="foo"),
    outputs=list(output_param1)
  )
  lambda_step2 = LambdaStep$new(
    name="MyLambdaStep2",
    depends_on=list("TestStep"),
    lambda_func=Lambda$new(
      function_arn="arn:aws:lambda:us-west-2:123456789012:function:sagemaker_test_lambda",
      session=sms
    ),
    inputs=list("arg1"=output_param1),
    outputs=list(output_param2)
  )
  pipeline = Pipeline$new(
    name="MyPipeline",
    parameters=list(parameter),
    steps=list(lambda_step1, lambda_step2),
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
        "Name"="MyLambdaStep1",
        "Type"="Lambda",
        "Arguments"=list("arg1"="foo"),
        "DependsOn"=list("TestStep"),
        "FunctionArn"="arn:aws:lambda:us-west-2:123456789012:function:sagemaker_test_lambda",
        "OutputParameters"=list(list("OutputName"="output1", "OutputType"="String"))
      ),
      list(
        "Name"="MyLambdaStep2",
        "Type"="Lambda",
        "Arguments"=list("arg1"=list("Get"="Steps.MyLambdaStep1.OutputParameters['output1']")),
        "DependsOn"=list("TestStep"),
        "FunctionArn"="arn:aws:lambda:us-west-2:123456789012:function:sagemaker_test_lambda",
        "OutputParameters"=list(list("OutputName"="output2", "OutputType"="String"))
      )
    )
  ))
})

test_that("test_lambda_step_no_inputs_outputs", {
  sms = sagemaker_session()
  lambda_step = LambdaStep$new(
    name="MyLambdaStep",
    depends_on=list("TestStep"),
    lambda_func=Lambda$new(
      function_arn="arn:aws:lambda:us-west-2:123456789012:function:sagemaker_test_lambda",
      session=sms
    ),
    inputs=list(),
    outputs=list()
  )
  lambda_step$add_depends_on(list("SecondTestStep"))
  expect_equal(lambda_step$to_request(), list(
    "Name"="MyLambdaStep",
    "Type"="Lambda",
    "Arguments"=list(),
    "DependsOn"=list("TestStep", "SecondTestStep"),
    "FunctionArn"="arn:aws:lambda:us-west-2:123456789012:function:sagemaker_test_lambda",
    "OutputParameters"=list()
  ))
})

test_that("test_lambda_step_with_function_arn", {
  sms = sagemaker_session()
  lambda_step = LambdaStep$new(
    name="MyLambdaStep",
    depends_on=list("TestStep"),
    lambda_func=Lambda$new(
      function_arn="arn:aws:lambda:us-west-2:123456789012:function:sagemaker_test_lambda",
      session=sms
    ),
    inputs=list(),
    outputs=list()
  )
  lambda_step$.__enclos_env__$private$.get_function_arn()
  expect_equal(sms$account_id(..count = T), 0)
})

test_that("test_lambda_step_without_function_arn", {
  sms = sagemaker_session()
  lambda_step = LambdaStep$new(
    name="MyLambdaStep",
    depends_on=list("TestStep"),
    lambda_func=Lambda$new(
      function_name="name",
      execution_role_arn="arn:aws:lambda:us-west-2:123456789012:execution_role",
      zipped_code_dir=zip_dir,
      handler="",
      session=sms
    ),
    inputs=list(),
    outputs=list()
  )
  lambda_step$.__enclos_env__$private$.get_function_arn()
  expect_equal(sms$account_id(..count = T), 1)
})

test_that("test_lambda_step_without_function_arn_and_with_error", {
  sms = sagemaker_session_cn()
  lambda_func = Mock$new(
    name="Lambda",
    function_arn=NULL,
    function_name="name",
    execution_role_arn="arn:aws:lambda:us-west-2:123456789012:execution_role",
    zipped_code_dir=zip_dir,
    handler="",
    session=sms
  )
  # The raised ValueError contains ResourceConflictException
  lambda_func$.call_args("create", side_effect=function(...) ValueError$new("ResourceConflictException"))
  lambda_step1 = LambdaStep$new(
    name="MyLambdaStep1",
    depends_on=list("TestStep"),
    lambda_func=lambda_func,
    inputs=list(),
    outputs=list()
  )
  function_arn = lambda_step1$.__enclos_env__$private$.get_function_arn()
  expect_equal(function_arn, "arn:aws-cn:lambda:cn-north-1:234567890123:function:name")

  lambda_func$.call_args("create", side_effect = function(...) ValueError$new())
  lambda_step2 = LambdaStep$new(
    name="MyLambdaStep2",
    depends_on=list("TestStep"),
    lambda_func=lambda_func,
    inputs=list(),
    outputs=list()
  )
  expect_error(
    lambda_step2$.__enclos_env__$private$.get_function_arn(),
    class = "ValueError"
  )
})
