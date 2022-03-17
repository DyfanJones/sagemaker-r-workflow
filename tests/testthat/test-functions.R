# NOTE: This code has been modified from AWS Sagemaker Python:
# https://github.com/aws/sagemaker-python-sdk/blob/master/tests/unit/sagemaker/workflow/test_functions.py


test_that("test_join_primitives_default_on", {
  expect_equal(Join$new(values=list(1, "a", FALSE, 1.1))$expr, list(
    "Std:Join"=list(
      "On"="",
      "Values"=list(1, "a", FALSE, 1.1)
    )
  ))
})

test_that("test_join_primitives",{
  expect_equal(Join$new(on=",", values=list(1, "a", FALSE, 1.1))$expr, list(
    "Std:Join"=list(
      "On"=",",
      "Values"=list(1, "a", FALSE, 1.1)
    )
  ))
})

test_that("test_join_expressions", {
  expect_equal(Join$new(
    values=list(
      "foo",
      ParameterFloat$new(name="MyFloat"),
      ParameterInteger$new(name="MyInt"),
      ParameterString$new(name="MyStr"),
      Properties$new(path="Steps.foo.OutputPath.S3Uri"),
      ExecutionVariables$PIPELINE_EXECUTION_ID,
      Join$new(on=",", values=list(1, "a", FALSE, 1.1))
    )
  )$expr, list(
    "Std:Join"=list(
      "On"="",
      "Values"=list(
        "foo",
        list("Get"="Parameters.MyFloat"),
        list("Get"="Parameters.MyInt"),
        list("Get"="Parameters.MyStr"),
        list("Get"="Steps.foo.OutputPath.S3Uri"),
        list("Get"="Execution.PipelineExecutionId"),
        list("Std:Join"=list("On"=",", "Values"=list(1, "a", FALSE, 1.1)))
      )
    )
  ))
})

test_that("test_json_get_expressions",{
  expect_equal(JsonGet$new(
    step_name="my-step",
    property_file="my-property-file",
    json_path="my-json-path"
  )$expr, list(
    "Std:JsonGet"=list(
      "PropertyFile"=list("Get"="Steps.my-step.PropertyFiles.my-property-file"),
      "Path"="my-json-path"
    )
  ))

  property_file = PropertyFile$new(
    name="name",
    output_name="result",
    path="output"
  )

  expect_equal(JsonGet$new(
    step_name="my-step",
    property_file=property_file,
    json_path="my-json-path"
  )$expr, list(
    "Std:JsonGet"=list(
      "PropertyFile"=list("Get"="Steps.my-step.PropertyFiles.name"),
      "Path"="my-json-path"
    )
  ))
})

test_that("test_json_get_expressions_with_invalid_step_name", {
  expect_error(
    JsonGet$new(
      step_name="",
      property_file="my-property-file",
      json_path="my-json-path"
    )$expr,
    class = "ValueError",
    "Please give a valid step name as a string"
  )
  expect_error(
    JsonGet$new(
      step_name=ParameterString$new(name="MyString"),
      property_file="my-property-file",
      json_path="my-json-path"
    )$expr,
    class = "ValueError",
    "Please give a valid step name as a string"
  )
})
