# NOTE: This code has been modified from AWS Sagemaker Python:
# https://github.com/aws/sagemaker-python-sdk/blob/dev/tests/unit/sagemaker/workflow/test_parameters.py


test_that("test_parameter", {
  param = ParameterBoolean$new("MyBool")
  expect_equal(param$to_request(), list("Name"="MyBool", "Type"="Boolean"))
  expect_equal(param$expr, list("Get"="Parameters.MyBool"))
  expect_equal(param$parameter_type$python_type, "logical")
})

test_that("test_parameter_with_default", {
  param = ParameterFloat$new(name="MyFloat", default_value=1.2)
  expect_equal(param$to_request(), list("Name"="MyFloat", "Type"="Float", "DefaultValue"=1.2))
})

test_that("test_parameter_with_default", {
  param = ParameterInteger$new(name="MyInteger", default_value=0L)
  expect_equal(param$to_request(), list("Name"="MyInteger", "Type"="Integer", "DefaultValue"=0L))
})

test_that("test_parameter_string_with_enum_values", {
  param = ParameterString$new("MyString", enum_values=list("a", "b"))
  expect_equal(param$to_request(), list("Name"="MyString", "Type"="String", "EnumValues"=list("a", "b")))
  param = ParameterString$new("MyString", default_value="a", enum_values=list("a", "b"))
  expect_equal(param$to_request(), list(
    "Name"="MyString",
    "Type"="String",
    "DefaultValue"="a",
    "EnumValues"=list("a", "b")
  ))
})

test_that("test_parameter_with_invalid_default", {
  expect_error(
    ParameterFloat$new(name="MyFloat", default_value="abc"),
    class = "TypeError"
  )
})

test_that("test_parameter_string_implicit_value", {
  param = ParameterString$new("MyString")
  expect_equal(param$str, "")
  param1 = ParameterString$new("MyString", "1")
  expect_equal(param1$default_value, "1")
  param2 = ParameterString$new("MyString", "2")
  expect_equal(param2$str, "2")
  param3 = ParameterString$new("MyString", "3")
  expect_equal(param3$str, "3")
  param3 = ParameterString$new(name="MyString", default_value="3", enum_values=list("3"))
  expect_equal(param3$str, "3")
})

test_that("test_parameter_integer_implicit_value", {
  param = ParameterInteger$new("MyInteger")
  expect_equal(param$int, 0L)
  param1 = ParameterInteger$new("MyInteger", 1L)
  expect_equal(param1$int, 1L)
  param2 = ParameterInteger$new("MyInteger", 2L)
  expect_equal(param2$int, 2L)
  param3 = ParameterInteger$new("MyInteger", 3L)
  expect_equal(param3$int, 3L)
})

test_that("test_parameter_float_implicit_value", {
  param = ParameterFloat$new("MyFloat")
  expect_equal(param$float, 0)
  param1 = ParameterFloat$new("MyFloat", 1.1)
  expect_equal(param1$float, 1.1)
  param2 = ParameterFloat$new("MyFloat", 2.1)
  expect_equal(param2$float, 2.1)
  param3 = ParameterFloat$new("MyFloat", 3.1)
  expect_equal(param3$float, 3.1)
})

test_that("test_parsable_parameter_string", {
  skip_if_not_installed("urltools")
  param = ParameterString$new("MyString", default_value="s3://foo/bar/baz.csv")
  expect_equal(httr::parse_url(param$str)$scheme, "s3")
})
