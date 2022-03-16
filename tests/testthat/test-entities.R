# NOTE: This code has been modified from AWS Sagemaker Python:
# https://github.com/aws/sagemaker-python-sdk/blob/master/tests/unit/sagemaker/workflow/test_entities.py

library(R6)
library(sagemaker.core)
library(sagemaker.common)
library(sagemaker.mlcore)
library(sagemaker.mlframework)

CustomEntity = R6Class("CustomEntity",
  inherit = Entity,
  public = list(
    initialize = function(foo){
      self$foo=foo
    },

    to_request = function(){
      return(list("foo"=self$foo))
    }
  ),
  lock_objects = F
)

custom_entity = function(){CustomEntity$new(1)}

custom_entity_list = function(){
  list(CustomEntity$new(1), CustomEntity$new(2))
}

test_that("test_entity", {
  request_struct = list("foo"=1)
  expect_equal(custom_entity()$to_request(), request_struct)
})
