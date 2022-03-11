# NOTE: This code has been modified from AWS Sagemaker Python:
# https://github.com/aws/sagemaker-python-sdk/blob/dev/tests/unit/sagemaker/workflow/test_condition_step.py

library(sagemaker.core)
library(sagemaker.common)
library(sagemaker.mlcore)
library(sagemaker.mlframework)


CustomStep=R6::R6Class("CustomStep",
  inherit = Step,
  public = list(

    initialize = function(name,
                          display_name=NULL,
                          description=NULL){
      super$initialize(name, display_name, description, StepTypeEnum$TRAINING)
      private$.properties = Properties$new(path=sprintf("Steps.%s", name))
    }
  ),
  active = list(
    arguments = function() {
      list()
    },

    properties = function(){
      private$.properties
    }
  )
)

test_that("test_condition_step", {
  param = ParameterInteger$new(name="MyInt")
  cond = ConditionEquals$new(left=param, right=1)
  step1 = CustomStep$new(name="MyStep1")
  step2 = CustomStep$new(name="MyStep2")
  cond_step = ConditionStep$new(
    name="MyConditionStep",
    depends_on=list("TestStep"),
    conditions=list(cond),
    if_steps=list(step1),
    else_steps=list(step2)
  )
  cond_step$add_depends_on(list("SecondTestStep"))

  expect_equal(cond_step$to_request(), list(
    "Name"="MyConditionStep",
    "Type"="Condition",
    "Arguments"=list(
      "Conditions"=list(
        list(
          "Type"="Equals",
          "LeftValue"=list("Get"="Parameters.MyInt"),
          "RightValue"=1
        )
      ),
      "IfSteps"=list(
        list(
          "Name"="MyStep1",
          "Type"="Training",
          "Arguments"=list()
        )
      ),
      "ElseSteps"=list(
        list(
          "Name"="MyStep2",
          "Type"="Training",
          "Arguments"=list()
          )
        )
      ),
    "DependsOn"=list("TestStep", "SecondTestStep")
    )
  )
  expect_equal(
    cond_step$properties$Outcome$expr,
    list("Get"="Steps.MyConditionStep.Outcome")
  )
})
