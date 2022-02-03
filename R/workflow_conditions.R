# NOTE: This code has been modified from AWS Sagemaker Python:
# https://github.com/aws/sagemaker-python-sdk/blob/master/src/sagemaker/workflow/conditions.py

#' @include workflow_entities.R
#' @include workflow_execution_variables.R
#' @include workflow_parameters.R
#' @include workflow_properties.R
#' @include r_utils.R

#' @import R6

#' @title Workflow CondiitionType class
#' @description Condition type enum.
#' @export
ConditionTypeEnum = Enum(
  EQ = "Equals",
  GT = "GreaterThan",
  GTE = "GreaterThanOrEqualTo",
  IN = "In",
  LT = "LessThan",
  LTE = "LessThanOrEqualTo",
  NOT = "Not",
  OR = "Or"
)

#' @title Workflow Condition class
#' @description Abstract Condition entity.
#' @export
Condition = R6Class("Condition",
  inherit = Entity,
  public = list(

    #' @field condition_type
    #' The type of condition.
    condition_type = NULL,

    #' @description Initialize Condition class
    #' @param condition_type (ConditionTypeEnum): The type of condition.
    initialize = function(condition_type = enum_items(ConditionTypeEnum)){
      self$condition_type = match.arg(condition_type)
    }
  )
)

#' @title Workflow ConditionComparison class
#' @description Generic comparison condition that can be used to derive specific
#'              condition comparisons.
#' @export
ConditionComparison = R6Class("ConditionComparison",
  inherit = Condition,
  public = list(

    #' @field left
    #' The execution variable, parameter, or property to use in the comparison.
    left = NULL,

    #' @field right
    #' The execution variable, parameter, property, or Python primitive value to compare to.
    right = NULL,

    #' @description Initialize ConditionComparison Class
    #' @param condition_type (ConditionTypeEnum): The type of condition.
    #' @param left (ConditionValueType): The execution variable, parameter, or
    #'              property to use in the comparison.
    #' @param right (Union[ConditionValueType, PrimitiveType]): The execution variable,
    #'              parameter, property, or Python primitive value to compare to.
    initialize = function(condition_type = enum_items(ConditionTypeEnum),
                          left,
                          right){
      super$initialize(condition_type)
      self$left = left
      self$right = right
    },

    #' @description Get the request structure for workflow service calls.
    to_request = function(){
      return(list(
        "Type"=self$condition_type,
        "LeftValue"=self$left$expr,
        "RightValue"=primitive_or_expr(self$right))
      )
    }
  )
)

#' @title Workflow ConditionEquals Class
#' @description A condition for equality comparisons.
#' @export
ConditionEquals = R6Class("ConditionEquals",
  inherit = ConditionComparison,
  public = list(

    #' @description Construct A condition for equality comparisons.
    #' @param left (ConditionValueType): The execution variable, parameter,
    #'              or property to use in the comparison.
    #' @param right (Union[ConditionValueType, PrimitiveType]): The execution
    #'              variable, parameter, property, or Python primitive value to compare to.
    initialize = function(left,
                          right){
      stopifnot(
        inherits(left, "ConditionValueType")
      )
      super$initialize(ConditionTypeEnum$EQ, left, right)
    }
  )
)

#' @title Workflow ConditionGreaterThan Class
#' @description A condition for greater than comparisons.
#' @export
ConditionGreaterThan = R6Class("ConditionGreaterThan",
  inherit = ConditionComparison,
  public = list(

    #' @description Construct an instance of ConditionGreaterThan for greater than comparisons.
    #' @param left (ConditionValueType): The execution variable, parameter,
    #'              or property to use in the comparison.
    #' @param right (Union[ConditionValueType, PrimitiveType]): The execution
    #'              variable, parameter, property, or Python primitive value to compare to.
    initialize = function(left,
                          right){
      stopifnot(
        inherits(left, "ConditionValueType")
      )
      super$initialize(ConditionTypeEnum$GT, left, right)

    }
  )
)

#' @title Workflow ConditionGreaterThanOrEqualTo class
#' @description A condition for greater than or equal to comparisons.
#' @export
ConditionGreaterThanOrEqualTo = R6Class("ConditionGreaterThanOrEqualTo",
  inherit = ConditionComparison,
  public = list(

    #' @description Construct of ConditionGreaterThanOrEqualTo for greater than or equal to comparisons.
    #' @param left (ConditionValueType): The execution variable, parameter,
    #'              or property to use in the comparison.
    #' @param right (Union[ConditionValueType, PrimitiveType]): The execution
    #'              variable, parameter, property, or Python primitive value to compare to.
    initialize = function(left,
                          right){

      super$initialize(ConditionTypeEnum$GTE, left, right)
    }
  )
)

#' @title Workflow ConditionLessThan class
#' @description A condition for less than or equal to comparisons.
#' @export
ConditionLessThan= R6Class("ConditionLessThan",
  inherit = ConditionComparison,
  public = list(

    #' @description Construct ConditionLessThanOrEqualTo for less than or equal to comparisons.
    #' @param left (ConditionValueType): The execution variable, parameter,
    #'              or property to use in the comparison.
    #' @param right (Union[ConditionValueType, PrimitiveType]): The execution
    #'              variable, parameter, property, or Python primitive value to compare to.
    initialize = function(left,
                          right){
      stopifnot(
        inherits(left, "ConditionValueType")
      )
      super$initialize(ConditionTypeEnum$LTE, left, right)
    }
  )
)

#' @title Workflow ConditionLessThanOrEqualTo class
#' @description A condition for less than or equal to comparisons.
#' @export
ConditionLessThanOrEqualTo = R6Class("ConditionLessThanOrEqualTo",
  inherit = ConditionComparison,
  public = list(

    #' @description Construct ConditionLessThanOrEqualTo for less than or equal to comparisons.
    #' @param left (ConditionValueType): The execution variable, parameter,
    #'              or property to use in the comparison.
    #' @param right (Union[ConditionValueType, PrimitiveType]): The execution
    #'              variable, parameter, property, or Python primitive value to compare to.
    initialize = function(left,
                          right){
      stopifnot(
        inherits(left, "ConditionValueType")
      )
      super$initialize(ConditionTypeEnum.LTE, left, right)
    }
  )
)

#' @title Workflow ConditionIn class
#' @description A condition to check membership.
#' @export
ConditionIn = R6Class("ConditionIn",
  inherit = Condition,
  public = list(

    #' @description Construct a `ConditionIn` condition to check membership.
    #' @param value (ConditionValueType): The execution variable,
    #'              parameter, or property to use for the in comparison.
    #' @param in_values (List[Union[ConditionValueType, PrimitiveType]]): The list
    #'              of values to check for membership in.
    initialize = function(value,
                          in_values){
      stopifnot(
        inherits(value, "ConditionValueType")
      )
      super$initialize(ConditionTypeEnum$IN)
      self$value = value
      self$in_values = in_values
    },

    #' @description Get the request structure for workflow service calls.
    to_request = function(){
      return(list(
        "Type"=self$condition_type,
        "QueryValue"=self$value$expr,
        "Values"=lapply(self$in_values, function(in_value) primitive_or_expr(in_value))
        )
      )
    }
  )
)

#' @title Workflow ConditionNot class
#' @description A condition for negating another `Condition`.
#' @export
ConditionNot = R6Class("ConditionNot",
  inherit = Condition,
  public = list(

    #' @description Construct a `ConditionNot` condition for negating another `Condition`.
    #' @param expression (Condition): A `Condition` to take the negation of.
    initialize = function(expression){
      stopifnot(
        inherits(expression, "Condition")
      )
      super$initialize(ConditionTypeEnum$NOT)
      self$expression = expression
    },

    #' @description Get the request structure for workflow service calls.
    to_request = function(){
      return(list(
        "Type"=self$condition_type,
        "Expression"=self$expression$to_request())
      )
    }
  )
)

#' @title Workflow ConditionOr class
#' @description A condition for taking the logical OR of a list of `Condition` instances.
#' @export
ConditionOr = R6Class("ConditionOr",
  inherit = Condition,
  public = list(

    #' @description Construct a `ConditionOr` condition.
    #' @param conditions (List[Condition]): A list of `Condition` instances to logically OR.
    initialize = function(conditions=NULL){
      stopifnot(
        is.list(condititons) || is.null(conditions)
      )
      super$initialize(ConditionTypeEnum$OR)
      self$conditions = conditions %||% list()
    },

    #' @description Get the request structure for workflow service calls.
    to_request = function(){
      return(list(
        "Type"=self$condition_type,
        "Conditions"=lapply(self$conditions, function(condition) condition$to_request())
        )
      )
    }
  )
)

#' @title Provide the expression of the value or return value if it is a primitive.
#' @param value (Union[ConditionValueType, PrimitiveType]): The value to evaluate.
#' @keywords internal
#' @return Either the expression of the value or the primitive value.
primitive_or_expr = function(value){
  if (inherits(value, c("ExecutionVariable", "Expression", "Parameter", "Properties")))
    return(value$expr)
  return(value)
}
