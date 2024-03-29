# NOTE: This code has been modified from AWS Sagemaker Python:
# https://github.com/aws/sagemaker-python-sdk/blob/master/src/sagemaker/workflow/parameters.py

#' @include workflow_entities.R

#' @import R6
#' @import sagemaker.core

# Parameter type enum.
ParameterTypeEnum = R6Class("ParameterTypeEnum",
  public = list(

    value = NULL,

    initialize = function(value = c("STRING","INTEGER", "BOOLEAN", "FLOAT")){
      self$value = private[[match.arg(value)]]
    }
  ),
  active = list(
    python_type = function(){
      mapping = list(
        String = "character",
        Integer = "integer",
        Boolean = "logical",
        Float = "numeric"
      )
      return(mapping[[self$value]])
    }
  ),
  private = list(
    STRING="String",
    INTEGER="Integer",
    BOOLEAN="Boolean",
    FLOAT="Float"
  )
)

#' @title Workflow Parameter Class
#' @description Pipeline parameter for workflow.
#' @export
Parameter = R6Class("Parameter",
  inherit = Entity,
  public = list(

    #' @field name
    #' The name of the parameter.
    name = NULL,

    #' @field parameter_type
    #' The type of the parameter
    parameter_type = NULL,

    #' @field default_value
    #' The default python value of the parameter
    default_value = NULL,

    #' @description Initialize Parameter class
    #' @param name (str): The name of the parameter.
    #' @param parameter_type (ParameterTypeEnum): The type of the parameter.
    #' @param default_value (PrimitiveType): The default Python value of the parameter.
    initialize = function(name,
                          parameter_type = ParameterTypeEnum$new(),
                          default_value = NULL){
      self$name = name
      self$parameter_type = parameter_type
      self$default_value = default_value
      private$.check_default_value_type(self$default_value, self$parameter_type$python_type)
    },

    #' @description Get the request structure for workflow service calls.
    to_request = function(){
      value = list(
        "Name"=self$name,
        "Type"=self$parameter_type$value
      )
      value[["DefaultValue"]] = self$default_value
      return(value)
    }
  ),
  active = list(

    #' @field expr
    #' The 'Get' expression dict for a `Parameter`
    expr = function(){
      return(private$.expr(self$name))
    }
  ),
  private = list(

    # An internal classmethod for the 'Get' expression dict for a `Parameter`.
    # Args:
    #   name (str): The name of the parameter.
    .expr = function(name){
      return(list("Get"=sprintf("Parameters.%s", name)))
    },

    # Check whether the default value is compatible with the parameter type.
    # Args:
    #   value: The value to check the type for.
    # python_type: The type to check the value against.
    # Raises:
    #   `TypeError` if the value is not compatible with the instance's Python type.
    .check_default_value_type = function(value, python_type){
      if (!is.null(value) && !inherits(value, python_type))
        TypeError$new("The default value specified does not match the Parameter Python type.")
    }
  )
)

#' @title  Workflow ParameterBoolean class
#' @description Pipeline boolean parameter for workflow.
#' @export
ParameterBoolean = R6Class("ParameterBoolean",
  inherit = Parameter,
  public = list(

    #' @description Create a pipeline boolean parameter.
    #' @param name (str): The name of the parameter.
    #' @param default_value (str): The default Python value of the parameter. Defaults to None.
    initialize = function(name,
                          default_value = NULL){
      super$initialize(
        name=name, parameter_type=ParameterTypeEnum$new("BOOLEAN"), default_value=default_value
      )
    }
  )
)

#' @title  Workflow ParameterString class
#' @description Pipeline string parameter for workflow.
#' @export
ParameterString = R6Class("ParameterString",
  inherit = Parameter,
  public = list(

    #' @field enum_values
    #' Placeholder
    enum_values = NULL,

    #' @description Create a pipeline string parameter.
    #' @param name (str): The name of the parameter.
    #' @param default_value (str): The default Python value of the parameter.
    #' @param enum_values (list): placeholder
    initialize =function(name,
                         default_value=NULL,
                         enum_values=NULL){
      super$initialize(
        name=name, parameter_type=ParameterTypeEnum$new("STRING"), default_value=default_value
      )
      self$enum_values = enum_values
    },

    #' @description Get the request structure for workflow service calls.
    to_request = function(){
      request_dict = super$to_request()
      request_dict[["EnumValues"]] = self$enum_values
      return(request_dict)
    }
  ),
  active = list(

    #' @field str
    #' Return default value or implicit value
    str = function(){
      return(self$default_value %||% character(1))
    }
  )
)

#' @title  Workflow ParameterInteger class
#' @description Pipeline integer parameter for workflow.
#' @export
ParameterInteger = R6Class("ParameterInteger",
  inherit = Parameter,
  public = list(

    #' @description Create a pipeline integer parameter.
    #' @param name (str): The name of the parameter.
    #' @param default_value (int): The default Python value of the parameter.
    initialize = function(name,
                          default_value=NULL){
      super$initialize(
        name=name, parameter_type=ParameterTypeEnum$new("INTEGER"), default_value=default_value
      )
    }
  ),
  active = list(

    #' @field int
    #' Return default value or implicit value
    int = function(){
      return(self$default_value %||% 0L)
    }
  )
)

#' @title  Workflow ParameterFloat class
#' @description Pipeline float parameter for workflow.
#' @export
ParameterFloat = R6Class("ParameterFloat",
  inherit = Parameter,
  public = list(

    #' @description Create a pipeline float parameter.
    #' @param name (str): The name of the parameter.
    #' @param default_value (float): The default Python value of the parameter.
    initialize = function(name,
                          default_value=NULL){
      super$initialize(
        name=name, parameter_type=ParameterTypeEnum$new("FLOAT"), default_value=default_value
      )
    }
  ),
  active = list(

    #' @field float
    #' Return default value or implicit value
    float = function(){
      return(self$default_value %||% 0)
    }
  )
)
