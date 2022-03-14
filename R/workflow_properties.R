# NOTE: This code has been modified from AWS Sagemaker Python:
# https://github.com/aws/sagemaker-python-sdk/blob/master/src/sagemaker/workflow/properties.py

#' @include workflow_entities.R
#' @include r_utils.R

#' @import R6
#' @import sagemaker.core
#' @importFrom jsonlite fromJSON
#' @importFrom fs path

#' @title Workflow PropertiesMeta Class
#' @description Load an internal shapes attribute from the botocore sagemaker service model.
#' @keywords internal
PropertiesMeta = R6Class("PropertiesMeta",
  public = list(

    #' @description Loads up the shapes from the botocore sagemaker service model.
    #' @param ... currently not implemented
    initialize = function(...){
      kwargs = list(...)
      if(is.null(private$.shapes_map)){
        private$.shapes_map = list(
          sagemaker = sagemaker_service,
          emr = emr_service
        )
      }
    },

    #' @description format class
    format = function(){
      format_class(self)
    }
  ),
  private = list(
    .shapes_map = NULL,
    .primitive_types = c("string", "boolean", "integer", "float")
  ),
  lock_objects = F
)

#' @title Workflow Properties Class
#' @description Properties for use in workflow expressions.
#' @export
Properties = R6Class("Properties",
  inherit = PropertiesMeta,
  public = list(

    #' @field shape_name
    #' The botocore sagemaker service model shape name.
    shape_name = NULL,

    #' @field shape_names
    #' A List of the botocore sagemaker service model shape name
    shape_names = NULL,

    #' @description Create a Properties instance representing the given shape.
    #' @param path (str): The parent path of the Properties instance.
    #' @param shape_name (str): The botocore sagemaker service model shape name.
    #' @param shape_names (str): A List of the botocore sagemaker service model shape name.
    #' @param service_name (str):
    initialize = function(path,
                          shape_name=NULL,
                          shape_names=NULL,
                          service_name="sagemaker"){
      stopifnot(
        is.character(path),
        is.character(shape_name) || is.null(shape_name),
        is.character(shape_names) || is.null(shape_names)
      )

      private$.path = path
      shape_names = shape_names %||% list()
      private$.shape_names = if(is.null(shape_name)) shape_names else c(shape_name, shape_names)

      super$initialize()
      shapes = private$.shapes_map[[service_name]] %||% list()

      for (name in private$.shape_names){
        shape = shapes[[name]] %||% list()
        shape_type = shape[["type"]] %||% ""
        if(shape_type %in% private$.primitive_types){
          # self$format = function() name
        } else if (shape_type == "structure") {
          members = shape[["members"]]
          for(key in names(members)){
            info = members[[key]]
            if (shapes[[info[["shape"]]]][["type"]] == "list"){
              self[[key]] = PropertiesList$new(
                sprintf("%s.%s", path, key), info[["shape"]], service_name
              )
            } else if (shapes[[info[["shape"]]]][["type"]] == "map") {
              self[[key]] = PropertiesMap$new(
                sprintf("%s.%s", path, key), info[["shape"]], service_name
              )
            } else {
              self[[key]] = Properties$new(
                sprintf("%s.%s", path, key), info[["shape"]], service_name=service_name
              )
            }
          }
        }
      }
    }
  ),
  active = list(

    #' @field expr
    #' The 'Get' expression dict for a `Properties`.
    expr = function(){
      return(list("Get"=private$.path))
    }
  ),
  private = list(
    .path=NULL,
    .shape_names=NULL
  ),
  lock_objects = F
)

#' @title Workflow PropertiesList class
#' @description PropertiesList for use in workflow expressions
#' @export
PropertiesList = R6Class("PropertiesList",
  inherit = Properties,
  public = list(

    #' @description Create a PropertiesList instance representing the given shape.
    #' @param path (str): The parent path of the PropertiesList instance.
    #' @param shape_name (str): The botocore sagemaker service model shape name.
    #' @param root_shape_name (str): The botocore sagemaker service model shape name.
    #' @param service_name (str): The botocore service name.
    initialize = function(path,
                          shape_name=NULL,
                          service_name="sagemaker"){
      super$initialize(path, shape_name)
      self$shape_name = shape_name
      self$service_name = service_name
      private$.items = list()
    },

    #' @description Populate the indexing item with a Property, for both lists and dictionaries.
    #' @param item (Union[int, str]): The index of the item in sequence.
    get_item = function(item){
      if (!(item %in% names(private$.items))){
        shape = private$.shapes_map[[self$service_name]][[self$shape_name]]
        member = shape[["member"]][["shape"]]
        if (is.character(item)){
          property_item = Properties$new(
            sprintf("%s[%s]", private$.path, item), member)
        } else {
          property_item = Properties$new(
            sprintf("%s[%s]", private$.path, item), member)
        }
        private$.items[[item]] = property_item
      }
      return(private$.items[[item]])
    }
  ),
  private = list(
    .items = NULL
  ),
  lock_objects = F
)

#' @title PropertiesMap class
#' @description PropertiesMap for use in workflow expressions.
#' @export
PropertiesMap = R6Class("PropertiesMap",
  inherit = Properties,
  public = list(

    #' @field path
    #' The parent path of the PropertiesMap instance.
    path = NULL,

    #' @field shape_name
    #' The botocore sagemaker service model shape name.
    shape_name = NULL,

    #' @field service_name
    #' The botocore service name.
    service_name = NULL,

    #' @description Create a PropertiesMap instance representing the given shape.
    #' @param path (str): The parent path of the PropertiesMap instance.
    #' @param shape_name (str): The botocore sagemaker service model shape name.
    #' @param service_name (str): The botocore service name.
    initialize = function(path,
                          shape_name=NULL,
                          service_name="sagemaker"){
      super$initialize(path, shape_name)
      self$shape_name = shape_name
      self$service_name = service_name
      private$.items = list()
    },

    #' @description Populate the indexing item with a Property, for both lists and dictionaries.
    #' @param item (Union[int, str]): The index of the item in sequence.
    get_item = function(item){
      if (!(item %in% names(private$.items))){
        shape = private$.shapes_map[[self$service_name]][[self$shape_name]]
        member = shape[["value"]][["shape"]]
        if (is.character(item)){
          property_item = Properties$new(
            sprintf("%s['%s']", private$.path, item), member
          )
        } else {
          property_item = Properties$new(
            sprintf("%s['%s']", private$.path, item), member
          )
        }
        private$.items[[item]] = property_item
      }
      return(private$.items[[item]])
    }
  ),
  lock_objects=F
)

#' @title PropertyFile Class
#' @description Provides a property file struct.
#' @export
PropertyFile = R6Class("PropertyFile",
  inherit = Expression,
  public = list(

    #' @field name
    #' The name of the property file for reference with `JsonGet` functions.
    name = NULL,

    #' @field output_name
    #' The name of the processing job output channel.
    output_name = NULL,

    #' @field path
    #' The path to the file at the output channel location.
    path = NULL,

    #' @description Initializing PropertyFile Class
    #' @param name (str): The name of the property file for reference with `JsonGet` functions.
    #' @param output_name (str): The name of the processing job output channel.
    #' @param path (str): The path to the file at the output channel location.
    initialize = function(name,
                          output_name,
                          path){
      self$name = name
      self$output_name = output_name
      self$path = path
    }
  ),
  active = list(

    #' @field expr
    #' Get the expression structure for workflow service calls.
    expr = function(){
      obj = list(
        "PropertyFileName"=self$name,
        "OutputName"=self$output_name,
        "FilePath"=self$path
      )
      return(private$.validate_value(obj, "list"))
    }
  ),
  lock_objects=F
)
