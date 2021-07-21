# NOTE: This code has been modified from AWS Sagemaker Python:
# https://github.com/aws/sagemaker-python-sdk/blob/master/src/sagemaker/workflow/properties.py

#' @include workflow_entities.R
#' @include r_utils.R

#' @import R6
#' @import R6sagemaker.common
#' @importFrom jsonlite read_json
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
      if(is.null(private$.shapes)){
        private$.shapes = properties_env$sagemaker_model[["shapes"]]
      }
    },

    #' @description format class
    format = function(){
      format_class(self)
    }
  ),
  private = list(
    .shapes = NULL,
    .primitive_types = c("string", "boolean", "integer", "float")
  ),
  lock_objects = F
)

.load_service_model = function(){
  path = system.file(
    fs::path("sagemaker", "2017-07-24", "service-2.json"),
    package = "R6sagemaker.workflow")
  return(jsonlite::read_json(path))
}

# cache internal attribute from botocore sagemaker service model
properties_env = new.env(parent = emptyenv())
properties_env$sagemaker_model = .load_service_model()

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
    initialize = function(path,
                          shape_name=NULL,
                          shape_names=NULL){
      stopifnot(
        is.character(path),
        is.character(shape_name) || is.null(shape_name),
        is.character(shape_names) || is.null(shape_names)
      )

      private$.path = path
      shape_names = shape_names %||% list()
      private$.shape_names = if(is.null(shape_name)) shape_names else c(shape_name, shape_names)

      super$initialize()

      for (name in private$.shape_names){
        shape = private$.shapes[[name]] %||% list()
        shape_type = shape[["type"]]
        if(shape_type %in% private$.primitive_types){
          # self$format = function() name
        } else if (shape_type == "structure") {
          members = shape[["members"]]
          for(i in seq_along(members)){
            key = names(members[i])
            info = members[[i]]
            if (private$.shapes[[info[["shape"]]]][["type"]] == "list"){
              self[[key]] = PropertiesList$new(sprintf("%s.%s", path, key), info[["shape"]])
            } else {
              self[[key]] = Properties$new(sprintf("%s.%s", path, key), info[["shape"]])
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
    initialize = function(path,
                          shape_name=NULL){
      super$initialize(path, shape_name)
      self$shape_name = shape_name
      private$.items = list()
    },

    #' @description Populate the indexing item with a Property, for both lists and dictionaries.
    #' @param item (Union[int, str]): The index of the item in sequence.
    get_item = function(){
      if (!(item %in% names(private$.items))){
        shape = Properties$private_fields$ ._shapes.get(self.shape_name)
        member = shape[["member"]][["shape"]]
        if (is.character(item)){
          property_item = Properties$new(
            sprintf("%s[%s]", private$.path, item), member)
        } else {
          property_item = Properties$new(
            sprintf("%s[%s]", private$.path, item), member)
          private$.items[[item]] = property_item
        }
      }
      return(private$.items[[item]])
    }
  ),
  private = list(
    .items = NULL
  )
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
  )
)
