# NOTE: This code has been modified from AWS Sagemaker Python:
# https://github.com/aws/sagemaker-python-sdk/blob/master/src/sagemaker/workflow/emr_step.py

#' @include workflow_entities.R
#' @include workflow_properties.R
#' @include workflow_steps.R

#' @import R6
#' @import sagemaker.core

#' @title EMRStepConfig class
#' @description Config for a Hadoop Jar step
#' @export
EMRStepConfig = R6Class("EMRStepConfig",
  public = list(

    #' @field jar
    #' A path to a JAR file run during the step.
    jar=NULL,

    #' @field args
    #' A list of command line arguments
    args=NULL,

    #' @field main_class
    #' The name of the main class in the specified Java file.
    main_class=NULL,

    #' @field properties
    #' A list of key-value pairs that are set when the step runs.
    properties=NULL,

    #' @description Create a definition for input data used by an EMR cluster(job flow) step.
    #'              See AWS documentation on the ``StepConfig`` API for more details on the parameters.
    #' @param jar (str): A path to a JAR file run during the step.
    #' @param args (List[str]): A list of command line arguments passed to
    #'              the JAR file's main function when executed.
    #' @param main_class (str): The name of the main class in the specified Java file.
    #' @param properties (List(dict)): A list of key-value pairs that are set when the step runs.
    initialize = function(jar,
                          args=NULL,
                          main_class=NULL,
                          properties=NULL){
      stopifnot(
        is.character(jar),
        is.null(args) || is.list(args),
        is.null(main_class) || is.character(main_class),
        is.null(properties) || is.list(properties)
      )
      self$jar = jar
      self$args = args
      self$main_class = main_class
      self$properties = properties
    },

    #' @description Convert EMRStepConfig object to request list.
    to_request = function(){
      config = list("HadoopJarStep"=list("Jar"=self$jar))
      config[["HadoopJarStep"]][["Args"]] = self$args
      config[["HadoopJarStep"]][["MainClass"]] = self$main_class
      config[["HadoopJarStep"]][["Properties"]] = self$properties
      return(config)
    },

    #' @description format class
    format = function(){
      format_class(self)
    }
  )
)

#' @title EMRStep class
#' @description EMR step for workflow.
#' @export
EMRStep = R6Class("EMRStep",
  inherit = Step,
  public = list(

    #' @description Constructs a EMRStep.
    #' @param name (str): The name of the EMR step.
    #' @param display_name (str): The display name of the EMR step.
    #' @param description (str): The description of the EMR step.
    #' @param cluster_id (str): The ID of the running EMR cluster.
    #' @param step_config (EMRStepConfig): One StepConfig to be executed by the job flow.
    #' @param depends_on (List[str]): A list of step names this `sagemaker.workflow.steps.EMRStep`
    #'              depends on
    #' @param cache_config (CacheConfig): A `sagemaker.workflow.steps.CacheConfig` instance.
    initialize = function(name,
                          display_name,
                          description,
                          cluster_id,
                          step_config,
                          depends_on=NULL,
                          cache_config=NULL){
      stopifnot(
        is.character(name),
        is.character(display_name),
        is.character(description),
        is.character(cluster_id),
        inherits(step_config, "EMRStepConfig"),
        is.null(depends_on) || is.list(depends_on),
        is.null(cache_config) || inherits(cache_config, "CacheConfig")
      )
      super$initialize(name, display_name, description, StepTypeEnum$EMR, depends_on)

      emr_step_args = list("ClusterId"=cluster_id, "StepConfig"=step_config$to_request())
      self$args = emr_step_args
      self$cache_config = cache_config

      root_property = Properties$new(
        path=sprintf("Steps.%s",name), shape_name="Step", service_name="emr"
      )
      root_property[["ClusterId"]] = cluster_id
      private$.properties = root_property
    },

    #' @description Updates the dictionary with cache configuration.
    to_request = function(){
      request_dict = super$to_request()
      if (!islistempty(self$cache_config))
        request_dict = modifyList(request_dict, self$cache_config$config)
      return(request_dict)
    }
  ),
  active = list(

    #' @field arguments
    #' The arguments dict that is used to call `AddJobFlowSteps`.
    #' NOTE: The AddFlowJobSteps request is not quite the args list that workflow needs.
    #' The Name attribute in AddJobFlowSteps cannot be passed; it will be set during runtime.
    #' In addition to that, we will also need to include emr job inputs and output config.
    arguments = function(){
      return(self$args)
    },

    #' @field properties
    #' A Properties object representing the EMR DescribeStepResponse model
    properties = function(){
      return(private$.properties)
    }
  ),
  lock_objects = F
)
