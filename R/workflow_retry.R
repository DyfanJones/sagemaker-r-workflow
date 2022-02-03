# NOTE: This code has been modified from AWS Sagemaker Python:
# https://github.com/aws/sagemaker-python-sdk/blob/master/src/sagemaker/workflow/retry.py

#' @include workflow_entities.R

#' @import R6
#' @import sagemaker.core

DEFAULT_BACKOFF_RATE = 2.0
DEFAULT_INTERVAL_SECONDS = 1
MAX_ATTEMPTS_CAP = 20
MAX_EXPIRE_AFTER_MIN = 14400

#' @title Step ExceptionType enum.
#' @export
StepExceptionTypeEnum = Enum(
  SERVICE_FAULT = "Step.SERVICE_FAULT",
  THROTTLING = "Step.THROTTLING"
)

#' @title SageMaker Job ExceptionType enum.
#' @export
SageMakerJobExceptionTypeEnum = Enum(
  INTERNAL_ERROR = "SageMaker.JOB_INTERNAL_ERROR",
  CAPACITY_ERROR = "SageMaker.CAPACITY_ERROR",
  RESOURCE_LIMIT = "SageMaker.RESOURCE_LIMIT"
)

#' @title RetryPolicy base class
#' @export
RetryPolicy = R6Class("RetryPolicy",
  inherit = Entity,
  public = list(

    #' @field backoff_rate
    #' (float): The multiplier by which the retry interval increases
    #' during each attempt (default: 2.0)
    backoff_rate = DEFAULT_BACKOFF_RATE,

    #' @field interval_seconds
    #' (int): An integer that represents the number of seconds before the
    #' first retry attempt (default: 1)
    interval_seconds = DEFAULT_INTERVAL_SECONDS,

    #' @field max_attempts
    #' (int): A positive integer that represents the maximum
    #' number of retry attempts. (default: None)
    max_attempts = NULL,

    #' @field expire_after_mins
    #' (int): A positive integer that represents the maximum minute
    #' to expire any further retry attempt (default: None)
    expire_after_mins = NULL,

    #' @description Initialize RetryPolicy class
    #' @param backoff_rate (float): The multiplier by which the retry interval increases
    #'              during each attempt (default: 2.0)
    #' @param interval_seconds (int): An integer that represents the number of seconds before the
    #'              first retry attempt (default: 1)
    #' @param max_attempts (int): A positive integer that represents the maximum
    #'              number of retry attempts. (default: None)
    #' @param expire_after_mins (int): A positive integer that represents the maximum minute
    #'              to expire any further retry attempt (default: None)
    initialize = function(backoff_rate = DEFAULT_BACKOFF_RATE,
                          interval_seconds = DEFAULT_INTERVAL_SECONDS,
                          max_attempts = NULL,
                          expire_after_mins = NULL){
      self$backoff_rate = backoff_rate
      self$interval_seconds = interval_seconds
      self$max_attempts = max_attempts
      self$expire_after_mins = expire_after_mins

      self$validate_backoff_rate(self$backoff_rate)
      self$validate_interval_seconds(self$interval_seconds)
      self$validate_max_attempts(self$max_attempts)
      self$validate_expire_after_mins(self$expire_after_mins)
    },

    #' @description Validate the input back off rate type
    #' @param value object to be checked
    validate_backoff_rate = function(value){
      retry_validator(value, {value >= 0.0}, "backoff_rate should be non-negative")
    },

    #' @description Validate the input interval seconds
    #' @param value object to be checked
    validate_interval_seconds = function(value) {
      retry_validator(value, {value >= 0.0}, "interval_seconds rate should be non-negative")
    },

    #' @description Validate the input max attempts
    #' @param value object to be checked
    validate_max_attempts = function(value){
      retry_validator(
        value,
        {MAX_ATTEMPTS_CAP >= value & value >= 1},
        sprintf("max_attempts must in range of (0, %s] attempts", MAX_ATTEMPTS_CAP)
      )
    },

    #' @description Validate expire after mins
    #' @param value object to be checked
    validate_expire_after_mins = function(value){
      retry_validator(
        value,
        {MAX_EXPIRE_AFTER_MIN >= value & value >= 0},
        sprintf("expire_after_mins must in range of (0, %s] minutes", MAX_EXPIRE_AFTER_MIN)
      )
    },

    #' @description Get the request structure for workflow service calls.
    #' @param value object to be checked
    to_request = function(){
      if (is.null(self$max_attempts) && is.null(self$expire_after_mins))
        ValueError$new("Only one of [max_attempts] and [expire_after_mins] can be given.")

      request = list(
        "BackoffRate"=self$backoff_rate,
        "IntervalSeconds"=self$interval_seconds
      )
      request[["MaxAttempts"]] = self$max_attempts
      request[["ExpireAfterMin"]] = self$expire_after_mins
      return(request)
    },

    #' @description format class
    format = function(){
      format_class(self)
    }
  )
)

retry_validator = function(value, cnd, error_msg){
  if(!missing(value) || !is.null(value)){
    if(!eval.parent(substitute(cnd))){
      ValueError$new(error_msg)
    }
  }
}

#' @title StepRetryPolicy class
#' @description RetryPolicy for a retryable step. The pipeline service will retry
#' @export
StepRetryPolicy = R6Class("StepRetryPolicy",
  inherit = RetryPolicy,
  public = list(

    #' @field exception_types
    #' (List[StepExceptionTypeEnum]): the exception types to match for this policy
    exception_types = NULL,

    #' @description Initialize StepRetryPolicy class
    #' @param exception_types (List[StepExceptionTypeEnum]): the exception types
    #'              to match for this policy
    #' @param backoff_rate (float): The multiplier by which the retry interval increases
    #'              during each attempt (default: 2.0)
    #' @param interval_seconds (int): An integer that represents the number of seconds before the
    #'              first retry attempt (default: 1)
    #' @param max_attempts (int): A positive integer that represents the maximum
    #'              number of retry attempts. (default: None)
    #' @param expire_after_mins (int): A positive integer that represents the maximum minute
    #'              to expire any further retry attempt (default: None)
    initialize = function(exception_types,
                          backoff_rate = 2.0,
                          interval_seconds = 1,
                          max_attempts = NULL,
                          expire_after_mins = NULL){
      super$intialize(backoff_rate, interval_seconds, max_attempts, expire_after_mins)
      for(exception_type in exception_types){
        if(!(exception_type %in% enum_items(StepExceptionTypeEnum)))
          ValueError$new(sprintf(
            "%s is not of StepExceptionTypeEnum", exception_type)
          )
      }
      self$exception_types = exception_types
    },

    #' @description Gets the request structure for retry policy.
    to_request = function(){
      request = super$to_request()
      request[["ExceptionType"]] = self$exception_types
      return(request)
    }
  )
)

#' @title SageMakerJobStepRetryPolicy class
#' @description RetryPolicy for exception thrown by SageMaker Job.
#' @export
SageMakerJobStepRetryPolicy = R6Class("SageMakerJobStepRetryPolicy",
  inherit = RetryPolicy,
  public = list(

    #' @field exception_type_list
    #' Contains exception_types or failure_reason_types
    exception_type_list = list(),

    #' @description Initialize SageMakerJobStepRetryPolicy
    #' @param exception_types (List[SageMakerJobExceptionTypeEnum]):
    #'              The SageMaker exception to match for this policy. The SageMaker exceptions
    #'              captured here are the exceptions thrown by synchronously
    #'              creating the job. For instance the resource limit exception.
    #' @param failure_reason_types (List[SageMakerJobExceptionTypeEnum]): the SageMaker
    #'              failure reason types to match for this policy. The failure reason type
    #'              is presented in FailureReason field of the Describe response, it indicates
    #'              the runtime failure reason for a job.
    #' @param backoff_rate (float): The multiplier by which the retry interval increases
    #'              during each attempt (default: 2.0)
    #' @param interval_seconds (int): An integer that represents the number of seconds before the
    #'              first retry attempt (default: 1)
    #' @param max_attempts (int): A positive integer that represents the maximum
    #'              number of retry attempts. (default: None)
    #' @param expire_after_mins (int): A positive integer that represents the maximum minute
    #'              to expire any further retry attempt (default: None)
    initialize = function(exception_types = NULL,
                          failure_reason_types = NULL,
                          backoff_rate = 2.0,
                          interval_seconds = 1,
                          max_attempts = NULL,
                          expire_after_mins = NULL){
      super$initialize(backoff_rate, interval_seconds, max_attempts, expire_after_mins)

      if (is.null(exception_types) && is.null(failure_reason_types))
        ValueError$new(
          "At least one of the [exception_types, failure_reason_types] needs to be given."
        )

      self$exception_type_list = c(exception_types, failure_reason_types)

      for(exception_type in self$exception_type_list){
        if(!(exception_type %in% enum_items(SageMakerJobExceptionTypeEnum)))
          ValueError$new(sprintf(
            "%s is not of SageMakerJobExceptionTypeEnum.",exception_type)
          )
      }
    },

    #' @description Gets the request structure for retry policy
    to_request = function(){
      request = super$to_request()
      request[["ExceptionType"]] = self$exception_type_list
      return(request)
    }
  )
)
