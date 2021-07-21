# NOTE: This code has been modified from AWS Sagemaker Python:
# https://github.com/aws/sagemaker-python-sdk/blob/master/src/sagemaker/workflow/utilities.py

#' @title Get the request structure for list of entities.
#' @param entities (Sequence[Entity]): A list of entities.
#' @return list: A request structure for a workflow service call.
list_to_request = function(entities){
  lapply(entities, function(entity){
    if(inherits(entity, "Entity")){
      entity$to_request()
    } else if (inherits(entity, "StepCollection")){
      entity$request_lists()
    }
  })
}
