# NOTE: This code has been modified from AWS Sagemaker Python:
# https://github.com/aws/sagemaker-python-sdk/blob/master/src/sagemaker/workflow/utilities.py

#' @importFrom digest digest

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

#' @title Get the MD5 hash of a file.
#' @param path (str): The local path for the file.
#' @return str: The MD5 hash of the file.
hash_file = function(path){
  BUFF_SIZE = 65536 # read in 64KiB chunks
  file_con = file(path, "rb")
  size = file.size(path)
  on.exit(close(file_con))

  splits = ceiling(size / BUFF_SIZE)
  obj = unlist(lapply(1:splits, function(split){
    readBin(file_con, "raw", n = BUFF_SIZE)
  }))
  return(digest::digest(obj, algo="md5", serialize = FALSE))
}
