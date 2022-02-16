# NOTE: This code has been modified from AWS Sagemaker Python:
# https://github.com/aws/sagemaker-python-sdk/blob/master/src/sagemaker/wrangler/ingestion.py

#' @import R6
#' @import sagemaker.core
#' @importFrom uuid UUIDgenerate

#' @title generate data ingestion flow from s3 input
#' @description Generate the data ingestion only flow from s3 input
#' @param input_name (str): the name of the input to flow source node
#' @param s3_uri (str): uri for the s3 input to flow source node
#' @param s3_content_type (str): s3 input content type
#' @param s3_has_header (bool): flag indicating the input has header or not
#' @param operator_version (str): the version of the operator
#' @param schema (list): the schema for the data to be ingested
#' @return list: A flow only conduct data ingestion with 1-1 mapping
#'              output_name (str): The output name used to configure
#'              `sagemaker.processing.FeatureStoreOutput`
#' @export
generate_data_ingestion_flow_from_s3_input = function(input_name,
                                                      s3_uri,
                                                      s3_content_type="csv",
                                                      s3_has_header=FALSE,
                                                      operator_version="0.1",
                                                      schema=NULL){
  stopifnot(is.character(input_name),
            is.character(s3_uri),
            is.character(s3_content_type),
            is.logical(s3_has_header),
            is.character(operator_version),
            is.list(schema) || is.null(schema))

  source_node = list(
    "node_id"=uuid::UUIDgenerate(),
    "type"="SOURCE",
    "inputs"=list(),
    "outputs"=list(list("name"="default")),
    "operator"=sprintf("sagemaker.s3_source_%s",operator_version),
    "parameters"=list(
      "dataset_definition"=list(
        "datasetSourceType"="S3",
        "name"=input_name,
        "s3ExecutionContext"=list(
          "s3Uri"=s3_uri,
          "s3ContentType"=s3_content_type,
          "s3HasHeader"=s3_has_header)
      )
    )
  )
  output_node = .get_output_node(source_node[["node_id"]], operator_version, schema)

  flow = list(
    "metadata"=list("version"=1, "disable_limits"=FALSE),
    "nodes"=list(source_node, output_node)
  )
  return(list(flow, sprintf('%s.default',output_node[["node_id"]])))
}

#' @title generate data ingestion flow from athena dataset definition
#' @description Generate the data ingestion only flow from athena input
#' @param input_name (str): the name of the input to flow source node
#' @param athena_dataset_definition (AthenaDatasetDefinition): athena input to flow source node
#' @param operator_version (str): the version of the operator
#' @param schema (list): the schema for the data to be ingested
#' @return dict (typing.Dict): A flow only conduct data ingestion with 1-1 mapping
#'              output_name (str): The output name used to configure
#'              `sagemaker.processing.FeatureStoreOutput`
#' @export
generate_data_ingestion_flow_from_athena_dataset_definition = function(input_name,
                                                                       athena_dataset_definition,
                                                                       operator_version="0.1",
                                                                       schema=NULL){
  stopifnot(is.character(input_name),
            inherits(athena_dataset_definition, "AthenaDatasetDefinition"),
            is.character(operator_version),
            is.list(schema) || is.null(schema))
  source_node = list(
    "node_id"=uuid::UUIDgenerate(),
    "type"="SOURCE",
    "inputs"=list(),
    "outputs"=list(list("name"="default")),
    "operator"=sprintf("sagemaker.athena_source_%s", operator_version),
    "parameters"=list(
      "dataset_definition"=list(
        "datasetSourceType"="Athena",
        "name"=input_name,
        "catalogName"=athena_dataset_definition$catalog,
        "databaseName"=athena_dataset_definition$database,
        "queryString"=athena_dataset_definition$query_string,
        "s3OutputLocation"=athena_dataset_definition$output_s3_uri,
        "outputFormat"=athena_dataset_definition$output_format)
    )
  )
  output_node = .get_output_node(source_node[["node_id"]], operator_version, schema)

  flow = list(
    "metadata"=list("version"=1, "disable_limits"=FALSE),
    "nodes"=list(source_node, output_node)
  )
  return(list(flow, sprintf('%s.default',output_node[["node_id"]])))
}

#' @title generate data ingestion flow from redshift dataset definition
#' @description Generate the data ingestion only flow from redshift input
#' @param input_name (str): the name of the input to flow source node
#' @param redshift_dataset_definition (RedshiftDatasetDefinition): redshift input to flow source node
#' @param operator_version (str): the version of the operator
#' @param schema (list): the schema for the data to be ingested
#' @return list: A flow only conduct data ingestion with 1-1 mapping
#'              output_name (str): The output name used to configure
#'              `sagemaker.processing.FeatureStoreOutput`
#' @export
generate_data_ingestion_flow_from_redshift_dataset_definition = function(input_name,
                                                                         redshift_dataset_definition,
                                                                         operator_version="0.1",
                                                                         schema=NULL){
  stopifnot(is.character(input_name),
            inherits(redshift_dataset_definition, "RedshiftDatasetDefinition"),
            is.character(operator_version),
            is.list(schema) || is.null(schema))
  source_node = list(
    "node_id"=uuid::UUIDgenerate(),
    "type"="SOURCE",
    "inputs"=list(),
    "outputs"=list(list("name"="default")),
    "operator"=sprintf("sagemaker.redshift_source_%s", operator_version),
    "parameters"=list(
      "dataset_definition"=list(
        "datasetSourceType"="Redshift",
        "name"=input_name,
        "clusterIdentifier"=redshift_dataset_definition$cluster_id,
        "database"=redshift_dataset_definition$database,
        "dbUser"=redshift_dataset_definition$db_user,
        "queryString"=redshift_dataset_definition$query_string,
        "unloadIamRole"=redshift_dataset_definition$cluster_role_arn,
        "s3OutputLocation"=redshift_dataset_definition$output_s3_uri,
        "outputFormat"=redshift_dataset_definition$output_format)
    )
  )
  output_node = .get_output_node(source_node[["node_id"]], operator_version, schema)

  flow = list(
    "metadata"=list("version"=1, "disable_limits"=FALSE),
    "nodes"=list(source_node, output_node)
  )
  return(list(flow, sprintf('%s.default',output_node[["node_id"]])))
}

# A helper function to generate output node, for internal use only
# Args:
#   source_node_id (str): source node id
# operator_version: (str): the version of the operator
# schema: (typing.Dict): the schema for the data to be ingested
# Returns:
#   dict (typing.Dict): output node
.get_output_node = function(source_node_id,
                            operator_version,
                            schema){
  stopifnot(is.character(source_node_id),
            is.character(operator_version),
            is.list(schema) || is.null(schema))

  return(list(
    "node_id"=uuid::UUIDgenerate(),
    "type"="TRANSFORM",
    "operator"=sprintf("sagemaker.spark.infer_and_cast_type_%s", operator_version),
    "trained_parameters"= schema %||% list(),
    "parameters"=list(),
    "inputs"=list(list("name"="default", "node_id"=source_node_id, "output_name"="default")),
    "outputs"=list(list("name"="default"))
    )
  )
}
