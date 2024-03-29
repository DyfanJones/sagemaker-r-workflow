% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/wrangler_ingestion.R
\name{generate_data_ingestion_flow_from_s3_input}
\alias{generate_data_ingestion_flow_from_s3_input}
\title{generate data ingestion flow from s3 input}
\usage{
generate_data_ingestion_flow_from_s3_input(
  input_name,
  s3_uri,
  s3_content_type = "csv",
  s3_has_header = FALSE,
  operator_version = "0.1",
  schema = NULL
)
}
\arguments{
\item{input_name}{(str): the name of the input to flow source node}

\item{s3_uri}{(str): uri for the s3 input to flow source node}

\item{s3_content_type}{(str): s3 input content type}

\item{s3_has_header}{(bool): flag indicating the input has header or not}

\item{operator_version}{(str): the version of the operator}

\item{schema}{(list): the schema for the data to be ingested}
}
\value{
list: A flow only conduct data ingestion with 1-1 mapping
             output_name (str): The output name used to configure
             `sagemaker.processing.FeatureStoreOutput`
}
\description{
Generate the data ingestion only flow from s3 input
}
