% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/workflow_airflow.R
\name{deploy_config}
\alias{deploy_config}
\title{Export Airflow deploy config from a SageMaker model}
\usage{
deploy_config(
  model,
  initial_instance_count,
  instance_type,
  endpoint_name = NULL,
  tags = NULL
)
}
\arguments{
\item{model}{(sagemaker.model.Model): The SageMaker model to export the Airflow
config from.}

\item{initial_instance_count}{(int): The initial number of instances to run in
the ``Endpoint`` created from this ``Model``.}

\item{instance_type}{(str): The EC2 instance type to deploy this Model to. For
example, 'ml.p2.xlarge'.}

\item{endpoint_name}{(str): The name of the endpoint to create (default: None).
If not specified, a unique endpoint name will be created.}

\item{tags}{(list[dict]): List of tags for labeling a training job. For more,
see https://docs.aws.amazon.com/sagemaker/latest/dg/API_Tag.html.}
}
\value{
dict: Deploy config that can be directly used by
             SageMakerEndpointOperator in Airflow.
}
\description{
Export Airflow deploy config from a SageMaker model
}
