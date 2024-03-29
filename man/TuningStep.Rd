% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/workflow_steps.R
\name{TuningStep}
\alias{TuningStep}
\title{Workflow TuningStep class}
\description{
Tuning step for workflow.
}
\section{Super classes}{
\code{\link[sagemaker.workflow:Entity]{sagemaker.workflow::Entity}} -> \code{\link[sagemaker.workflow:Step]{sagemaker.workflow::Step}} -> \code{\link[sagemaker.workflow:ConfigurableRetryStep]{sagemaker.workflow::ConfigurableRetryStep}} -> \code{TuningStep}
}
\section{Active bindings}{
\if{html}{\out{<div class="r6-active-bindings">}}
\describe{
\item{\code{arguments}}{The arguments dict that is used to call `create_hyper_parameter_tuning_job`.
NOTE: The CreateHyperParameterTuningJob request is not quite the
args list that workflow needs.
The HyperParameterTuningJobName attribute cannot be included.}

\item{\code{properties}}{A Properties object representing
`DescribeHyperParameterTuningJobResponse` and
`ListTrainingJobsForHyperParameterTuningJobResponse` data model.}
}
\if{html}{\out{</div>}}
}
\section{Methods}{
\subsection{Public methods}{
\itemize{
\item \href{#method-new}{\code{TuningStep$new()}}
\item \href{#method-to_request}{\code{TuningStep$to_request()}}
\item \href{#method-get_top_model_s3_uri}{\code{TuningStep$get_top_model_s3_uri()}}
\item \href{#method-clone}{\code{TuningStep$clone()}}
}
}
\if{html}{
\out{<details open ><summary>Inherited methods</summary>}
\itemize{
\item \out{<span class="pkg-link" data-pkg="sagemaker.workflow" data-topic="Step" data-id="add_depends_on">}\href{../../sagemaker.workflow/html/Step.html#method-add_depends_on}{\code{sagemaker.workflow::Step$add_depends_on()}}\out{</span>}
\item \out{<span class="pkg-link" data-pkg="sagemaker.workflow" data-topic="Step" data-id="format">}\href{../../sagemaker.workflow/html/Step.html#method-format}{\code{sagemaker.workflow::Step$format()}}\out{</span>}
\item \out{<span class="pkg-link" data-pkg="sagemaker.workflow" data-topic="ConfigurableRetryStep" data-id="add_retry_policy">}\href{../../sagemaker.workflow/html/ConfigurableRetryStep.html#method-add_retry_policy}{\code{sagemaker.workflow::ConfigurableRetryStep$add_retry_policy()}}\out{</span>}
}
\out{</details>}
}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-new"></a>}}
\if{latex}{\out{\hypertarget{method-new}{}}}
\subsection{Method \code{new()}}{
Construct a TuningStep, given a `HyperparameterTuner` instance.
             In addition to the tuner instance, the other arguments are those that are supplied to
             the `fit` method of the `sagemaker.tuner.HyperparameterTuner`.
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{TuningStep$new(
  name,
  tuner,
  display_name = NULL,
  description = NULL,
  inputs = NULL,
  job_arguments = NULL,
  cache_config = NULL,
  depends_on = NULL,
  retry_policies = NULL
)}\if{html}{\out{</div>}}
}

\subsection{Arguments}{
\if{html}{\out{<div class="arguments">}}
\describe{
\item{\code{name}}{(str): The name of the tuning step.}

\item{\code{tuner}}{(HyperparameterTuner): A `sagemaker.tuner.HyperparameterTuner` instance.}

\item{\code{display_name}}{(str): The display name of the tuning step.}

\item{\code{description}}{(str): The description of the tuning step.}

\item{\code{inputs}}{: Information about the training data. Please refer to the
      ``fit()`` method of the associated estimator, as this can take
      any of the following forms:
\itemize{
   \item{(str) - The S3 location where training data is saved.}
   \item{(dict[str, str] or dict[str, sagemaker.inputs.TrainingInput]) -
         If using multiple channels for training data, you can specify
         a dict mapping channel names to strings or
         :func:`~sagemaker.inputs.TrainingInput` objects.}
   \item{(sagemaker.inputs.TrainingInput) - Channel configuration for S3 data sources
         that can provide additional information about the training dataset.
         See :func:`sagemaker.inputs.TrainingInput` for full details.}
   \item{(sagemaker.session.FileSystemInput) - channel configuration for
         a file system data source that can provide additional information as well as
         the path to the training dataset.}
   \item{(sagemaker.amazon.amazon_estimator.RecordSet) - A collection of
         Amazon :class:~`Record` objects serialized and stored in S3.
         For use with an estimator for an Amazon algorithm.}
   \item{(sagemaker.amazon.amazon_estimator.FileSystemRecordSet) -
         Amazon SageMaker channel configuration for a file system data source for
         Amazon algorithms.}
   \item{(list[sagemaker.amazon.amazon_estimator.RecordSet]) - A list of
         :class:~`sagemaker.amazon.amazon_estimator.RecordSet` objects,
         where each instance is a different channel of training data.}
   \item{(list[sagemaker.amazon.amazon_estimator.FileSystemRecordSet]) - A list of
         :class:~`sagemaker.amazon.amazon_estimator.FileSystemRecordSet` objects,
         where each instance is a different channel of training data.}
  }}

\item{\code{job_arguments}}{(List[str]): A list of strings to be passed into the processing job.
Defaults to `None`.}

\item{\code{cache_config}}{(CacheConfig):  A `sagemaker.workflow.steps.CacheConfig` instance.}

\item{\code{depends_on}}{(List[str] or List[Step]): A list of step names or step instance
this `sagemaker.workflow.steps.ProcessingStep` depends on}

\item{\code{retry_policies}}{(List[RetryPolicy]):  A list of retry policy}
}
\if{html}{\out{</div>}}
}
}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-to_request"></a>}}
\if{latex}{\out{\hypertarget{method-to_request}{}}}
\subsection{Method \code{to_request()}}{
Updates the dictionary with cache configuration.
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{TuningStep$to_request()}\if{html}{\out{</div>}}
}

}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-get_top_model_s3_uri"></a>}}
\if{latex}{\out{\hypertarget{method-get_top_model_s3_uri}{}}}
\subsection{Method \code{get_top_model_s3_uri()}}{
Get the model artifact s3 uri from the top performing training jobs.
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{TuningStep$get_top_model_s3_uri(top_k, s3_bucket, prefix = "")}\if{html}{\out{</div>}}
}

\subsection{Arguments}{
\if{html}{\out{<div class="arguments">}}
\describe{
\item{\code{top_k}}{(int): the index of the top performing training job
tuning step stores up to 50 top performing training jobs, hence
a valid top_k value is from 0 to 49. The best training job
model is at index 0}

\item{\code{s3_bucket}}{(str): the s3 bucket to store the training job output artifact}

\item{\code{prefix}}{(str): the s3 key prefix to store the training job output artifact}
}
\if{html}{\out{</div>}}
}
}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-clone"></a>}}
\if{latex}{\out{\hypertarget{method-clone}{}}}
\subsection{Method \code{clone()}}{
The objects of this class are cloneable with this method.
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{TuningStep$clone(deep = FALSE)}\if{html}{\out{</div>}}
}

\subsection{Arguments}{
\if{html}{\out{<div class="arguments">}}
\describe{
\item{\code{deep}}{Whether to make a deep clone.}
}
\if{html}{\out{</div>}}
}
}
}
