% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/workflow_retry.R
\name{RetryPolicy}
\alias{RetryPolicy}
\title{RetryPolicy base class}
\description{
RetryPolicy base class

RetryPolicy base class
}
\section{Super class}{
\code{\link[sagemaker.workflow:Entity]{sagemaker.workflow::Entity}} -> \code{RetryPolicy}
}
\section{Public fields}{
\if{html}{\out{<div class="r6-fields">}}
\describe{
\item{\code{backoff_rate}}{(float): The multiplier by which the retry interval increases
during each attempt (default: 2.0)}

\item{\code{interval_seconds}}{(int): An integer that represents the number of seconds before the
first retry attempt (default: 1)}

\item{\code{max_attempts}}{(int): A positive integer that represents the maximum
number of retry attempts. (default: None)}

\item{\code{expire_after_mins}}{(int): A positive integer that represents the maximum minute
to expire any further retry attempt (default: None)}
}
\if{html}{\out{</div>}}
}
\section{Methods}{
\subsection{Public methods}{
\itemize{
\item \href{#method-new}{\code{RetryPolicy$new()}}
\item \href{#method-validate_backoff_rate}{\code{RetryPolicy$validate_backoff_rate()}}
\item \href{#method-validate_interval_seconds}{\code{RetryPolicy$validate_interval_seconds()}}
\item \href{#method-validate_max_attempts}{\code{RetryPolicy$validate_max_attempts()}}
\item \href{#method-validate_expire_after_mins}{\code{RetryPolicy$validate_expire_after_mins()}}
\item \href{#method-to_request}{\code{RetryPolicy$to_request()}}
\item \href{#method-format}{\code{RetryPolicy$format()}}
\item \href{#method-clone}{\code{RetryPolicy$clone()}}
}
}
\if{html}{
\out{<details open ><summary>Inherited methods</summary>}
\itemize{
}
\out{</details>}
}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-new"></a>}}
\if{latex}{\out{\hypertarget{method-new}{}}}
\subsection{Method \code{new()}}{
Initialize RetryPolicy class
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{RetryPolicy$new(
  backoff_rate = DEFAULT_BACKOFF_RATE,
  interval_seconds = DEFAULT_INTERVAL_SECONDS,
  max_attempts = NULL,
  expire_after_mins = NULL
)}\if{html}{\out{</div>}}
}

\subsection{Arguments}{
\if{html}{\out{<div class="arguments">}}
\describe{
\item{\code{backoff_rate}}{(float): The multiplier by which the retry interval increases
during each attempt (default: 2.0)}

\item{\code{interval_seconds}}{(int): An integer that represents the number of seconds before the
first retry attempt (default: 1)}

\item{\code{max_attempts}}{(int): A positive integer that represents the maximum
number of retry attempts. (default: None)}

\item{\code{expire_after_mins}}{(int): A positive integer that represents the maximum minute
to expire any further retry attempt (default: None)}
}
\if{html}{\out{</div>}}
}
}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-validate_backoff_rate"></a>}}
\if{latex}{\out{\hypertarget{method-validate_backoff_rate}{}}}
\subsection{Method \code{validate_backoff_rate()}}{
Validate the input back off rate type
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{RetryPolicy$validate_backoff_rate(value)}\if{html}{\out{</div>}}
}

\subsection{Arguments}{
\if{html}{\out{<div class="arguments">}}
\describe{
\item{\code{value}}{object to be checked}
}
\if{html}{\out{</div>}}
}
}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-validate_interval_seconds"></a>}}
\if{latex}{\out{\hypertarget{method-validate_interval_seconds}{}}}
\subsection{Method \code{validate_interval_seconds()}}{
Validate the input interval seconds
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{RetryPolicy$validate_interval_seconds(value)}\if{html}{\out{</div>}}
}

\subsection{Arguments}{
\if{html}{\out{<div class="arguments">}}
\describe{
\item{\code{value}}{object to be checked}
}
\if{html}{\out{</div>}}
}
}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-validate_max_attempts"></a>}}
\if{latex}{\out{\hypertarget{method-validate_max_attempts}{}}}
\subsection{Method \code{validate_max_attempts()}}{
Validate the input max attempts
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{RetryPolicy$validate_max_attempts(value)}\if{html}{\out{</div>}}
}

\subsection{Arguments}{
\if{html}{\out{<div class="arguments">}}
\describe{
\item{\code{value}}{object to be checked}
}
\if{html}{\out{</div>}}
}
}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-validate_expire_after_mins"></a>}}
\if{latex}{\out{\hypertarget{method-validate_expire_after_mins}{}}}
\subsection{Method \code{validate_expire_after_mins()}}{
Validate expire after mins
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{RetryPolicy$validate_expire_after_mins(value)}\if{html}{\out{</div>}}
}

\subsection{Arguments}{
\if{html}{\out{<div class="arguments">}}
\describe{
\item{\code{value}}{object to be checked}
}
\if{html}{\out{</div>}}
}
}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-to_request"></a>}}
\if{latex}{\out{\hypertarget{method-to_request}{}}}
\subsection{Method \code{to_request()}}{
Get the request structure for workflow service calls.
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{RetryPolicy$to_request()}\if{html}{\out{</div>}}
}

\subsection{Arguments}{
\if{html}{\out{<div class="arguments">}}
\describe{
\item{\code{value}}{object to be checked}
}
\if{html}{\out{</div>}}
}
}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-format"></a>}}
\if{latex}{\out{\hypertarget{method-format}{}}}
\subsection{Method \code{format()}}{
format class
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{RetryPolicy$format()}\if{html}{\out{</div>}}
}

}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-clone"></a>}}
\if{latex}{\out{\hypertarget{method-clone}{}}}
\subsection{Method \code{clone()}}{
The objects of this class are cloneable with this method.
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{RetryPolicy$clone(deep = FALSE)}\if{html}{\out{</div>}}
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
