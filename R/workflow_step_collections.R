# NOTE: This code has been modified from AWS Sagemaker Python:
# https://github.com/aws/sagemaker-python-sdk/blob/master/src/sagemaker/workflow/step_collections.py

#' @include workflow_entities.R
#' @include workflow_steps.R
#' @include workflow__utils.R
#' @include r_utils.R

#' @import R6
#' @import sagemaker.core
#' @import sagemaker.common
#' @import sagemaker.mlcore

#' @title Workflow StepCollection class
#' @description A wrapper of pipeline steps for workflow.
#' @export
StepCollection = R6Class("StepCollection",
  public = list(

    #' @field steps
    #' A list of steps.
    steps = NULL,

    #' @description Initialize StepCollection class
    #' @param steps (List[Step]): A list of steps.
    initialize = function(steps){
      self$steps
    },

    #' @description Get the request structure for workflow service calls.
    request_list = function(){
      return(lapply(self$steps, function(step) step$to_request()))
    },

    #' @description format class
    format = function(){
      format_class(self)
    }
  )
)

#' @title Workflow RegisterModel class
#' @description Register Model step collection for workflow.
#' @export
RegisterModel = R6Class("RegisterModel",
  inherit = StepCollection,
  public = list(

    #' @description Construct steps `_RepackModelStep` and `_RegisterModelStep` based on the estimator.
    #' @param name (str): The name of the training step.
    #' @param estimator The estimator instance.
    #' @param model_data The S3 uri to the model data from training.
    #' @param content_types (list): The supported MIME types for the input data (default: None).
    #' @param response_types (list): The supported MIME types for the output data (default: None).
    #' @param inference_instances (list): A list of the instance types that are used to
    #'              generate inferences in real-time (default: None).
    #' @param transform_instances (list): A list of the instance types on which a transformation
    #'              job can be run or on which an endpoint can be deployed (default: None).
    #' @param depends_on (List[str] or List[Step]): The list of step names or step instances
    #'              the first step in the collection depends on
    #' @param repack_model_step_retry_policies (List[RetryPolicy]): The list of retry policies
    #'              for the repack model step
    #' @param register_model_step_retry_policies (List[RetryPolicy]): The list of retry policies
    #'              for register model step
    #' @param model_package_group_name (str): The Model Package Group name, exclusive to
    #'              `model_package_name`, using `model_package_group_name` makes the Model Package
    #'              versioned (default: None).
    #' @param model_metrics (ModelMetrics): ModelMetrics object (default: None).
    #' @param approval_status (str): Model Approval Status, values can be "Approved", "Rejected",
    #'              or "PendingManualApproval" (default: "PendingManualApproval").
    #' @param image_uri (str): The container image uri for Model Package, if not specified,
    #'              Estimator's training container image is used (default: None).
    #' @param compile_model_family (str): The instance family for the compiled model. If
    #'              specified, a compiled model is used (default: None).
    #' @param display_name (str): The display name of the step.
    #' @param description (str): Model Package description (default: None).
    #' @param tags (List[dict[str, str]]): The list of tags to attach to the model package group. Note
    #'              that tags will only be applied to newly created model package groups; if the
    #'              name of an existing group is passed to "model_package_group_name",
    #'              tags will not be applied.
    #' @param model (object or Model): A PipelineModel object that comprises a list of models
    #'              which gets executed as a serial inference pipeline or a Model object.
    #' @param drift_check_baselines (DriftCheckBaselines): DriftCheckBaselines object (default: None).
    #' @param ... : additional arguments to `create_model`.
    initialize = function(name,
                          content_types,
                          response_types,
                          inference_instances,
                          transform_instances,
                          estimator=NULL,
                          model_data=NULL,
                          depends_on=NULL,
                          repack_model_step_retry_policies=NULL,
                          register_model_step_retry_policies=NULL,
                          model_package_group_name=NULL,
                          model_metrics=NULL,
                          approval_status=NULL,
                          image_uri=NULL,
                          compile_model_family=NULL,
                          display_name=NULL,
                          description=NULL,
                          tags=NULL,
                          model=NULL,
                          drift_check_baselines=NULL,
                          ...){
      steps = list()
      repack_model = FALSE
      self$model_list = None
      self$container_def_list = NULL
      subnets = NULL
      security_group_ids = NULL
      kwargs = list(...)

      if (!is.null(estimator)) {
        subnets = estimator$subnets
        security_group_ids = estimator$security_group_ids
      } else if (!is.null(model) && !is.null(model$vpc_config)){
        subnets = model$vpc_config[["Subnets"]]
        security_group_ids = model$vpc_config[["SecurityGroupIds"]]
      }
      if ("entry_point" %in% names(kwargs)){
        repack_model = TRUE
        entry_point = kwargs[["entry_point"]]
        source_dir = kwargs[["source_dir"]]
        dependencies = kwargs[["dependencies"]]
        kwargs[["entry_point"]] = NULL
        kwargs[["source_dir"]] = NULL
        kwargs[["dependencies"]] = NULL
        kwargs = c(kwargs, output_kms_key=kwargs[["model_kms_key"]])
        kwargs[["model_kms_key"]] = NULL

        kwargs2 = c(
          name=sprintf("%sRepackModel", name),
          depends_on=depends_on,
          retry_policies=repack_model_step_retry_policies,
          sagemaker_session=estimator$sagemaker_session,
          role=estimator$role,
          model_data=model_data,
          entry_point=entry_point,
          source_dir=source_dir,
          dependencies=dependencies,
          tags=tags,
          subnets=subnets,
          security_group_ids=security_group_ids,
          description=description,
          display_name=display_name,
          kwargs
        )
        repack_model_step = do.call(.RepackModelStep$new, kwargs2)
        steps = list.append(steps, repack_model_step)
        model_data = repack_model_step$properties$ModelArtifacts$S3ModelArtifacts

        # remove kwargs consumed by model repacking step
        kwargs[["output_kms_key"]] = NULL

      } else if (!is.null(model)){
        if (inherits(model, "PipelineModel")) {
          self$model_list = model$models
        } else if (inherits(model, "Model")) {
            self$model_list = list(model)
        }

        for (model_entity in self$model_list){
          if (!is.null(estimator)){
            sagemaker_session = estimator$sagemaker_session
            role = estimator$role
          } else {
            sagemaker_session = model_entity$sagemaker_session
            role = model_entity$role
          }
          if ("entry_point" %in% names(model_entity) && !is.null(model_entity$entry_point)){
            repack_model = TRUE
            entry_point = model_entity$entry_point
            source_dir = model_entity$source_dir
            dependencies = model_entity$dependencies
            kwargs = c(kwargs, output_kms_key=model_entity$model_kms_key)
            model_name = model_entity$name %||% attr(model_entity, "_framework_name")

            kwargs2 = c(
              name=sprintf("%sRepackModel", model_name),
              depends_on=depends_on,
              retry_policies=repack_model_step_retry_policies,
              sagemaker_session=sagemaker_session,
              role=role,
              model_data=model_entity$model_data,
              entry_point=entry_point,
              source_dir=source_dir,
              dependencies=dependencies,
              tags=tags,
              subnets=subnets,
              security_group_ids=security_group_ids,
              description=description,
              display_name=display_name,
              kwargs2
            )
            repack_model_step = do.call(.RepackModelStep$new, kwargs2)

            steps = list.append(steps, repack_model_step)
            model_entity$model_data = (
              repack_model_step$properties$ModelArtifacts$S3ModelArtifacts
            )
            # remove kwargs consumed by model repacking step
            kwargs[["output_kms_key"]] = NULL
          }
        }

        if (inherits(model, "PipelineModel")){
          self$container_def_list = model$pipeline_container_def(inference_instances[[1]])
        } else if (inherits(model, "Model")){
          self$container_def_list = list(model$prepare_container_def(inference_instances[[1]]))
        }
      }
      kwargs2 = c(
        name=name,
        estimator=estimator,
        model_data=model_data,
        content_types=content_types,
        response_types=response_types,
        inference_instances=inference_instances,
        transform_instances=transform_instances,
        model_package_group_name=model_package_group_name,
        model_metrics=model_metrics,
        drift_check_baselines=drift_check_baselines,
        approval_status=approval_status,
        image_uri=image_uri,
        compile_model_family=compile_model_family,
        description=description,
        display_name=display_name,
        tags=tags,
        container_def_list=self$container_def_list,
        retry_policies=register_model_step_retry_policies,
        kwargs
      )
      register_model_step = do.call(.RegisterModelStep$new, kwargs2)

      if (!isTRUE(repack_model))
        register_model_step$add_depends_on(depends_on)

      steps = list.append(steps, register_model_step)
      self$steps = steps
    }
  ),
  lock_objects = F
)

#' @title Workflow EstimatorTransformer class
#' @description Creates a Transformer step collection for workflow.
#' @export
EstimatorTransformer = R6Class("EstimatorTransformer",
  inherit = StepCollection,
  public = list(

    #' @description Construct steps required for a Transformer step collection:
    #'              An estimator-centric step collection. It models what happens in workflows
    #'              when invoking the `transform()` method on an estimator instance:
    #'              First, if custom model artifacts are required, a `_RepackModelStep` is included.
    #'              Second, a `CreateModelStep` with the model data passed in from
    #'              a training step or other training job output. Finally, a `TransformerStep`.
    #'              If repacking the model artifacts is not necessary, only the CreateModelStep
    #'              and TransformerStep are in the step collection.
    #' @param name (str): The name of the Transform Step.
    #' @param estimator : The estimator instance.
    #' @param model_data (str): The S3 location of a SageMaker model data
    #'              ``.tar.gz`` file (default: None).
    #' @param model_inputs (CreateModelInput): A `sagemaker.inputs.CreateModelInput` instance.
    #'              Defaults to `None`.
    #' @param instance_count (int): The number of EC2 instances to use.
    #' @param instance_type (str): The type of EC2 instance to use.
    #' @param transform_inputs (TransformInput): A `sagemaker.inputs.TransformInput` instance.
    #' @param description (str): The description of the step.
    #' @param display_name (str): The display name of the step.
    #' @param image_uri (str): A Docker image URI.
    #' @param predictor_cls (callable[string, :Session]): A
    #'              function to call to create a predictor (default: None). If not
    #'              None, ``deploy`` will return the result of invoking this
    #'              function on the created endpoint name.
    #' @param strategy (str): The strategy used to decide how to batch records in
    #'              a single request (default: None). Valid values: 'MultiRecord'
    #'              and 'SingleRecord'.
    #' @param assemble_with (str): How the output is assembled (default: None).
    #'              Valid values: 'Line' or 'None'.
    #' @param output_path (str): The S3 location for saving the transform result. If
    #'              not specified, results are stored to a default bucket.
    #' @param output_kms_key (str): Optional. A KMS key ID for encrypting the
    #'              transform output (default: None).
    #' @param accept (str): The accept header passed by the client to
    #'              the inference endpoint. If it is supported by the endpoint,
    #'              it will be the format of the batch transform output.
    #' @param max_concurrent_transforms (int): The maximum number of HTTP requests
    #'              to be made to each individual transform container at one time.
    #' @param max_payload (int): Maximum size of the payload in a single HTTP
    #' @param tags (list[dict]): List of tags for labeling a training job. For
    #'              more, see \url{https://docs.aws.amazon.com/sagemaker/latest/dg/API_Tag.html}.
    #' @param volume_kms_key (str): Optional. KMS key ID for encrypting the volume
    #'              attached to the ML compute instance (default: None).
    #' @param env (dict): The Environment variables to be set for use during the
    #'              transform job (default: None).
    #' @param depends_on (List[str] or List[Step]): The list of step names or step instances
    #'              the first step in the collection depends on
    #' @param repack_model_step_retry_policies (List[RetryPolicy]): The list of retry policies
    #'              for the repack model step
    #' @param model_step_retry_policies (List[RetryPolicy]): The list of retry policies for
    #'              model step
    #' @param transform_step_retry_policies (List[RetryPolicy]): The list of retry policies for
    #'              transform step
    #' @param ... : pass onto model class.
    initialize = function(name,
                          estimator,
                          model_data,
                          model_inputs,
                          instance_count,
                          instance_type,
                          transform_inputs,
                          description=NULL,
                          display_name=NULL,
                          # model arguments
                          image_uri=NULL,
                          predictor_cls=NULL,
                          env=NULL,
                          # transformer arguments
                          strategy=NULL,
                          assemble_with=NULL,
                          output_path=NULL,
                          output_kms_key=NULL,
                          accept=NULL,
                          max_concurrent_transforms=NULL,
                          max_payload=NULL,
                          tags=NULL,
                          volume_kms_key=NULL,
                          depends_on=NULL,
                          # step retry policies
                          repack_model_step_retry_policies=NULL,
                          model_step_retry_policies=NULL,
                          transform_step_retry_policies=NULL,
                          ...){
      steps = list()
      kwargs = list(...)
      if ("entry_point" %in% names(kwargs)){
        entry_point = kwargs[["entry_point"]]
        source_dir = kwargs[["source_dir"]]
        dependencies = kwargs[["dependencies"]]
        repack_model_step = .RepackModelStep$new(
          name=sprintf("%sRepackModel", name),
          depends_on=depends_on,
          retry_policies=repack_model_step_retry_policies,
          sagemaker_session=estimator$sagemaker_session,
          role=estimator$sagemaker_session,
          model_data=model_data,
          entry_point=entry_point,
          source_dir=source_dir,
          dependencies=dependencies,
          tags=tags,
          subnets=estimator.subnets,
          security_group_ids=estimator$security_group_ids,
          description=description,
          display_name=display_name
        )
        steps = list.append(steps, repack_model_step)
        model_data = repack_model_step$properties$ModelArtifacts$S3ModelArtifacts
      }
      predict_wrapper = function(endpoint, session){
        return(Predictor$new(endpoint, session))
      }
      predictor_cls = predictor_cls %||% predict_wrapper

      kwargs2 = c(
        image_uri=image_uri %||% estimator$training_image_uri(),
        model_data=model_data,
        predictor_cls=predictor_cls,
        vpc_config=None,
        sagemaker_session=estimator$sagemaker_session,
        role=estimator$role,
        kwargs
      )
      model = do.call(Model$new, kwargs2)

      model_step = CreateModelStep$new(
        name=sprintf("%sCreateModelStep", name),
        model=model,
        inputs=model_inputs,
        description=description,
        display_name=display_name,
        retry_policies=model_step_retry_policies
      )
      if (!("entry_point" %in% names(kwargs)) && !is.null(depends_on)){
        # if the CreateModelStep is the first step in the collection
        model_step$add_depends_on(depends_on)
      }
      steps = list.append(steps, model_step)

      transformer = Transformer$new(
        model_name=model_step$properties$ModelName,
        instance_count=instance_count,
        instance_type=instance_type,
        strategy=strategy,
        assemble_with=assemble_with,
        output_path=output_path,
        output_kms_key=output_kms_key,
        accept=accept,
        max_concurrent_transforms=max_concurrent_transforms,
        max_payload=max_payload,
        env=env,
        tags=tags,
        base_transform_job_name=name,
        volume_kms_key=volume_kms_key,
        sagemaker_session=estimator$sagemaker_session
      )
      transform_step = TransformStep$new(
        name=sprintf("%sTransformStep",name),
        transformer=transformer,
        inputs=transform_inputs,
        description=description,
        display_name=display_name,
        retry_policies=transform_step_retry_policies
      )
      steps = list.append(steps, transform_step)

      self$steps = steps
    }
  ),
  lock_objects = F
)
