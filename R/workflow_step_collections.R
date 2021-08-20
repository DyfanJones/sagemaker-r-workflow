# NOTE: This code has been modified from AWS Sagemaker Python:
# https://github.com/aws/sagemaker-python-sdk/blob/master/src/sagemaker/workflow/step_collections.py

#' @include workflow_entities.R
#' @include workflow_steps.R
#' @include workflow__utils.R
#' @include r_utils.R

#' @import R6
#' @import R6sagemaker.common
#' @import R6sagemaker.mlcore

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
    #' @param estimator : The estimator instance.
    #' @param model_data : The S3 uri to the model data from training.
    #' @param content_types (list): The supported MIME types for the input data (default: None).
    #' @param response_types (list): The supported MIME types for the output data (default: None).
    #' @param inference_instances (list): A list of the instance types that are used to
    #'              generate inferences in real-time (default: None).
    #' @param transform_instances (list): A list of the instance types on which a transformation
    #'              job can be run or on which an endpoint can be deployed (default: None).
    #' @param depends_on (List[str]): The list of step names the first step in the collection
    #'              depends on
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
    #' @param description (str): Model Package description (default: None).
    #' @param tags (List[dict[str, str]]): The list of tags to attach to the model package group. Note
    #'              that tags will only be applied to newly created model package groups; if the
    #'              name of an existing group is passed to "model_package_group_name",
    #'              tags will not be applied.
    #' @param model (object or Model): A PipelineModel object that comprises a list of models
    #'              which gets executed as a serial inference pipeline or a Model object.
    #' @param ... : additional arguments to `create_model`.
    initialize = function(name,
                          content_types,
                          response_types,
                          inference_instances,
                          transform_instances,
                          estimator=NULL,
                          model_data=NULL,
                          depends_on=NULL,
                          model_package_group_name=NULL,
                          model_metrics=NULL,
                          approval_status=NULL,
                          image_uri=NULL,
                          compile_model_family=NULL,
                          description=NULL,
                          tags=NULL,
                          model=NULL,
                          ...){
      steps=list()
      repack_model = FALSE
      self$model_list = NULL
      self$container_def_list = NULL
      kwargs = list(...)
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

        kwargs2 = c(name=sprintf("%sRepackModel",name),
                    depends_on=depends_on,
                    sagemaker_session=estimator$sagemaker_session,
                    role=estimator$role,
                    model_data=model_data,
                    entry_point=entry_point,
                    source_dir=source_dir,
                    dependencies=dependencies,
                    kwargs)
        repack_model_step = do.call(.RepackModelStep$new, kwargs2)
        steps = c(steps, repack_model_step)
        model_data = repack_model_step$properties$ModelArtifacts$S3ModelArtifacts

        # remove kwargs consumed by model repacking step
        kwargs[["output_kms_key"]] = NULL
      } else if (!is.null(model)){
        if (inherits(model, "PipelineModel")) {
          self.model_list = model.models
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
          if ("entry_point" %in% names(model_entity)){
            repack_model = TRUE
            entry_point = model_entity$entry_point
            source_dir = model_entity$source_dir
            dependencies = model_entity$dependencies
            kwargs = c(kwargs, output_kms_key=model_entity$model_kms_key)
            model_name = model_entity$name %||% model_entity$.__ $.framework_name

            kwargs2 = c(kwargs,
                        name=sprintf("%sRepackModel", model_name),
                        depends_on=depends_on,
                        sagemaker_session=sagemaker_session,
                        role=role,
                        model_data=model_entity.model_data,
                        entry_point=entry_point,
                        source_dir=source_dir,
                        dependencies=dependencies
                        )
            repack_model_step = do.call(.RepackModelStep$new, kwargs2)

            steps = c(steps, repack_model_step)
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
      kwargs2 = c(name=name,
                  estimator=estimator,
                  model_data=model_data,
                  content_types=content_types,
                  response_types=response_types,
                  inference_instances=inference_instances,
                  transform_instances=transform_instances,
                  model_package_group_name=model_package_group_name,
                  model_metrics=model_metrics,
                  approval_status=approval_status,
                  image_uri=image_uri,
                  compile_model_family=compile_model_family,
                  description=description,
                  tags=tags,
                  container_def_list=self$container_def_list,
                  kwargs)
      register_model_step = do.call(.RegisterModelStep$new, kwargs2)

      if (!isTRUE(repack_model))
        register_model_step$add_depends_on(depends_on)

      steps = c(steps, register_model_step)
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
    #'              First, if custom
    #'              model artifacts are required, a `_RepackModelStep` is included.
    #'              Second, a `CreateModelStep` with the model data passed in from
    #'              a training step or other training job output. Finally, a `TransformerStep`.
    #'              If repacking the model artifacts is not necessary, only the
    #'              CreateModelStep and TransformerStep are in the step collection.
    #' @param name (str): The name of the Transform Step.
    #' @param estimator : The estimator instance.
    #' @param model_data : The S3 uri to the model data from training.
    #' @param model_inputs : Placeholder
    #' @param instance_count (int): The number of EC2 instances to use.
    #' @param instance_type (str): The type of EC2 instance to use.
    #' @param transform_inputs : Placeholder
    #' @param image_uri : Placeholder
    #' @param predictor_cls : Placeholder
    #' @param env (dict): The Environment variables to be set for use during the
    #'              transform job (default: None).
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
    #' @param max_concurrent_transforms : Placeholder
    #' @param max_payload : Placeholder
    #' @param tags : Placeholder
    #' @param volume_kms_key : Placeholder
    #' @param depends_on (List[str]): The list of step names the first step in
    #'              the collection depends on
    #' @param ... : pass onto model class.
    initialize = function(name,
                          estimator,
                          model_data,
                          model_inputs,
                          instance_count,
                          instance_type,
                          transform_inputs,
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
          sagemaker_session=estimator$sagemaker_session,
          role=estimator$sagemaker_session,
          model_data=model_data,
          entry_point=entry_point,
          source_dir=source_dir,
          dependencies=dependencies
        )
        steps = c(steps, repack_model_step)
        model_data = repack_model_step$properties$ModelArtifacts$S3ModelArtifacts
      }
      predict_wrapper = function(endpoint, session){
        return(Predictor$new(endpoint, session))
      }
      predictor_cls = predictor_cls %||% predict_wrapper

      kwargs2 = c(
        kwargs,
        image_uri=image_uri %||% estimator$training_image_uri(),
        model_data=model_data,
        predictor_cls=predictor_cls,
        vpc_config=NULL,
        sagemaker_session=estimator.sagemaker_session,
        role=estimator$role
      )
      model = do.call(Model$new, kwargs2)

      model_step = CreateModelStep$new(
        name = sprintf("%sCreateModelStep", name),
        model = model_data,
        inputs = model_inputs
      )
      if (!("entry_point" %in% names(kwargs)) && !is.null(depends_on)){
        # if the CreateModelStep is the first step in the collection
        model_step$add_depends_on(depends_on)
      }
      steps = c(steps, model_step)

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
        name=sprintf("%sTransformStep", name),
        transformer=transformer,
        inputs=transform_inputs
      )
      steps = c(steps, transform_step)

      self$steps = steps
    }
  ),
  lock_objects = F
)
