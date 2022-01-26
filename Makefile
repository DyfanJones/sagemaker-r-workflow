src_botocore_dir = vendor/botocore/botocore/data/sagemaker/2017-07-24/service-2.json
dest_botocore_dir = ./inst/sagemaker/2017-07-24

src_sagemaker_dir = vendor/sagemaker/src/sagemaker/workflow/_repack_model.py
dest_sagemaker_dir = ./inst/sagemaker/workflow

get-sagemaker-service:
	@git submodule init
	@git submodule update --remote vendor/botocore
	@mkdir -p $(dest_botocore_dir)
	@cp -R $(src_botocore_dir) $(dest_botocore_dir)

get-sagemaker-repack:
	@git submodule init
	@git submodule update --remote vendor/sagemaker
	@mkdir -p $(dest_sagemaker_dir)
	@cp -R $(src_sagemaker_dir) $(dest_sagemaker_dir)
