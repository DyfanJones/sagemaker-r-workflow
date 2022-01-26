src_dir = vendor/botocore/botocore/data/sagemaker/2017-07-24/service-2.json
dest_dir = ./inst/sagemaker/2017-07-24

get-sagemaker-service:
	@git submodule init
	@git submodule update --remote
	@mkdir -p $(dest_dir)
	@cp -R $(src_dir) $(dest_dir)
