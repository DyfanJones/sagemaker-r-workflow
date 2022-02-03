src_botocore_dir = ./vendor/botocore/botocore/data
botocore_sagemaker = sagemaker/2017-07-24
botocore_emr = emr/2009-03-31

dest_data = ./data-raw

src_sagemaker_dir = ./vendor/sagemaker/src/sagemaker/workflow/_repack_model.py
dest_sagemaker_dir = ./inst/sagemaker/workflow

get-botocore-service:
	@git submodule init
	@git submodule update --remote vendor/botocore
	@mkdir -p $(dest_data)/emr
	@mkdir -p $(dest_data)/sagemaker
	@cp -R $(src_botocore_dir)/$(botocore_sagemaker)/service-2.json $(dest_data)/emr
	@cp -R $(src_botocore_dir)/$(botocore_emr)/service-2.json $(dest_data)/sagemaker
	@Rscript $(dest_data)/compress_data.R

get-sagemaker-repack:
	@git submodule init
	@git submodule update --remote vendor/sagemaker
	@mkdir -p $(dest_sagemaker_dir)
	@cp -R $(src_sagemaker_dir) $(dest_sagemaker_dir)
