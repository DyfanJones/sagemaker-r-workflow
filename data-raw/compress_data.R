# read --------------------------------------------------------------------
emr_service = jsonlite::fromJSON("data-raw/emr/service-2.json")[["shapes"]]
sagemaker_service = jsonlite::fromJSON("data-raw/sagemaker/service-2.json")[["shapes"]]

# Save --------------------------------------------------------------------
usethis::use_data(
  emr_service,
  sagemaker_service,
  overwrite = TRUE,
  internal = TRUE
)
