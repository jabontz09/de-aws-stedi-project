# de-aws-stedi-project

### Glue Job Scripts

- [customer_landing_to_trusted.py](customer_landing_to_trusted.py)
- [accelerometer_landing_to_trusted.py](accelerometer_landing_to_trusted.py)
- [step_trainer_trusted.py](step_trainer_trusted.py)
- [customer_trusted_to_curated.py](customer_trusted_to_curated.py)
- [machine_learning_curated.py](machine_learning_curated.py)

### Glue Catalog Table DDL Scripts

- [customer_landing.sql](glue_catalog_ddl_scripts\customer_landing.sql)
- [accelerometer_landing.sql](glue_catalog_ddl_scripts\accelerometer_landing.sql)
- [step_trainer_landing.sql](glue_catalog_ddl_scripts\step_trainer_landing.sql)

### Athena Screenshots

- Count of customer_landing
  ![customer_landing_cnt](athena_screenshots\landing\customer_landing_cnt.png)
- The customer_landing data contains multiple rows with a blank shareWithResearchAsOfDate.
  ![customer_landing_no_sharewithresearch](athena_screenshots\landing\customer_landing_no_sharewithresearchasofdate.png)
- count of accelerometer_landing
  ![accelerometer_landing_cnt](athena_screenshots\landing\accelerometer_landing_cnt.png)
- Count of customer_trusted. The resulting customer_trusted data also has no rows where shareWithResearchAsOfDate is blank.
  ![customer_trusted_cnt](athena_screenshots\trusted\customer_trusted_cnt.png)
- Count of accelerometer_trusted
  ![accelerometer_trusted_cnt](athena_screenshots\trusted\accelerometer_trusted_cnt.png)
- Count of step_trainer_trusted
  ![step_trainer_trusted_cnt](athena_screenshots\trusted\step_trainer_trusted_cnt.png)
- Count of customer_curated
  ![customer_curated_cnt](athena_screenshots\curated\customer_curated_cnt.png)
- Count of machine_learning_curated![machine_learning_curated_cnt](athena_screenshots\curated\machine_learning_curated_cnt.png)
