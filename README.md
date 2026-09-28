# D609 – STEDI Human Balance Analytics

This project implements a data engineering and analytics pipeline for STEDI Human Balance data using Amazon S3, AWS Glue, AWS Glue Data Catalog, AWS Glue Studio and Amazon Athena.

## Landing Zone

| Dataset | Records |
|---|---:|
| customer_landing | 956 |
| accelerometer_landing | 81,273 |
| step_trainer_landing | 28,680 |

## Trusted Zone

| Dataset | Records |
|---|---:|
| customer_trusted | 482 |
| accelerometer_trusted | 40,981 |
| step_trainer_trusted | 14,460 |

## Curated Zone

| Dataset | Records |
|---|---:|
| customer_curated | 482 |
| machine_learning_curated | 43,681 |

## AWS Services

- Amazon S3
- AWS Glue
- AWS Glue Data Catalog
- AWS Glue Studio
- Amazon Athena

## Repository Structure

glue/ contains the five Glue Python scripts.

sql/ contains the three landing-zone SQL DDL scripts.

README.md contains project documentation.

## Pipeline Summary

1. Customer landing data is filtered using research consent.
2. Accelerometer data is joined with trusted customers using email.
3. Customer curated data is created from trusted customer and accelerometer data.
4. Step Trainer data is joined with customer curated data using serial number.
5. Step Trainer and accelerometer data are joined using the sensor reading timestamp.
6. The final machine-learning curated dataset is prepared for downstream analytics.
