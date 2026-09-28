CREATE EXTERNAL TABLE stedi.step_trainer_landing (
    sensorreadingtime bigint,
    serialnumber string,
    distancefromobject double
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://stedi-human-balance-analytics12/step_trainer_landing/';
