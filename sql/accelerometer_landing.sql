CREATE EXTERNAL TABLE stedi.accelerometer_landing (
    timestamp bigint,
    user string,
    x double,
    y double,
    z double
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://stedi-human-balance-analytics12/accelerometer_landing/';
