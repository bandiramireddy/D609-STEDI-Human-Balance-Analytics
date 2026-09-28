CREATE EXTERNAL TABLE stedi.customer_landing (
    serialnumber string,
    sharewithpublicasofdate string,
    birthday string,
    registrationdate string,
    sharewithresearchasofdate string,
    customername string,
    email string,
    lastupdatedate string,
    phone string
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://stedi-human-balance-analytics12/customer_landing/';
