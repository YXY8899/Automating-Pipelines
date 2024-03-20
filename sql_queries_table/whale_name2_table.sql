CREATE TABLE WHALE_NAME_2 (
  WHALE_ID INT PRIMARY KEY,
  WHALE_NAME STRING
) WITH (
  KAFKA_TOPIC='whale_names_farm',
  KEY_FORMAT='DELIMITED',
  VALUE_FORMAT='DELIMITED'
);