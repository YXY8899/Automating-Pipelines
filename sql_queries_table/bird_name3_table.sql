CREATE TABLE BIRD_NAME_3 (
  BIRD_ID INT PRIMARY KEY,
  BIRD_NAME STRING
) WITH (
  KAFKA_TOPIC='bird_names_farm',
  KEY_FORMAT='DELIMITED',
  VALUE_FORMAT='DELIMITED'
);