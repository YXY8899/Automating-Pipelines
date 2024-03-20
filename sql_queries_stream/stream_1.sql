CREATE STREAM SHEEP_STREAM_1 (
  id INT,
  farm STRING,
  sheep_id INT,
  longitude DOUBLE,
  latitude DOUBLE,
  date_time STRING,
  file_path STRING
) WITH (
  KAFKA_TOPIC='sheeps',
  VALUE_FORMAT='AVRO'
);