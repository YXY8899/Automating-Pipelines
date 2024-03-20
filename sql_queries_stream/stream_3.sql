CREATE STREAM BIRD_STREAM_3 (
  id INT,
  farm STRING,
  bird_id INT,
  longitude DOUBLE,
  latitude DOUBLE,
  date_time STRING,
  file_path STRING
) WITH (
  KAFKA_TOPIC='birds',
  VALUE_FORMAT='AVRO'
);