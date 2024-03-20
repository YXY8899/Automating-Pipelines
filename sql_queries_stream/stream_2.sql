CREATE STREAM WHALE_STREAM_2 (
  id INT,
  farm STRING,
  whale_id INT,
  longitude DOUBLE,
  latitude DOUBLE,
  date_time STRING,
  file_path STRING
) WITH (
  KAFKA_TOPIC='whales',
  VALUE_FORMAT='AVRO'
);