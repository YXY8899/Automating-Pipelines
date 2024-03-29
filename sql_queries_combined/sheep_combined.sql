CREATE STREAM sheep_combined AS
SELECT SHEEP_STREAM_1.ID, SHEEP_STREAM_1.FARM, SHEEP_STREAM_1.SHEEP_ID, SHEEP_STREAM_1.LONGITUDE, SHEEP_STREAM_1.LATITUDE, SHEEP_STREAM_1.DATE_TIME, SHEEP_STREAM_1.FILE_PATH, SHEEP_NAME_1.SHEEP_ID AS SHEEP_ID, SHEEP_NAME_1.SHEEP_NAME 
FROM  SHEEP_STREAM_1
JOIN  SHEEP_NAME_1
ON SHEEP_STREAM_1.SHEEP_ID = SHEEP_NAME_1.SHEEP_ID
EMIT CHANGES;