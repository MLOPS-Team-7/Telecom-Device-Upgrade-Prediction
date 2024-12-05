DECLARE query_string STRING;
DECLARE table_list STRING;

-- Step 1: Get the list of table names
SET table_list = (
  SELECT STRING_AGG(
    CONCAT(
      'SELECT *, ',
      'SAFE.PARSE_TIMESTAMP("%Y_%m_%dT%H_%M_%S", ',
      'REGEXP_EXTRACT("', table_name, '", r"_(\\d{4}_\\d{2}_\\d{2}T\\d{2}_\\d{2}_\\d{2})")) AS prediction_date, ',
      -- Add the logic to determine the FinalPredictedChurnClass
      'CASE ',
      'WHEN ARRAY_LENGTH(predicted_Churn.scores) = 2 AND predicted_Churn.scores[ORDINAL(1)] > predicted_Churn.scores[ORDINAL(2)] THEN 0 ',
      'WHEN ARRAY_LENGTH(predicted_Churn.scores) = 2 AND predicted_Churn.scores[ORDINAL(2)] > predicted_Churn.scores[ORDINAL(1)] THEN 1 ',
      'ELSE NULL END AS FinalPredictedChurnClass ',
      'FROM `axial-rigging-438817-h4.Big_query_batch_prediction.', table_name, '`'),
    ' UNION ALL '
  )
  FROM `axial-rigging-438817-h4.Big_query_batch_prediction.INFORMATION_SCHEMA.TABLES`
  WHERE table_name LIKE 'predictions_%'
);

-- Step 2: Create the dynamic query string to create the view
SET query_string = CONCAT(
  'CREATE OR REPLACE VIEW `axial-rigging-438817-h4.Big_query_batch_prediction.latest_predictions_view` AS ',
  table_list
);

-- Step 3: Execute the dynamic query to create the view
EXECUTE IMMEDIATE query_string;
