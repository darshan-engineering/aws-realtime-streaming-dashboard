##############################################
# --- Lambda: ws-connect --- #
##############################################

# Fires on WebSocket $connect. Saves connection ID to DynamoDB.
resource "aws_lambda_function" "ws_connect" {
  function_name    = "ws-connect"
  role             = aws_iam_role.ws_connect.arn
  filename         = "lambda/ws_connect.zip"
  handler          = "ws_connect.lambda_handler"
  runtime          = "python3.12"
  source_code_hash = filebase64sha256("lambda/ws_connect.zip")
}

resource "aws_lambda_permission" "ws_connect" {
  statement_id  = "AllowWebSocketConnect"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ws_connect.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.websocket.execution_arn}/*/*"
}

##############################################
# --- Lambda: ws-disconnect --- #
##############################################

# Fires on WebSocket $disconnect. Removes connection ID from DynamoDB.
resource "aws_lambda_function" "ws_disconnect" {
  function_name    = "ws-disconnect"
  role             = aws_iam_role.ws_disconnect.arn
  filename         = "lambda/ws_disconnect.zip"
  handler          = "ws_disconnect.lambda_handler"
  runtime          = "python3.12"
  source_code_hash = filebase64sha256("lambda/ws_disconnect.zip")
}

resource "aws_lambda_permission" "ws_disconnect" {
  statement_id  = "AllowWebSocketDisconnect"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ws_disconnect.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.websocket.execution_arn}/*/*"
}

##############################################
# --- Lambda: stream-processor --- #
##############################################

# Triggered by Kinesis. Processes order events, writes to DynamoDB,
# pushes updates to all connected WebSocket clients.
resource "aws_lambda_function" "stream_processor" {
  function_name    = "stream-processor"
  role             = aws_iam_role.stream_processor.arn
  filename         = "lambda/stream_processor.zip"
  handler          = "stream_processor.lambda_handler"
  runtime          = "python3.12"
  source_code_hash = filebase64sha256("lambda/stream_processor.zip")
  timeout          = 60

  environment {
    variables = {
      WEBSOCKET_API_ID = aws_apigatewayv2_api.websocket.id
      WEBSOCKET_STAGE  = "production"
    }
  }
}

# Kinesis event source mapping — triggers stream-processor on new records.
# Batch size 10, starting from Latest (only new records, not historical).
resource "aws_lambda_event_source_mapping" "kinesis_trigger" {
  event_source_arn  = aws_kinesis_stream.dashboard.arn
  function_name     = aws_lambda_function.stream_processor.arn
  starting_position = "LATEST"
  batch_size        = 10
}

##############################################
# --- Lambda: get-state --- #
##############################################

# REST endpoint for initial dashboard load.
# Scans stream-state table and returns all active orders.
resource "aws_lambda_function" "get_state" {
  function_name    = "get-state"
  role             = aws_iam_role.get_state.arn
  filename         = "lambda/get_state.zip"
  handler          = "get_state.lambda_handler"
  runtime          = "python3.12"
  source_code_hash = filebase64sha256("lambda/get_state.zip")
}

resource "aws_lambda_permission" "get_state" {
  statement_id  = "AllowRESTAPIInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.get_state.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.rest.execution_arn}/*/*"
}
