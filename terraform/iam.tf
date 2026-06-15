##############################################
# --- Shared: Lambda Trust Policy --- #
##############################################

data "aws_iam_policy_document" "lambda_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

##############################################
# --- Role: stream-processor-role --- #
##############################################

# Needs: kinesis (read stream), dynamodb (read/write both tables),
# execute-api:ManageConnections (push to WebSocket clients).
resource "aws_iam_role" "stream_processor" {
  name               = "stream-processor-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
}

resource "aws_iam_role_policy_attachment" "stream_processor_logs" {
  role       = aws_iam_role.stream_processor.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "stream_processor_policy" {
  name = "stream-processor-policy"
  role = aws_iam_role.stream_processor.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ReadKinesis"
        Effect = "Allow"
        Action = [
          "kinesis:GetRecords",
          "kinesis:GetShardIterator",
          "kinesis:DescribeStream",
          "kinesis:ListShards"
        ]
        Resource = aws_kinesis_stream.dashboard.arn
      },
      {
        Sid      = "ListKinesisStreams"
        Effect   = "Allow"
        Action   = "kinesis:ListStreams"
        Resource = "*"
      },
      {
        Sid    = "ReadWriteDynamoDB"
        Effect = "Allow"
        Action = [
          "dynamodb:PutItem",
          "dynamodb:Scan",
          "dynamodb:DeleteItem"
        ]
        Resource = [
          aws_dynamodb_table.stream_state.arn,
          aws_dynamodb_table.connections.arn
        ]
      },
      {
        Sid      = "PostToWebSocket"
        Effect   = "Allow"
        Action   = "execute-api:ManageConnections"
        Resource = "arn:aws:execute-api:${var.aws_region}:*:${aws_apigatewayv2_api.websocket.id}/*/@connections/*"
      }
    ]
  })
}

##############################################
# --- Role: ws-connect-role --- #
##############################################

resource "aws_iam_role" "ws_connect" {
  name               = "ws-connect-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
}

resource "aws_iam_role_policy_attachment" "ws_connect_logs" {
  role       = aws_iam_role.ws_connect.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "ws_connect_policy" {
  name = "ws-connect-policy"
  role = aws_iam_role.ws_connect.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid      = "SaveConnection"
      Effect   = "Allow"
      Action   = "dynamodb:PutItem"
      Resource = aws_dynamodb_table.connections.arn
    }]
  })
}

##############################################
# --- Role: ws-disconnect-role --- #
##############################################

resource "aws_iam_role" "ws_disconnect" {
  name               = "ws-disconnect-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
}

resource "aws_iam_role_policy_attachment" "ws_disconnect_logs" {
  role       = aws_iam_role.ws_disconnect.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "ws_disconnect_policy" {
  name = "ws-disconnect-policy"
  role = aws_iam_role.ws_disconnect.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid      = "DeleteConnection"
      Effect   = "Allow"
      Action   = "dynamodb:DeleteItem"
      Resource = aws_dynamodb_table.connections.arn
    }]
  })
}

##############################################
# --- Role: get-state-role --- #
##############################################

resource "aws_iam_role" "get_state" {
  name               = "get-state-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
}

resource "aws_iam_role_policy_attachment" "get_state_logs" {
  role       = aws_iam_role.get_state.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "get_state_policy" {
  name = "get-state-policy"
  role = aws_iam_role.get_state.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid      = "ReadState"
      Effect   = "Allow"
      Action   = "dynamodb:Scan"
      Resource = aws_dynamodb_table.stream_state.arn
    }]
  })
}
