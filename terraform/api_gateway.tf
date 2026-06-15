##############################################
# --- WebSocket API Gateway --- #
##############################################

resource "aws_apigatewayv2_api" "websocket" {
  name                       = "dashboard-ws"
  protocol_type              = "WEBSOCKET"
  route_selection_expression = "$request.body.action"
}

resource "aws_apigatewayv2_stage" "websocket" {
  api_id      = aws_apigatewayv2_api.websocket.id
  name        = "production"
  auto_deploy = true
}

# $connect route — fires when a client opens a WebSocket connection
resource "aws_apigatewayv2_integration" "ws_connect" {
  api_id           = aws_apigatewayv2_api.websocket.id
  integration_type = "AWS_PROXY"
  integration_uri  = aws_lambda_function.ws_connect.invoke_arn
}

resource "aws_apigatewayv2_route" "ws_connect" {
  api_id    = aws_apigatewayv2_api.websocket.id
  route_key = "$connect"
  target    = "integrations/${aws_apigatewayv2_integration.ws_connect.id}"
}

# $disconnect route — fires when a client closes the connection
resource "aws_apigatewayv2_integration" "ws_disconnect" {
  api_id           = aws_apigatewayv2_api.websocket.id
  integration_type = "AWS_PROXY"
  integration_uri  = aws_lambda_function.ws_disconnect.invoke_arn
}

resource "aws_apigatewayv2_route" "ws_disconnect" {
  api_id    = aws_apigatewayv2_api.websocket.id
  route_key = "$disconnect"
  target    = "integrations/${aws_apigatewayv2_integration.ws_disconnect.id}"
}

##############################################
# --- REST HTTP API Gateway --- #
##############################################

resource "aws_apigatewayv2_api" "rest" {
  name          = "dashboard-rest"
  protocol_type = "HTTP"

  cors_configuration {
    allow_origins = ["*"]
    allow_methods = ["GET"]
    allow_headers = ["content-type"]
  }
}

resource "aws_apigatewayv2_stage" "rest" {
  api_id      = aws_apigatewayv2_api.rest.id
  name        = "$default"
  auto_deploy = true
}

resource "aws_apigatewayv2_integration" "get_state" {
  api_id                 = aws_apigatewayv2_api.rest.id
  integration_type       = "AWS_PROXY"
  integration_uri        = aws_lambda_function.get_state.invoke_arn
  payload_format_version = "2.0"
}

# GET /state — returns all active orders for initial dashboard load
resource "aws_apigatewayv2_route" "get_state" {
  api_id    = aws_apigatewayv2_api.rest.id
  route_key = "GET /state"
  target    = "integrations/${aws_apigatewayv2_integration.get_state.id}"
}
