output "websocket_url" {
  description = "WebSocket URL — set as WS_URL in frontend/dashboard.html"
  value       = aws_apigatewayv2_stage.websocket.invoke_url
}

output "rest_api_url" {
  description = "REST API base URL — set as REST_URL in frontend/dashboard.html (append /state)"
  value       = aws_apigatewayv2_stage.rest.invoke_url
}

output "kinesis_stream_name" {
  description = "Kinesis stream name — used in producer.py"
  value       = aws_kinesis_stream.dashboard.name
}
