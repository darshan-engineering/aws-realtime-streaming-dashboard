##############################################
# --- DynamoDB: connections --- #
##############################################

# Tracks active WebSocket connection IDs.
# stream-processor scans this to know which clients to push updates to.
resource "aws_dynamodb_table" "connections" {
  name         = "connections"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "connection_id"

  attribute {
    name = "connection_id"
    type = "S"
  }
}

##############################################
# --- DynamoDB: stream-state --- #
##############################################

# Stores the latest state of each active order.
# Keyed by entity_id (order ID). Queried on initial dashboard load.
resource "aws_dynamodb_table" "stream_state" {
  name         = "stream-state"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "entity_id"

  attribute {
    name = "entity_id"
    type = "S"
  }
}
