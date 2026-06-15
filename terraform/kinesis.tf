##############################################
# --- Kinesis Data Stream --- #
##############################################

# On-demand capacity — scales automatically with event volume.
# Partition key = order_id ensures all events for the same order
# go to the same shard, preserving per-order event ordering.
resource "aws_kinesis_stream" "dashboard" {
  name             = "dashboard-stream"
  stream_mode_details {
    stream_mode = "ON_DEMAND"
  }
}
