resource "aws_glue_catalog_database" "lakehouse" {
  name        = "lakehouse_db"
  description = "Glue Data Catalog database for lakehouse Delta tables."
}

resource "aws_athena_workgroup" "lakehouse" {
  name = "lakehouse-${var.environment}"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${var.logs_bucket_name}/athena-results/"

      encryption_configuration {
        encryption_option = "SSE_KMS"
        kms_key           = var.kms_key_arn
      }
    }
  }
}

resource "aws_athena_named_query" "products_sanity" {
  name      = "lakehouse-products-sanity-check"
  workgroup = aws_athena_workgroup.lakehouse.id
  database  = aws_glue_catalog_database.lakehouse.name
  query     = <<-SQL
    SELECT
      COUNT(*)                                            AS total_products,
      COUNT(DISTINCT product_id)                          AS unique_products,
      SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END) AS null_pks,
      COUNT(DISTINCT department)                          AS department_count
    FROM lakehouse_db.products;
  SQL
}

resource "aws_athena_named_query" "orders_sanity" {
  name      = "lakehouse-orders-sanity-check"
  workgroup = aws_athena_workgroup.lakehouse.id
  database  = aws_glue_catalog_database.lakehouse.name
  query     = <<-SQL
    SELECT
      COUNT(*)                                              AS total_orders,
      COUNT(DISTINCT order_id)                              AS unique_orders,
      SUM(CASE WHEN total_amount <= 0 THEN 1 ELSE 0 END)   AS invalid_amounts,
      MIN(order_timestamp)                                  AS earliest_order,
      MAX(order_timestamp)                                  AS latest_order
    FROM lakehouse_db.orders;
  SQL
}

resource "aws_athena_named_query" "referential_integrity" {
  name      = "lakehouse-referential-integrity-check"
  workgroup = aws_athena_workgroup.lakehouse.id
  database  = aws_glue_catalog_database.lakehouse.name
  query     = <<-SQL
    -- Finds order_items rows whose order_id has no matching order.
    SELECT COUNT(*) AS orphaned_order_items
    FROM lakehouse_db.order_items oi
    LEFT JOIN lakehouse_db.orders o ON oi.order_id = o.order_id
    WHERE o.order_id IS NULL;
  SQL
}
