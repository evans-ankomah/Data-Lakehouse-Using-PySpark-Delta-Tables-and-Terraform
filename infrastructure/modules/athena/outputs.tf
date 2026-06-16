output "workgroup_name" {
  value = aws_athena_workgroup.lakehouse.name
}

output "glue_database_name" {
  value = aws_glue_catalog_database.lakehouse.name
}
