from pyspark.sql import SparkSession


def create_spark_session():
        spark = SparkSession.builder.appName('RetailEventsApp').getOrCreate()
        return spark


def process_invoices_data(spark, input_data, output_data):

    # get filepath to purchase_events data File
    invoices_data = input_data + 'purchase_events/*.json'

    # read invoices data file
    invoices_df = spark.read.options(header='True', multiLine=True).json(invoices_data)

    # Rename invoice column to invoiceno
    invoices_renamed_df = invoices_df.withColumnRenamed("invoice","invoiceno")

    # extract columns to create invoices table
    invoices_columns = ['customerid', 'invoiceno', 'invoicedate', 'country']
    invoices_table = invoices_renamed_df.select(invoices_columns)

    # write invoices table to parquet files partitioned by invoicedate and country
    output_location = output_data + 'invoices_table_parquet'
    invoices_table.write.partitionBy("invoicedate","country").parquet(output_location, mode='overwrite')


def process_items_data(spark, input_data, output_data):

    # get filepath to purchase_events data File
    input_data = input_data
    items_data = input_data + "purchase_events/*.json"

    # read invoices data file
    items_df = spark.read.options(header='True', multiLine=True).json(items_data)

    # Rename invoice column to invoiceno
    items_renamed_df = items_df.withColumnRenamed("invoice","invoiceno")

    # extract columns to create invoices table
    items_columns = ['stockcode', 'description', 'invoiceno', 'quantity', 'country']
    items_table = items_renamed_df.select(items_columns)

    # write invoices table to parquet files partitioned by invoicedate and country
    output_location = output_data + 'items_table_parquet'
    items_table.write.partitionBy("country").parquet(output_location, mode='overwrite')


def main():
    spark = create_spark_session()
    input_data = 's3://retail-events-bkt/'
    output_data = 's3://retail-events-bkt/analytics/'

    process_invoices_data(spark, input_data, output_data)
    process_items_data(spark, input_data, output_data)

if __name__ == '__main__':
    main()