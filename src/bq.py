from google.cloud import bigquery 

def build_schema():
    job_config = bigquery.LoadJobConfig(
                schema=[
                    bigquery.SchemaField("id", bigquery.enums.SqlTypeNames.STRING),
                    bigquery.SchemaField("committeeid", bigquery.enums.SqlTypeNames.STRING),
                    # bigquery.SchemaField("fileddocid", bigquery.enums.SqlTypeNames.STRING),
                    bigquery.SchemaField("amount", bigquery.enums.SqlTypeNames.FLOAT64),
                ],
            )
    
    return job_config

def data_to_bq(client, data, bq_project, bq_dataset, bq_table, schema=None, replace=False):

    dataset_ref = client.dataset(bq_dataset, project=bq_project)
    table_ref = dataset_ref.table(bq_table)

    job_config = bigquery.LoadJobConfig()

    if schema is None:
        job_config.autodetect = True
    else:
        job_config.schema = schema

    if replace:
        try:
            client.get_table(table_ref)
        except Exception as e:
            print('New table, reason:', e)
        else:
            client.delete_table(table_ref)

    table = bigquery.Table(table_ref)

    job = client.load_table_from_dataframe(data, table, location="US")

    job.result()

    assert job.state == "DONE"
