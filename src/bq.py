from google.cloud import bigquery 

def build_schema(cast_fields):
    schema = []
    
    for c in cast_fields.keys():
        if cast_fields[c] == 'date':
            schema.append( bigquery.SchemaField(c, bigquery.enums.SqlTypeNames.DATE))
        if cast_fields[c] == 'string':
            schema.append( bigquery.SchemaField(c, bigquery.enums.SqlTypeNames.STRING))
    
    print(schema)

    return schema

def data_to_bq(client, data, bq_project, bq_dataset, bq_table, cast_cols, replace=False):

    print(data.dtypes)

    dataset_ref = client.dataset(bq_dataset, project=bq_project)
    table_ref = dataset_ref.table(bq_table)

    job_config = bigquery.LoadJobConfig(
        autodetect=False,
        source_format=bigquery.SourceFormat.CSV
    )

    job_config.schema = build_schema(cast_cols)

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
