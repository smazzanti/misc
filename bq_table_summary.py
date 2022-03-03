

def bq_table_summary(table_name, bq_client, len_topcounts=10):
  import pandas as pd
  import jinja2
      
  # column names

  if " " in table_name:
      query_limit_1 = f"SELECT * FROM ({table_name}) LIMIT 1"
      field_names = bq_client.query(query_limit_1).to_dataframe().columns.to_list()
  else:
      schema = bq_client.get_table(table_name).schema
      field_names = [field.name for field in schema]

  # number of rows

  query_nrows = f"SELECT COUNT(*) FROM {table_name}"
  output_nrows = pd.Series(bq_client.query(query_nrows).to_dataframe().iloc[0, 0], index=field_names, name="nrows")

  # number of distinct values

  jinja_query_distinct = """
  {% set COLUMNS = field_names%}
  SELECT
    {% for COLUMN in COLUMNS -%}
    ROUND(COUNT(DISTINCT {{COLUMN}}) / COUNT(*)) AS {{COLUMN}}{% if not loop.last %},{% endif %}
    {% endfor -%}
  FROM 
    table_name
  """ \
      .replace('field_names', str(field_names)) \
      .replace('table_name', table_name)

  output_distinct = bq_client.query(query=jinja2.Template(jinja_query_distinct).render()).to_dataframe().iloc[0, :]
  output_distinct.name = "distinct"

  # number of nulls

  jinja_query_nulls = """
  {% set COLUMNS = field_names%}
  SELECT
    {% for COLUMN in COLUMNS -%}
    ROUND(COUNTIF({{COLUMN}} IS NULL) / COUNT(*), 2) AS {{COLUMN}}{% if not loop.last %},{% endif %}
    {% endfor -%}
  FROM 
    table_name
  """ \
      .replace('field_names', str(field_names)) \
      .replace('table_name', table_name)

  output_null = bq_client.query(query=jinja2.Template(jinja_query_nulls).render()).to_dataframe().iloc[0, :]
  output_null.name = "null"

  # top counts

  jinja_query_topcount = """
  {% set COLUMNS = field_names%}
  SELECT
    {% for COLUMN in COLUMNS -%}
    APPROX_TOP_COUNT({{COLUMN}}, len_topcounts) AS {{COLUMN}}{% if not loop.last %},{% endif %}
    {% endfor -%}
  FROM 
    table_name
  """ \
      .replace('field_names', str(field_names)) \
      .replace('table_name', table_name) \
      .replace('len_topcounts', str(len_topcounts))

  output_topcount = bq_client.query(query=jinja2.Template(jinja_query_topcount).render()).to_dataframe().iloc[0, :]

  def format_topcount(output_topcount_row):

      l = [(el["value"], round(el["count"] / output_nrows.iloc[0], 2)) for el in output_topcount_row]
      fl = [item for sublist in l for item in sublist]
      fl += [""] * (len_topcounts * 2 - len(fl))

      i = [(f"value_{i}", f"count_{i}") for i in range(1,len_topcounts+1)]
      fi = [item for sublist in i for item in sublist]

      return pd.Series(fl, index=fi)

  output_topcount = output_topcount.apply(format_topcount)

  # put all statistics together

  output_full = pd.concat([
      output_nrows,
      output_distinct,
      output_null,
      output_topcount
  ], axis = 1)

  return output_full
