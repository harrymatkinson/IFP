# script to create our engine and create the tables if they don't exist

from models import Base
import sqlalchemy
import pandas as pd
import uuid

def upsert_df(df: pd.DataFrame, table_name: str, engine: sqlalchemy.engine.Engine, dtype: dict):
    """Implements the equivalent of pd.DataFrame.to_sql(..., if_exists='update')
    (which does not exist). Creates or updates the db records based on the
    dataframe records.
    Conflicts to determine update are based on the dataframes index.
    This will set unique keys constraint on the table equal to the index names
    1. Create a temp table from the dataframe
    2. Insert/update from temp table into table_name
    Returns: True if successful
    """

    # If the table does not exist, we should just use to_sql to create it
    if not engine.execute(
        sqlalchemy.text(f"""SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE  table_schema = 'public'
            AND    table_name   = '{table_name}');
            """)
    ).first()[0]:
        df.to_sql(table_name, engine)
        return True

    # If it already exists...
    temp_table_name = f"temp_{uuid.uuid4().hex[:6]}"
    df.to_sql(temp_table_name, engine, index=True, dtype=dtype)

    index = list(df.index.names)
    index_sql_txt = ", ".join([f'"{i}"' for i in index])
    columns = list(df.columns)
    headers = index + columns
    headers_sql_txt = ", ".join(
        [f'"{i}"' for i in headers]
    )  # index1, index2, ..., column 1, col2, ...

    # col1 = exluded.col1, col2=excluded.col2
    update_column_stmt = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in columns])

    # Compose and execute upsert query
    query_upsert = sqlalchemy.text(f"""
    INSERT INTO "{table_name}" ({headers_sql_txt}) 
    SELECT {headers_sql_txt} FROM "{temp_table_name}"
    ON CONFLICT ({index_sql_txt}) DO UPDATE 
    SET {update_column_stmt};
    """)
    engine.execute(query_upsert)
    engine.execute(sqlalchemy.text(f"DROP TABLE {temp_table_name}"))

    return True

bars_dtype = {
    "BAR_NAME":sqlalchemy.types.String(),
    "BAR_STOCK":sqlalchemy.types.JSON(),
}

drinks_dtype = {
    "DRINK_NAME":sqlalchemy.types.String(),
    "GLASS_NAME":sqlalchemy.types.String(),
}

glasses_dtype = {
    "GLASS_NAME":sqlalchemy.types.String(),
    "STOCK":sqlalchemy.types.Integer(),
}

transactions_dtype = {
    "TRANS_ID":sqlalchemy.types.Integer(),
    "BAR_NAME":sqlalchemy.types.String(),
    "DRINK_NAME":sqlalchemy.types.String(),
    "VALUE":sqlalchemy.types.Float(),
    "TRANS_TIME":sqlalchemy.types.DateTime(timezone=True),
}

# I used the free tier of AWS to create my DB as it was convenient within the time restriction
# having the user and pw right in the script is obviously not ideal
# but I didn't have time to explore alternatives
path="postgresql://hmatkinson:c0cktail8ar@harry-atkinson-ifp-postgresql.c49trazajk8o.eu-west-2.rds.amazonaws.com:5432/ifpCocktails"
engine = sqlalchemy.create_engine(path, echo=True)

# run this script once to create the DB tables
if __name__ == "__main__":
    Base.metadata.create_all(engine)