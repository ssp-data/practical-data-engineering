"test creating delta table"

from deltalake import write_deltalake, DeltaTable
import pandas as pd
import os
import json

MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "miniostorage")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://127.0.0.1:9000")

storage_options = {
    "AWS_ACCESS_KEY_ID": MINIO_ACCESS_KEY,
    "AWS_SECRET_ACCESS_KEY": MINIO_SECRET_KEY,
    "AWS_ENDPOINT_URL": MINIO_ENDPOINT,
    "AWS_ALLOW_HTTP": "true",
    # "AWS_REGION": AWS_REGION, #do not use
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
}

s3_path_property = "s3a://real-estate/test/property"

def read_property_test_delta_table():
    # read a special version
    # dt = DeltaTable("s3a://real-estate/test/property", version=2)
    return DeltaTable(s3_path_property, storage_options=storage_options)

def test_create_test_delta_table():
    with open("property.json", "r") as file:
        json_data = json.load(file)

    df = pd.json_normalize(json_data)

    write_deltalake(
        s3_path_property,
        df,
        storage_options=storage_options,
        mode="overwrite",
    )

    #read and check
    dt = read_property_test_delta_table()
    schema = dt.schema().to_pyarrow()
    print(f"Schema: {schema}")

    print(f"Version: {dt.version()}")
    print(f"Files: {dt.files()}")

    dataset = dt.to_pyarrow_dataset()
    # condition = (ds.field("year") == "2024") & (ds.field("value") > "4")
    # dataset.to_table(filter=condition, columns=["value"]).to_pandas()
    result_df = dataset.to_table().to_pandas()

    #check df is not empty
    assert not result_df.empty, "DataFrame is unexpectedly empty"



def test_create_test_delta_table_dummy_df():
    df = pd.DataFrame({"x": [1, 2, 3]})
    write_deltalake(
        "s3a://real-estate/test/test1",
        df,
        storage_options=storage_options,
        mode="overwrite",
    )
    print("wrote delta table test1")
    assert True

# def flatten_json(json_data):
#     def flatten_json_pandas(df, remove_columns=[]):
#         # Check each column to see if it needs flattening or exploding
#         for col_name in df.columns:
#             # Skip removal columns
#             if col_name in remove_columns:
#                 df = df.drop(columns=[col_name])
#                 continue

#             # Flatten if the column type is dict (similar to StructType)
#             if isinstance(df[col_name].iloc[0], dict):
#                 # Flatten and merge with original DataFrame
#                 flattened = pd.json_normalize(df[col_name])
#                 flattened.columns = [f"{col_name}_{subcol}" for subcol in flattened.columns]
#                 df = df.drop(columns=[col_name]).join(flattened)

#             # Explode if the column type is list (similar to ArrayType)
#             elif isinstance(df[col_name].iloc[0], list):
#                 df = df.explode(col_name)

#         return df
#     # Example usage
#     # Assuming you have a DataFrame 'df' loaded from a JSON source

#     # Define columns to remove
#     remove_columns = [
#         "propertyDetails_images",
#         "propertyDetails_pdfs",
#         # Add more as needed
#     ]

#     # Call the function
#     flattened_df = flatten_json_pandas(df, remove_columns=remove_columns)

#     # Now, 'flattened_df' is your flattened DataFrame


