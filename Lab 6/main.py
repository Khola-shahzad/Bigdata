import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

data = [
    {"id": 1, "name": "Khola", "age": 23},
    {"id": 2, "name": "Ayesha", "age": 25},
    {"id": 3, "name": "Hassan", "age": 28}
]
df = pd.DataFrame(data)
df.to_parquet("sample_data.parquet", engine="pyarrow", index=False)

print("Parquet file created successfully: sample_data.parquet")
print(df)

