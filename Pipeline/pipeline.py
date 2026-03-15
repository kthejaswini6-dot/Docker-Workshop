import sys
import pandas as pd
print("Hello Pipeline")
print("arguments", sys.argv)

day = int(sys.argv[1])
df = pd.DataFrame({"month": [1, 2], "num_passengers": [3, 4]})
df['day']=day
print(df.head())
print(f"Running pipeline for day {day}")

df.to_parquet(f"output_{day}.parquet")