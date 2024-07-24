import pandas as pd
import sys

READ_PATH = sys.argv[1]
SAVE_PATH = sys.argv[2]

columns = ['dt', 'cmd', 'cnt']
df = pd.read_csv(READ_PATH, on_bad_lines='skip', names=columns)

for column in columns:
    df[column] = df[column].str.replace('^', '')

# 결측치 처리
df['cnt'] = pd.to_numeric(df['cnt'], errors='coerce')
df['cnt'] = df['cnt'].fillna(0).astype(int)

# df['cnt'] = df['cnt'].astype(int)
# df.to_parquet(SAVE_PATH)

df.to_parquet(f'{SAVE_PATH}', partition_cols=['dt'])
