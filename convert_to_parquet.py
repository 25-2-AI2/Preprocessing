import json
import pandas as pd
import os

# JSON 파일 읽기
json_path = "json_data/preprocessed_reviews.json"
output_dir = "parquet_data"

print(f"Loading JSON from {json_path}...")
with open(json_path, "r", encoding="utf-8") as f:
    data = json.load(f)

print(f"Total records: {len(data)}")

# DataFrame으로 변환
df = pd.DataFrame(data)

# 절반으로 분할
mid_point = len(df) // 2
df_part1 = df.iloc[:mid_point]
df_part2 = df.iloc[mid_point:]

print(f"Part 1: {len(df_part1)} records")
print(f"Part 2: {len(df_part2)} records")

# parquet_data 디렉토리 생성 (없으면)
os.makedirs(output_dir, exist_ok=True)

# Parquet으로 저장
part1_path = os.path.join(output_dir, "reviews_part1.parquet")
part2_path = os.path.join(output_dir, "reviews_part2.parquet")

df_part1.to_parquet(part1_path, index=False)
df_part2.to_parquet(part2_path, index=False)

print(f"Saved: {part1_path}")
print(f"Saved: {part2_path}")
print("Done!")
