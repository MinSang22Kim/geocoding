import pandas as pd
import glob

# 처리된 chunk CSV 경로
chunk_files = glob.glob("../output/charger_coordinates_*.csv")

# CSV 읽어서 통합
df_list = [pd.read_csv(f) for f in chunk_files]
df_all = pd.concat(df_list, ignore_index=True)

# 최종 통합 CSV 저장
output_file = "../output/charger_coordinates_all.csv"
df_all.to_csv(output_file, index=False, encoding="utf-8-sig")

print(f"✅ 모든 CSV 통합 완료: {output_file}")
