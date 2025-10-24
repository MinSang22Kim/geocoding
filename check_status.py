import pandas as pd
from pathlib import Path

print("=" * 70)
print("🔍 현재 상태 진단")
print("=" * 70)

# 1. progress.csv 확인
progress_file = Path("output/progress.csv")
if progress_file.exists():
    print(f"\n📂 {progress_file}")
    df = pd.read_csv(progress_file, encoding='utf-8-sig')
    
    print(f"\n전체 행 수: {len(df):,}건")
    print(f"\n컬럼 목록:")
    for col in df.columns:
        print(f"  - {col}")
    
    # 처리상태 확인
    if '처리상태' in df.columns:
        print(f"\n처리상태 분포:")
        print(df['처리상태'].value_counts())
    
    # 위도/경도 확인
    if '위도' in df.columns and '경도' in df.columns:
        has_coord = df['위도'].notna() & df['경도'].notna()
        print(f"\n실제 좌표 보유:")
        print(f"  ✅ 있음: {has_coord.sum():,}건")
        print(f"  ❌ 없음: {(~has_coord).sum():,}건")
        
        # 샘플 출력
        print(f"\n처음 5건 샘플:")
        print(df[['주소', '위도', '경도', '처리상태']].head())
        
        # 문제 진단
        success_but_no_coord = (df['처리상태'] == 'success') & (~has_coord)
        if success_but_no_coord.sum() > 0:
            print(f"\n⚠️ 문제 발견!")
            print(f"   성공으로 표시되었지만 좌표가 없는 건수: {success_but_no_coord.sum():,}건")
            print(f"\n샘플:")
            print(df[success_but_no_coord][['주소', '위도', '경도', '처리상태']].head())
else:
    print(f"\n❌ {progress_file} 파일이 없습니다!")

# 2. 기타 파일 확인
print("\n" + "=" * 70)
print("📁 output 폴더 파일 목록")
print("=" * 70)

output_dir = Path("output")
if output_dir.exists():
    for file in sorted(output_dir.glob("*.csv")):
        size = file.stat().st_size / 1024 / 1024  # MB
        print(f"  {file.name:<40} {size:>8.2f} MB")
else:
    print("❌ output 폴더가 없습니다!")

print("\n" + "=" * 70)
