import pandas as pd
from pathlib import Path
from datetime import datetime
import shutil

print("=" * 70)
print("🔧 긴급 복구 도구")
print("=" * 70)

input_file = Path("input/charger_v2.csv")
progress_file = Path("output/progress.csv")
output_dir = Path("output")

# 1. 원본 파일 확인
if not input_file.exists():
    print(f"\n❌ 원본 파일을 찾을 수 없습니다: {input_file}")
    exit(1)

print(f"\n[1단계] 파일 비교")
print("-" * 70)

df_original = pd.read_csv(input_file, encoding='utf-8-sig')
print(f"✅ 원본 파일:     {len(df_original):,}건")

if progress_file.exists():
    df_progress = pd.read_csv(progress_file, encoding='utf-8-sig')
    print(f"⚠️ 진행 파일:     {len(df_progress):,}건")
    
    if len(df_progress) < len(df_original):
        print(f"\n🚨 문제 발견!")
        print(f"   원본보다 {len(df_original) - len(df_progress):,}건 적습니다!")
        print(f"   데이터가 손실되었습니다.")
    
    # 실제 좌표 확인
    if '위도' in df_progress.columns and '경도' in df_progress.columns:
        has_coord = df_progress['위도'].notna() & df_progress['경도'].notna()
        real_success = has_coord.sum()
        print(f"\n   실제 좌표 있음: {real_success:,}건")
        
        if real_success > 0:
            print(f"\n   ✅ {real_success:,}건의 좌표 데이터는 살릴 수 있습니다!")
else:
    print(f"⚠️ 진행 파일 없음")
    df_progress = None

# 2. 복구 방법 제시
print("\n" + "=" * 70)
print("[2단계] 복구 방법")
print("=" * 70)

if df_progress is not None and len(df_progress) < len(df_original):
    print("\n옵션:")
    print("1. 원본 파일 기준으로 재구성 + 기존 좌표 데이터 보존 (추천)")
    print("2. 원본 파일로 완전히 새로 시작 (모든 진행 초기화)")
    print("3. daily 백업 파일들 찾아서 병합")
    print("4. 취소")
    
    choice = input("\n선택 (1-4): ").strip()
    
    if choice == "1":
        print("\n" + "=" * 70)
        print("🔄 원본 기준 재구성 + 좌표 데이터 보존")
        print("=" * 70)
        
        # 백업
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_file = output_dir / f"progress_broken_{timestamp}.csv"
        shutil.copy2(progress_file, backup_file)
        print(f"\n💾 손상 파일 백업: {backup_file.name}")
        
        # 원본 기준으로 새 데이터프레임 생성
        df_new = df_original.copy()
        df_new['위도'] = None
        df_new['경도'] = None
        df_new['처리상태'] = 'pending'
        df_new['처리일시'] = None
        
        # 기존 좌표 데이터 복원
        recovered = 0
        if '위도' in df_progress.columns and '경도' in df_progress.columns:
            has_coord = df_progress['위도'].notna() & df_progress['경도'].notna()
            
            for idx in df_progress[has_coord].index:
                if idx < len(df_new):
                    df_new.at[idx, '위도'] = df_progress.at[idx, '위도']
                    df_new.at[idx, '경도'] = df_progress.at[idx, '경도']
                    df_new.at[idx, '처리상태'] = 'success'
                    if '처리일시' in df_progress.columns:
                        df_new.at[idx, '처리일시'] = df_progress.at[idx, '처리일시']
                    recovered += 1
        
        # 저장
        df_new.to_csv(progress_file, index=False, encoding='utf-8-sig')
        
        print(f"\n✅ 복구 완료!")
        print(f"   전체:         {len(df_new):,}건")
        print(f"   복원된 좌표:  {recovered:,}건")
        print(f"   재처리 필요:  {len(df_new) - recovered:,}건")
        
    elif choice == "2":
        print("\n" + "=" * 70)
        print("🔄 완전히 새로 시작")
        print("=" * 70)
        
        confirm = input("\n정말로 모든 진행 상황을 초기화하시겠습니까? (yes 입력): ").strip()
        if confirm != "yes":
            print("❌ 취소")
            exit(0)
        
        # 백업
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        if progress_file.exists():
            backup_file = output_dir / f"progress_old_{timestamp}.csv"
            shutil.copy2(progress_file, backup_file)
            print(f"\n💾 기존 파일 백업: {backup_file.name}")
        
        # 새로 생성
        df_new = df_original.copy()
        df_new['위도'] = None
        df_new['경도'] = None
        df_new['처리상태'] = 'pending'
        df_new['처리일시'] = None
        
        df_new.to_csv(progress_file, index=False, encoding='utf-8-sig')
        
        print(f"\n✅ 초기화 완료!")
        print(f"   전체: {len(df_new):,}건 (모두 대기 상태)")
        
    elif choice == "3":
        print("\n" + "=" * 70)
        print("🔍 daily 백업 파일 검색")
        print("=" * 70)
        
        daily_files = sorted(output_dir.glob("daily_*.csv"))
        
        if not daily_files:
            print("\n❌ daily 백업 파일을 찾을 수 없습니다!")
            print("   다른 옵션을 선택하세요.")
            exit(1)
        
        print(f"\n발견된 파일: {len(daily_files)}개\n")
        for df in daily_files:
            try:
                df_daily = pd.read_csv(df, encoding='utf-8-sig')
                has_coord = df_daily['위도'].notna() & df_daily['경도'].notna()
                print(f"  📅 {df.name}: {has_coord.sum():,}건 좌표")
            except:
                print(f"  ⚠️ {df.name}: 읽기 실패")
        
        proceed = input("\n이 파일들을 병합하시겠습니까? (y/n): ").strip().lower()
        if proceed != 'y':
            print("❌ 취소")
            exit(0)
        
        # 백업
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        if progress_file.exists():
            backup_file = output_dir / f"progress_old_{timestamp}.csv"
            shutil.copy2(progress_file, backup_file)
            print(f"\n💾 기존 파일 백업: {backup_file.name}")
        
        # 병합
        df_merged = df_original.copy()
        df_merged['위도'] = None
        df_merged['경도'] = None
        df_merged['처리상태'] = 'pending'
        df_merged['처리일시'] = None
        
        total_recovered = 0
        
        for daily_file in daily_files:
            try:
                df_daily = pd.read_csv(daily_file, encoding='utf-8-sig')
                has_coord = df_daily['위도'].notna() & df_daily['경도'].notna()
                
                for idx in df_daily[has_coord].index:
                    if idx < len(df_merged) and pd.isna(df_merged.at[idx, '위도']):
                        df_merged.at[idx, '위도'] = df_daily.at[idx, '위도']
                        df_merged.at[idx, '경도'] = df_daily.at[idx, '경도']
                        df_merged.at[idx, '처리상태'] = 'success'
                        if '처리일시' in df_daily.columns:
                            df_merged.at[idx, '처리일시'] = df_daily.at[idx, '처리일시']
                        total_recovered += 1
            except Exception as e:
                print(f"⚠️ {daily_file.name} 처리 오류: {e}")
        
        df_merged.to_csv(progress_file, index=False, encoding='utf-8-sig')
        
        print(f"\n✅ 병합 완료!")
        print(f"   전체:         {len(df_merged):,}건")
        print(f"   복원된 좌표:  {total_recovered:,}건")
        print(f"   재처리 필요:  {len(df_merged) - total_recovered:,}건")
    
    else:
        print("❌ 취소")
        exit(0)

else:
    # progress.csv가 없거나 문제없음
    print("\n새 progress.csv를 생성합니다.")
    
    df_new = df_original.copy()
    df_new['위도'] = None
    df_new['경도'] = None
    df_new['처리상태'] = 'pending'
    df_new['처리일시'] = None
    
    output_dir.mkdir(exist_ok=True)
    df_new.to_csv(progress_file, index=False, encoding='utf-8-sig')
    
    print(f"\n✅ 생성 완료!")
    print(f"   전체: {len(df_new):,}건")

print("\n" + "=" * 70)
print("✅ 작업 완료!")
print("=" * 70)
print("\n다음 단계:")
print("  py geocode_vworld_smart.py")
print("=" * 70)
