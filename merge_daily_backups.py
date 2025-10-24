import pandas as pd
from pathlib import Path
from datetime import datetime
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s'
)

def merge_daily_backups():
    """output 폴더의 모든 daily_*.csv 파일을 하나로 병합"""
    
    print("=" * 70)
    print("🔄 일일 백업 파일 병합 도구")
    print("=" * 70)
    
    output_dir = Path("output")
    input_file = Path("input/charger_v2.csv")
    
    # 1. 원본 파일 로드
    if not input_file.exists():
        print(f"❌ 원본 파일을 찾을 수 없습니다: {input_file}")
        return
    
    print(f"\n[1단계] 원본 파일 로드")
    print("-" * 70)
    df_original = pd.read_csv(input_file, encoding='utf-8-sig')
    print(f"✅ 원본 파일: {len(df_original):,}건")
    
    # 병합할 데이터프레임 초기화
    df_merged = df_original.copy()
    df_merged['위도'] = None
    df_merged['경도'] = None
    df_merged['처리상태'] = 'pending'
    df_merged['처리일시'] = None
    
    # 2. daily 파일 찾기
    print(f"\n[2단계] daily 백업 파일 검색")
    print("-" * 70)
    
    daily_files = sorted(output_dir.glob("daily_*.csv"))
    
    if not daily_files:
        print("❌ daily_*.csv 파일을 찾을 수 없습니다!")
        print("\n파일 이름 형식: daily_YYYYMMDD.csv")
        print("예: daily_20251023.csv")
        return
    
    print(f"✅ 발견된 daily 파일: {len(daily_files)}개\n")
    
    # 파일 정보 출력
    file_info = []
    for daily_file in daily_files:
        try:
            df_daily = pd.read_csv(daily_file, encoding='utf-8-sig')
            has_coord = df_daily['위도'].notna() & df_daily['경도'].notna()
            success_count = has_coord.sum()
            
            # 날짜 추출
            date_str = daily_file.stem.replace('daily_', '')
            if len(date_str) == 8:
                date_formatted = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
            else:
                date_formatted = date_str
            
            file_info.append({
                'file': daily_file,
                'date': date_formatted,
                'total': len(df_daily),
                'success': success_count
            })
            
            print(f"  📅 {daily_file.name}")
            print(f"     날짜: {date_formatted}")
            print(f"     성공: {success_count:,}건 / 전체: {len(df_daily):,}건")
            
        except Exception as e:
            print(f"  ⚠️ {daily_file.name} 읽기 실패: {e}")
    
    if not file_info:
        print("\n❌ 읽을 수 있는 파일이 없습니다.")
        return
    
    # 3. 병합 옵션 선택
    print("\n" + "=" * 70)
    print("[3단계] 병합 방법 선택")
    print("=" * 70)
    print("\n옵션:")
    print("1. 모든 daily 파일 병합 (최신 것 우선)")
    print("2. 모든 daily 파일 병합 (오래된 것 우선)")
    print("3. 특정 날짜만 선택해서 병합")
    print("4. 취소")
    
    choice = input("\n선택 (1-4): ").strip()
    
    if choice == "3":
        print("\n사용 가능한 날짜:")
        for i, info in enumerate(file_info, 1):
            print(f"  {i}. {info['date']} ({info['success']:,}건)")
        
        selected = input("\n선택할 번호들 (쉼표로 구분, 예: 1,2,3): ").strip()
        try:
            indices = [int(x.strip()) - 1 for x in selected.split(',')]
            file_info = [file_info[i] for i in indices if 0 <= i < len(file_info)]
        except:
            print("❌ 잘못된 입력")
            return
    
    if choice == "4":
        print("❌ 취소")
        return
    
    # 4. 병합 실행
    print("\n" + "=" * 70)
    print("[4단계] 병합 중...")
    print("=" * 70 + "\n")
    
    # 정렬 (옵션 2면 오래된 것 우선)
    if choice == "2":
        file_info = sorted(file_info, key=lambda x: x['date'])
    else:
        file_info = sorted(file_info, key=lambda x: x['date'], reverse=True)
    
    total_recovered = 0
    
    for info in file_info:
        daily_file = info['file']
        try:
            df_daily = pd.read_csv(daily_file, encoding='utf-8-sig')
            
            # 성공한 레코드만 필터링
            has_coord = df_daily['위도'].notna() & df_daily['경도'].notna()
            success_records = df_daily[has_coord]
            
            if len(success_records) == 0:
                print(f"⚠️ {daily_file.name}: 좌표 데이터 없음")
                continue
            
            recovered_this_file = 0
            
            # 병합 (이미 좌표가 있으면 덮어쓰지 않음)
            for idx in success_records.index:
                if idx < len(df_merged):
                    # 아직 좌표가 없는 경우에만 업데이트
                    if pd.isna(df_merged.at[idx, '위도']):
                        df_merged.at[idx, '위도'] = df_daily.at[idx, '위도']
                        df_merged.at[idx, '경도'] = df_daily.at[idx, '경도']
                        df_merged.at[idx, '처리상태'] = 'success'
                        if '처리일시' in df_daily.columns:
                            df_merged.at[idx, '처리일시'] = df_daily.at[idx, '처리일시']
                        recovered_this_file += 1
                        total_recovered += 1
            
            print(f"✅ {daily_file.name}: {recovered_this_file:,}건 병합")
            
        except Exception as e:
            print(f"❌ {daily_file.name} 처리 중 오류: {e}")
    
    # 5. 결과 저장
    if total_recovered == 0:
        print("\n❌ 병합된 데이터가 없습니다.")
        return
    
    print("\n" + "=" * 70)
    print("[5단계] 저장")
    print("=" * 70)
    
    # 통계
    total = len(df_merged)
    success = (df_merged['처리상태'] == 'success').sum()
    pending = (df_merged['처리상태'] == 'pending').sum()
    
    print(f"\n📊 병합 결과:")
    print(f"  전체:      {total:,}건")
    print(f"  ✅ 성공:   {success:,}건 ({success/total*100:.1f}%)")
    print(f"  ⏳ 대기:   {pending:,}건 ({pending/total*100:.1f}%)")
    
    # 파일명 생성
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    merged_file = output_dir / f"merged_{timestamp}.csv"
    
    # 저장
    df_merged.to_csv(merged_file, index=False, encoding='utf-8-sig')
    
    print(f"\n✅ 저장 완료!")
    print(f"  위치: {merged_file}")
    print(f"  크기: {merged_file.stat().st_size / 1024 / 1024:.1f} MB")
    
    # 6. progress.csv 교체 여부 확인
    print("\n" + "=" * 70)
    print("[6단계] progress.csv 업데이트")
    print("=" * 70)
    
    progress_file = output_dir / "progress.csv"
    
    if progress_file.exists():
        df_current = pd.read_csv(progress_file, encoding='utf-8-sig')
        current_success = (df_current['처리상태'] == 'success').sum()
        print(f"\n현재 progress.csv: {current_success:,}건 성공")
        print(f"병합된 파일:       {success:,}건 성공")
        
        if success > current_success:
            print(f"\n✨ 병합 파일이 {success - current_success:,}건 더 많습니다!")
            replace = input("progress.csv를 교체하시겠습니까? (y/n): ").strip().lower()
            
            if replace == 'y':
                # 백업
                backup_file = output_dir / f"progress_backup_{timestamp}.csv"
                import shutil
                shutil.copy2(progress_file, backup_file)
                print(f"\n💾 기존 파일 백업: {backup_file.name}")
                
                # 교체
                shutil.copy2(merged_file, progress_file)
                print(f"✅ progress.csv 업데이트 완료!")
        else:
            print(f"\n⚠️ 현재 progress.csv가 더 많은 데이터를 가지고 있습니다.")
            print(f"   교체하지 않습니다.")
    else:
        print("\n⚠️ progress.csv가 없습니다.")
        create = input("병합 파일을 progress.csv로 저장하시겠습니까? (y/n): ").strip().lower()
        
        if create == 'y':
            import shutil
            shutil.copy2(merged_file, progress_file)
            print(f"✅ progress.csv 생성 완료!")
    
    print("\n" + "=" * 70)
    print("✅ 작업 완료!")
    print("=" * 70)


if __name__ == "__main__":
    merge_daily_backups()
    