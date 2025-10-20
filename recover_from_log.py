import pandas as pd
import re
from pathlib import Path
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s'
)

def parse_log_file(log_file='geocoding.log'):
    """로그 파일에서 성공한 지오코딩 결과 추출"""
    
    results = []
    
    with open(log_file, 'r', encoding='utf-8') as f:
        for line in f:
            # 성공 로그 패턴 매칭
            # 예: 2025-10-20 13:54:36,576 - INFO - ✅ 성공 [도로명·도로명]: 강원도 삼척시 엑스포로 → (37.435992, 129.146897)
            
            if '✅ 성공' in line or '✓ 성공' in line:
                try:
                    # 주소 추출
                    addr_match = re.search(r']: (.+?) →', line)
                    if not addr_match:
                        continue
                    
                    address = addr_match.group(1).strip()
                    
                    # 좌표 추출
                    coord_match = re.search(r'→ \(([0-9.]+), ([0-9.]+)\)', line)
                    if not coord_match:
                        continue
                    
                    lat = float(coord_match.group(1))
                    lon = float(coord_match.group(2))
                    
                    # 시간 추출
                    time_match = re.match(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})', line)
                    timestamp = time_match.group(1) if time_match else None
                    
                    results.append({
                        'address': address,
                        'lat': lat,
                        'lon': lon,
                        'timestamp': timestamp
                    })
                    
                except Exception as e:
                    continue
    
    return results


def match_addresses(original_df, log_results):
    """로그 결과를 원본 데이터와 매칭"""
    
    # 로그 결과를 딕셔너리로 변환 (주소 → 좌표)
    addr_dict = {}
    for result in log_results:
        # 주소 정제 (비교를 위해)
        clean_addr = result['address'].strip()
        
        # 중복 제거 - 최신 것만 유지
        if clean_addr not in addr_dict or result['timestamp'] > addr_dict[clean_addr]['timestamp']:
            addr_dict[clean_addr] = result
    
    logging.info(f"로그에서 추출된 고유 주소: {len(addr_dict):,}개")
    
    # 원본 데이터에 매칭
    matched = 0
    for idx, row in original_df.iterrows():
        original_addr = str(row['주소']).strip() if '주소' in row else ''
        
        # 다양한 매칭 시도
        for clean_addr, result in addr_dict.items():
            # 1. 완전 일치
            if clean_addr == original_addr:
                original_df.at[idx, '위도'] = result['lat']
                original_df.at[idx, '경도'] = result['lon']
                original_df.at[idx, '처리상태'] = 'success'
                original_df.at[idx, '처리일시'] = result['timestamp']
                matched += 1
                break
            
            # 2. 로그 주소가 원본 주소에 포함됨
            elif clean_addr in original_addr:
                original_df.at[idx, '위도'] = result['lat']
                original_df.at[idx, '경도'] = result['lon']
                original_df.at[idx, '처리상태'] = 'success'
                original_df.at[idx, '처리일시'] = result['timestamp']
                matched += 1
                break
            
            # 3. 원본 주소가 로그 주소에 포함됨
            elif original_addr in clean_addr:
                original_df.at[idx, '위도'] = result['lat']
                original_df.at[idx, '경도'] = result['lon']
                original_df.at[idx, '처리상태'] = 'success'
                original_df.at[idx, '처리일시'] = result['timestamp']
                matched += 1
                break
    
    logging.info(f"매칭 성공: {matched:,}개")
    
    return original_df


def main():
    logging.info("=" * 70)
    logging.info("🔧 로그 파일에서 지오코딩 데이터 복구")
    logging.info("=" * 70)
    
    # 1. 로그 파일 파싱
    log_file = 'geocoding.log'
    if not Path(log_file).exists():
        logging.error(f"❌ 로그 파일을 찾을 수 없습니다: {log_file}")
        return
    
    logging.info(f"📂 로그 파일 읽기: {log_file}")
    log_results = parse_log_file(log_file)
    
    if not log_results:
        logging.error("❌ 로그에서 성공 기록을 찾을 수 없습니다!")
        return
    
    logging.info(f"✅ 로그에서 추출된 성공 기록: {len(log_results):,}건")
    
    # 2. 원본 파일 로드
    input_file = 'input/charger_v2.csv'
    logging.info(f"📂 원본 파일 읽기: {input_file}")
    
    df = pd.read_csv(input_file, encoding='utf-8-sig')
    
    # 컬럼 추가
    if '위도' not in df.columns:
        df['위도'] = None
    if '경도' not in df.columns:
        df['경도'] = None
    if '처리상태' not in df.columns:
        df['처리상태'] = 'pending'
    if '처리일시' not in df.columns:
        df['처리일시'] = None
    
    logging.info(f"원본 데이터: {len(df):,}건")
    
    # 3. 매칭
    logging.info("🔍 주소 매칭 중...")
    df = match_addresses(df, log_results)
    
    # 4. 저장
    output_file = Path('output/progress_from_log.csv')
    df.to_csv(output_file, index=False, encoding='utf-8-sig')
    
    # 통계
    total = len(df)
    success = (df['처리상태'] == 'success').sum()
    failed = (df['처리상태'] == 'failed').sum()
    pending = (df['처리상태'] == 'pending').sum()
    
    logging.info("")
    logging.info("=" * 70)
    logging.info("✅ 복구 완료!")
    logging.info(f"저장 위치: {output_file}")
    logging.info("=" * 70)
    logging.info(f"전체:      {total:>10,}건 (100.0%)")
    logging.info(f"✅ 성공:   {success:>10,}건 ({success/total*100:>5.1f}%)")
    logging.info(f"❌ 실패:   {failed:>10,}건 ({failed/total*100:>5.1f}%)")
    logging.info(f"⏳ 대기:   {pending:>10,}건 ({pending/total*100:>5.1f}%)")
    logging.info("=" * 70)
    
    logging.info("\n📌 다음 단계:")
    logging.info("1. progress_from_log.csv 확인")
    logging.info("2. 문제없으면:")
    logging.info("   copy output\\progress.csv output\\progress_old.csv")
    logging.info("   copy output\\progress_from_log.csv output\\progress.csv")
    logging.info("3. python geocode_vworld.py 실행")


if __name__ == "__main__":
    main()