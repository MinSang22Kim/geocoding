import pandas as pd
import requests
import time
from typing import Optional, Tuple
import logging
from pathlib import Path
from datetime import datetime
import re

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('geocoding.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

class VWorldGeocoder:
    def __init__(self, api_key: str, daily_limit: int = 40000):
        self.api_key = api_key
        self.base_url = "https://api.vworld.kr/req/address"
        self.daily_limit = daily_limit
        self.today_count = 0
        
    def clean_address(self, address: str) -> str:
        """주소 정제"""
        if pd.isna(address) or not address.strip():
            return ""
        
        addr = address.strip()
        
        # 여러 공백을 하나로
        addr = re.sub(r'\s+', ' ', addr)
        
        # 끝의 공백 제거
        addr = addr.strip()
        
        return addr
    
    def geocode_address(self, address: str, jibun_address: str = None) -> Tuple[Optional[float], Optional[float], str]:
        """
        주소를 좌표로 변환
        Returns: (위도, 경도, 상태)
        상태: 'success', 'failed', 'limit_reached'
        """
        # 일일 제한 확인
        if self.today_count >= self.daily_limit:
            return None, None, 'limit_reached'
        
        # 도로명 주소 먼저 시도
        addresses_to_try = []
        
        if address and not pd.isna(address):
            cleaned = self.clean_address(address)
            if cleaned:
                addresses_to_try.append(cleaned)
        
        # 지번 주소도 시도
        if jibun_address and not pd.isna(jibun_address):
            cleaned_jibun = self.clean_address(jibun_address)
            if cleaned_jibun and cleaned_jibun not in addresses_to_try:
                addresses_to_try.append(cleaned_jibun)
        
        if not addresses_to_try:
            return None, None, 'empty'
        
        # 각 주소로 시도
        for addr in addresses_to_try:
            result = self._try_geocode(addr)
            if result[2] == 'success':
                return result
        
        # 모두 실패
        logging.warning(f"지오코딩 실패: {addresses_to_try[0]}")
        return None, None, 'failed'
    
    def _try_geocode(self, address: str) -> Tuple[Optional[float], Optional[float], str]:
        """실제 지오코딩 요청"""
        params = {
            'service': 'address',
            'request': 'getCoord',
            'version': '2.0',
            'crs': 'epsg:4326',
            'address': address,
            'refine': 'true',
            'simple': 'false',
            'format': 'json',
            'type': 'parcel',  # parcel(지번) 우선 시도
            'key': self.api_key
        }
        
        try:
            response = requests.get(self.base_url, params=params, timeout=10)
            self.today_count += 1
            
            if response.status_code == 200:
                data = response.json()
                
                if data['response']['status'] == 'OK':
                    result = data['response']['result']
                    if result and 'point' in result:
                        lon = float(result['point']['x'])
                        lat = float(result['point']['y'])
                        return lat, lon, 'success'
                elif data['response']['status'] == 'NOT_FOUND':
                    # 도로명으로 재시도
                    params['type'] = 'road'
                    response2 = requests.get(self.base_url, params=params, timeout=10)
                    
                    if response2.status_code == 200:
                        data2 = response2.json()
                        if data2['response']['status'] == 'OK':
                            result2 = data2['response']['result']
                            if result2 and 'point' in result2:
                                lon = float(result2['point']['x'])
                                lat = float(result2['point']['y'])
                                return lat, lon, 'success'
                    
            return None, None, 'failed'
            
        except Exception as e:
            logging.error(f"요청 오류 ({address}): {str(e)}")
            return None, None, 'failed'

def load_progress(progress_file: Path) -> pd.DataFrame:
    """진행 상황 로드"""
    if progress_file.exists():
        logging.info(f"이전 진행 상황 로드: {progress_file}")
        return pd.read_csv(progress_file, encoding='utf-8-sig')
    return None

def save_progress(df: pd.DataFrame, progress_file: Path):
    """진행 상황 저장"""
    df.to_csv(progress_file, index=False, encoding='utf-8-sig')

def get_batch_number(output_dir: Path) -> int:
    """현재 배치 번호 확인"""
    existing_files = list(output_dir.glob("batch_*.csv"))
    if not existing_files:
        return 1
    
    batch_numbers = []
    for f in existing_files:
        try:
            num = int(f.stem.split('_')[1])
            batch_numbers.append(num)
        except:
            continue
    
    return max(batch_numbers) + 1 if batch_numbers else 1

def main():
    # 설정
    API_KEY = "01C14AE8-F90A-312A-92A2-395337FDB8AF"
    INPUT_FILE = "input/charger_v2.csv"
    OUTPUT_DIR = Path("output")
    PROGRESS_FILE = OUTPUT_DIR / "progress.csv"
    DAILY_LIMIT = 40000
    
    # 출력 디렉토리 생성
    OUTPUT_DIR.mkdir(exist_ok=True)
    
    today_str = datetime.now().strftime('%Y%m%d')
    
    logging.info("=" * 60)
    logging.info(f"브이월드 지오코딩 시작 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info("=" * 60)
    
    # 진행 상황 확인
    progress_df = load_progress(PROGRESS_FILE)
    
    if progress_df is not None:
        # 이전 작업 이어서 하기
        df = progress_df
        logging.info(f"이전 작업 이어서 진행 (총 {len(df):,}건)")
        
        # 아직 처리 안 된 것 카운트
        pending = df['처리상태'].isna() | (df['처리상태'] == 'pending')
        pending_count = pending.sum()
        completed_count = len(df) - pending_count
        
        logging.info(f"완료: {completed_count:,}건, 대기: {pending_count:,}건")
        
        if pending_count == 0:
            logging.info("모든 데이터 처리 완료!")
            return
    else:
        # 새로 시작
        logging.info(f"파일 읽기: {INPUT_FILE}")
        df = pd.read_csv(INPUT_FILE, encoding='utf-8-sig')
        df['위도'] = None
        df['경도'] = None
        df['처리상태'] = 'pending'
        df['처리일시'] = None
        logging.info(f"총 {len(df):,}건 로드됨")
    
    # 현재 배치 번호 확인
    batch_num = get_batch_number(OUTPUT_DIR)
    
    # 지오코더 초기화
    geocoder = VWorldGeocoder(API_KEY, DAILY_LIMIT)
    
    # 처리 시작
    success_count = 0
    fail_count = 0
    skip_count = 0
    batch_start_idx = None
    
    for idx in df.index:
        # 이미 처리된 건은 건너뛰기
        if df.at[idx, '처리상태'] == 'success':
            skip_count += 1
            continue
        
        # 배치 시작 인덱스 기록
        if batch_start_idx is None:
            batch_start_idx = idx
        
        # 일일 제한 도달 확인
        if geocoder.today_count >= DAILY_LIMIT:
            logging.warning("=" * 60)
            logging.warning(f"일일 제한({DAILY_LIMIT:,}건) 도달!")
            logging.warning("=" * 60)
            break
        
        # 주소 가져오기 - 도로명과 지번 모두 전달
        road_address = df.at[idx, '주소'] if '주소' in df.columns else None
        jibun_address = df.at[idx, '지번 주소'] if '지번 주소' in df.columns else None
        
        # 지오코딩 실행
        lat, lon, status = geocoder.geocode_address(road_address, jibun_address)
        
        # 결과 저장
        df.at[idx, '위도'] = lat
        df.at[idx, '경도'] = lon
        df.at[idx, '처리상태'] = status
        df.at[idx, '처리일시'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        if status == 'success':
            success_count += 1
        elif status == 'failed':
            fail_count += 1
        
        # 진행상황 출력 (100건마다)
        if geocoder.today_count % 100 == 0:
            success_rate = (success_count / geocoder.today_count * 100) if geocoder.today_count > 0 else 0
            logging.info(f"진행: {geocoder.today_count:,}/{DAILY_LIMIT:,}건 "
                       f"(성공: {success_count} [{success_rate:.1f}%], 실패: {fail_count})")
        
        # 중간 저장 (1000건마다)
        if geocoder.today_count % 1000 == 0:
            save_progress(df, PROGRESS_FILE)
        
        # API 요청 제한 (초당 5회)
        time.sleep(0.2)
    
    # 최종 진행상황 저장
    save_progress(df, PROGRESS_FILE)
    
    # 오늘 처리한 데이터만 추출하여 배치 파일로 저장
    if geocoder.today_count > 0:
        batch_output = OUTPUT_DIR / f"batch_{batch_num:02d}_{today_str}.csv"
        
        # 오늘 처리된 데이터 필터링 (처리일시가 오늘인 것)
        today_processed = df[df['처리일시'].notna() & df['처리일시'].str.startswith(today_str[:10])]
        
        if len(today_processed) > 0:
            today_processed.to_csv(batch_output, index=False, encoding='utf-8-sig')
            logging.info(f"\n✅ 오늘 배치 파일 저장: {batch_output}")
            logging.info(f"   저장된 건수: {len(today_processed):,}건")
    
    # 통계 출력
    total_processed = (df['처리상태'] == 'success').sum() + (df['처리상태'] == 'failed').sum()
    total_success = (df['처리상태'] == 'success').sum()
    total_pending = (df['처리상태'] == 'pending').sum()
    
    logging.info("\n" + "=" * 60)
    logging.info("오늘 작업 완료!")
    logging.info("=" * 60)
    logging.info(f"오늘 처리: {geocoder.today_count:,}건")
    logging.info(f"오늘 성공: {success_count:,}건 ({success_count/geocoder.today_count*100:.1f}%)")
    logging.info(f"오늘 실패: {fail_count:,}건 ({fail_count/geocoder.today_count*100:.1f}%)")
    logging.info(f"건너뜀: {skip_count:,}건 (이미 완료)")
    logging.info("")
    logging.info("=== 전체 진행 상황 ===")
    logging.info(f"전체 완료: {total_success:,}건 / {len(df):,}건 ({total_success/len(df)*100:.1f}%)")
    logging.info(f"남은 건수: {total_pending:,}건")
    
    if total_pending > 0:
        remaining_days = (total_pending + DAILY_LIMIT - 1) // DAILY_LIMIT
        logging.info(f"예상 남은 일수: 약 {remaining_days}일")
        logging.info("\n📌 내일 같은 명령어로 다시 실행하면 이어서 진행됩니다:")
        logging.info("   py geocode_vworld.py")
    else:
        # 최종 결과 파일 생성
        final_output = OUTPUT_DIR / "geocoded_final_complete.csv"
        df.to_csv(final_output, index=False, encoding='utf-8-sig')
        logging.info(f"\n🎉 전체 작업 완료! 최종 파일: {final_output}")
        
        # 실패 데이터 추출
        failed_df = df[df['처리상태'] == 'failed']
        if len(failed_df) > 0:
            failed_output = OUTPUT_DIR / "geocoded_failed.csv"
            failed_df.to_csv(failed_output, index=False, encoding='utf-8-sig')
            logging.info(f"실패 데이터 ({len(failed_df)}건): {failed_output}")

if __name__ == "__main__":
    main()