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
        """주소 정제 - 공격적으로 불필요한 정보 제거"""
        if pd.isna(address) or not address.strip():
            return ""
        
        addr = address.strip()

        # 1. 괄호 및 괄호 안 내용 제거
        addr = re.sub(r'\([^)]*\)', '', addr)
        addr = re.sub(r'\[[^\]]*\]', '', addr)
        
        # 2. 충전기/주차장 관련 키워드 완전 제거
        noise_keywords = [
            '완속충전기', '급속충전기', '충전기', '충전소', 
            '주차장', '지하주차장', '옥외주차장',
            '입구', '앞', '옆', '뒤', '뒤편', '맞은편', '건너편',
            '우측', '좌측', '방면', '방향', '인근', '근처',
            '1층', '2층', '지하', 'B1', 'B2', '층'
        ]
        for keyword in noise_keywords:
            addr = addr.replace(keyword, ' ')
        
        # 3. 행정구역명 표준화
        addr = addr.replace('강원특별자치도', '강원도')
        addr = addr.replace('전북특별자치도', '전라북도')
        addr = addr.replace('전남특별자치도', '전라남도')
        addr = addr.replace('제주특별자치도', '제주도')
        
        # 4. 오타 교정 및 행정구역명 수정
        corrections = {
            # 오타 교정
            "홍덕구": "흥덕구",
            "원롱면": "월롱면",
            "원삭로": "원당로",
            "송백로로": "송백로",
            
            # 행정구역 변경 (면 → 읍)
            "달성군 현풍면": "달성군 현풍읍",
            "예천군 호명면": "예천군 호명읍",
            
            # 도로명 변환 (신설 → 기존)
            "현풍동로": "현풍중앙로",
            "현풍서로": "현풍중앙로",
            "도청대로": "충효로",
            "행복로": "충효로",
            "청석로": "경충대로",
            "수타사로": "수타사길",
            "덕풍로": "삼척로",
        }
        for wrong, right in corrections.items():
            addr = addr.replace(wrong, right)

        # 5. 띄어쓰기 보정
        addr = re.sub(r'([가-힣])(\d)', r'\1 \2', addr)
        
        # 6. 중복 공백 제거 및 정리
        addr = re.sub(r'\s+', ' ', addr).strip()

        return addr

    def extract_address_candidates(self, address: str) -> list:
        """다양한 레벨의 주소 후보를 생성 (공격적)"""
        candidates = []
        
        # Level 1: 원본 주소
        candidates.append(address)
        
        # Level 2: 건물번호 제거
        no_number = re.sub(r'\s+\d+(-\d+)?(\s|$)', ' ', address).strip()
        if no_number != address and len(no_number) > 5:
            candidates.append(no_number)
        
        # Level 3: 길/로/대로까지만 (도로명만)
        road_match = re.match(r'(.+?[시도군구]\s+.+?[읍면동]?\s*.+?[로길대로가])\s*', address)
        if road_match:
            road_only = road_match.group(1).strip()
            if road_only not in candidates and len(road_only) > 5:
                candidates.append(road_only)
        
        # Level 4: 읍면동까지만
        dong_match = re.match(r'(.+?[시도군구]\s+.+?[읍면동리])', address)
        if dong_match:
            dong_only = dong_match.group(1).strip()
            if dong_only not in candidates and len(dong_only) > 5:
                candidates.append(dong_only)
        
        # Level 5: 시군구까지만 (마지막 수단)
        sigungu_match = re.match(r'(.+?[시군구])', address)
        if sigungu_match:
            sigungu_only = sigungu_match.group(1).strip()
            if sigungu_only not in candidates and len(sigungu_only) > 3:
                candidates.append(sigungu_only)
        
        return candidates

    def geocode_address(self, address: str, jibun_address: str = None) -> Tuple[Optional[float], Optional[float], str]:
        """주소를 좌표로 변환 (다단계 폴백)"""
        if self.today_count >= self.daily_limit:
            return None, None, 'limit_reached'

        addresses_to_try = []
        
        # 도로명 주소 처리
        if address and not pd.isna(address):
            cleaned = self.clean_address(address)
            if cleaned and len(cleaned) > 3:
                addresses_to_try.append((cleaned, 'road', '도로명'))
        
        # 지번 주소 처리
        if jibun_address and not pd.isna(jibun_address):
            cleaned_jibun = self.clean_address(jibun_address)
            if cleaned_jibun and len(cleaned_jibun) > 3:
                addresses_to_try.append((cleaned_jibun, 'parcel', '지번'))

        if not addresses_to_try:
            return None, None, 'empty'

        # 각 주소 타입별로 다단계 시도
        for addr, addr_type, type_name in addresses_to_try:
            candidates = self.extract_address_candidates(addr)
            
            for i, candidate in enumerate(candidates):
                if len(candidate) < 3:  # 너무 짧은 주소는 스킵
                    continue
                    
                lat, lon, status = self._try_geocode(candidate, addr_type)
                if status == 'success':
                    level_names = ['정확주소', '도로명', '도로기준', '읍면동', '시군구']
                    level = level_names[i] if i < len(level_names) else '광역'
                    logging.info(f"✅ 성공 [{level}·{type_name}]: {candidate[:40]} → ({lat:.6f}, {lon:.6f})")
                    return lat, lon, status
                
                # API 호출 간 짧은 대기
                time.sleep(0.08)

        # 모든 시도 실패
        reason = self._analyze_failure(addresses_to_try[0][0])
        short_addr = addresses_to_try[0][0][:50]
        logging.warning(f"❌ 실패 [{reason}]: {short_addr}")
        return None, None, 'failed'

    def _analyze_failure(self, address: str) -> str:
        """실패 원인 분석"""
        if re.search(r'\d{4,}', address):
            return "큰번지"
        elif len(address.split()) < 2:
            return "정보부족"
        else:
            return "미등록"

    def _try_geocode(self, address: str, addr_type: str = 'road') -> Tuple[Optional[float], Optional[float], str]:
        """VWorld API 요청"""
        params = {
            'service': 'address',
            'request': 'getcoord',
            'version': '2.0',
            'crs': 'epsg:4326',
            'address': address,
            'refine': 'true',
            'simple': 'false',
            'format': 'json',
            'type': addr_type,
            'key': self.api_key
        }

        try:
            response = requests.get(self.base_url, params=params, timeout=10)
            self.today_count += 1

            if response.status_code == 200:
                data = response.json()
                res = data.get('response', {})
                status = res.get('status')
                result = res.get('result', {})

                if status == 'OK' and isinstance(result, dict) and 'point' in result:
                    lon = float(result['point']['x'])
                    lat = float(result['point']['y'])
                    return lat, lon, 'success'

            return None, None, 'failed'

        except requests.exceptions.Timeout:
            return None, None, 'failed'
        except requests.exceptions.RequestException:
            return None, None, 'failed'
        except Exception as e:
            logging.error(f"API 오류: {str(e)}")
            return None, None, 'failed'


def load_progress(progress_file: Path) -> Optional[pd.DataFrame]:
    if progress_file.exists():
        try:
            logging.info(f"📂 이전 진행 상황 로드: {progress_file}")
            return pd.read_csv(progress_file, encoding='utf-8-sig', low_memory=False)
        except Exception as e:
            logging.warning(f"진행 파일 읽기 실패: {e}")
    return None


def save_progress(df: pd.DataFrame, progress_file: Path):
    df.to_csv(progress_file, index=False, encoding='utf-8-sig')
    logging.info(f"💾 저장 완료")


def get_batch_number(output_dir: Path) -> int:
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


def print_progress_stats(df: pd.DataFrame):
    """진행 상황 통계 출력"""
    total = len(df)
    success = (df['처리상태'] == 'success').sum()
    failed = (df['처리상태'] == 'failed').sum()
    pending = (df['처리상태'] == 'pending').sum()
    
    success_rate = (success / total * 100) if total > 0 else 0
    progress_rate = ((success + failed) / total * 100) if total > 0 else 0
    
    logging.info("=" * 70)
    logging.info("📊 전체 진행 상황")
    logging.info("-" * 70)
    logging.info(f"전체:      {total:>10,}건 (100.0%)")
    logging.info(f"✅ 성공:   {success:>10,}건 ({success_rate:>5.1f}%)")
    logging.info(f"❌ 실패:   {failed:>10,}건 ({failed/total*100:>5.1f}%)")
    logging.info(f"⏳ 대기:   {pending:>10,}건 ({pending/total*100:>5.1f}%)")
    logging.info("-" * 70)
    logging.info(f"진행률:    {progress_rate:>5.1f}%")
    
    if pending > 0:
        est_days = (pending + 39999) // 40000
        logging.info(f"예상:      약 {est_days}일 (40,000건/일)")
    
    logging.info("=" * 70)


def main():
    API_KEY = "01C14AE8-F90A-312A-92A2-395337FDB8AF"
    INPUT_FILE = "input/charger_v2.csv"
    OUTPUT_DIR = Path("output")
    PROGRESS_FILE = OUTPUT_DIR / "progress.csv"
    DAILY_LIMIT = 40000

    OUTPUT_DIR.mkdir(exist_ok=True)
    today_str = datetime.now().strftime('%Y%m%d')

    logging.info("=" * 70)
    logging.info(f"🚀 브이월드 지오코딩 시작")
    logging.info(f"   {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info("=" * 70)

    progress_df = load_progress(PROGRESS_FILE)
    if progress_df is not None:
        df = progress_df
        logging.info(f"✅ 이전 데이터 로드 완료")
        print_progress_stats(df)
    else:
        logging.info(f"📂 파일 읽기: {INPUT_FILE}")
        df = pd.read_csv(INPUT_FILE, encoding='utf-8-sig')
        df['위도'] = None
        df['경도'] = None
        df['처리상태'] = 'pending'
        df['처리일시'] = None
        logging.info(f"✅ {len(df):,}건 로드 (신규)")
        logging.info("=" * 70)

    batch_num = get_batch_number(OUTPUT_DIR)
    geocoder = VWorldGeocoder(API_KEY, DAILY_LIMIT)

    success_count = 0
    fail_count = 0
    skip_count = 0
    start_time = time.time()

    try:
        for idx in df.index:
            if df.at[idx, '처리상태'] == 'success':
                skip_count += 1
                continue

            if geocoder.today_count >= DAILY_LIMIT:
                logging.warning("=" * 70)
                logging.warning(f"⚠️  일일 한도 도달 ({DAILY_LIMIT:,}건)")
                logging.warning("=" * 70)
                save_progress(df, PROGRESS_FILE)
                break

            road_address = df.at[idx, '주소'] if '주소' in df.columns else None
            jibun_address = df.at[idx, '지번 주소'] if '지번 주소' in df.columns else None

            lat, lon, status = geocoder.geocode_address(road_address, jibun_address)

            df.at[idx, '위도'] = lat
            df.at[idx, '경도'] = lon
            df.at[idx, '처리상태'] = status
            df.at[idx, '처리일시'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            if status == 'success':
                success_count += 1
            elif status == 'failed':
                fail_count += 1

            # 50건마다 진행률 출력
            if geocoder.today_count % 50 == 0:
                rate = (success_count / geocoder.today_count * 100) if geocoder.today_count > 0 else 0
                elapsed = time.time() - start_time
                speed = geocoder.today_count / elapsed if elapsed > 0 else 0
                remaining_requests = DAILY_LIMIT - geocoder.today_count
                eta_sec = remaining_requests / speed if speed > 0 else 0
                eta_min = int(eta_sec / 60)
                
                logging.info(f"📊 {geocoder.today_count:>5,}건 | "
                             f"성공: {success_count:>4,} ({rate:>4.1f}%) | "
                             f"실패: {fail_count:>4,} | "
                             f"{speed:>4.1f}건/초 | "
                             f"남은시간: {eta_min}분")

            # 1000건마다 중간 저장
            if geocoder.today_count % 1000 == 0:
                save_progress(df, PROGRESS_FILE)

            # 적절한 딜레이
            time.sleep(0.12)

        save_progress(df, PROGRESS_FILE)

    except KeyboardInterrupt:
        logging.warning("\n⚠️  중단됨. 저장 중...")
        save_progress(df, PROGRESS_FILE)
        logging.warning("✅ 저장 완료")
        return

    # 오늘 처리 결과 저장
    if geocoder.today_count > 0:
        batch_output = OUTPUT_DIR / f"batch_{batch_num:02d}_{today_str}.csv"
        today_processed = df[df['처리일시'].notna() & df['처리일시'].str.startswith(today_str[:10])]
        if len(today_processed) > 0:
            today_processed.to_csv(batch_output, index=False, encoding='utf-8-sig')
            logging.info(f"\n💾 배치 저장: {batch_output.name} ({len(today_processed):,}건)")

    elapsed_time = time.time() - start_time
    avg_speed = geocoder.today_count / elapsed_time if elapsed_time > 0 else 0

    logging.info("\n" + "=" * 70)
    logging.info("🎯 오늘 작업 완료")
    logging.info("=" * 70)
    logging.info(f"API 요청:  {geocoder.today_count:>10,}건")
    logging.info(f"✅ 성공:   {success_count:>10,}건 ({success_count/geocoder.today_count*100 if geocoder.today_count > 0 else 0:>5.1f}%)")
    logging.info(f"❌ 실패:   {fail_count:>10,}건 ({fail_count/geocoder.today_count*100 if geocoder.today_count > 0 else 0:>5.1f}%)")
    logging.info(f"⏩ 스킵:   {skip_count:>10,}건")
    logging.info(f"⏱️  시간:   {int(elapsed_time//60)}분 {int(elapsed_time%60)}초")
    logging.info(f"⚡ 속도:   {avg_speed:>10.1f}건/초")
    logging.info("-" * 70)
    
    print_progress_stats(df)

    total_pending = (df['처리상태'] == 'pending').sum()
    if total_pending > 0:
        logging.info("\n💡 내일 실행 시 이어서 진행됩니다.")
    else:
        final_output = OUTPUT_DIR / "geocoded_final_complete.csv"
        df.to_csv(final_output, index=False, encoding='utf-8-sig')
        logging.info(f"\n🎉 전체 완료! {final_output.name}")
        
        failed_df = df[df['처리상태'] == 'failed']
        if len(failed_df) > 0:
            failed_output = OUTPUT_DIR / "geocoded_failed.csv"
            failed_df.to_csv(failed_output, index=False, encoding='utf-8-sig')
            logging.info(f"📋 실패 파일: {failed_output.name} ({len(failed_df):,}건)")
    
    logging.info("=" * 70)

if __name__ == "__main__":
    main()
    