import pandas as pd
import requests
import time
from typing import Optional, Tuple
import logging
from pathlib import Path
from datetime import datetime, timedelta
import re
import shutil

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
        self.already_used_today = 0

    def clean_address(self, address: str) -> str:
        """주소 정제"""
        if pd.isna(address) or not address.strip():
            return ""
        
        addr = address.strip()
        addr = re.sub(r'\([^)]*\)', '', addr)
        addr = re.sub(r'\[[^\]]*\]', '', addr)
        
        noise_keywords = [
            '완속충전기', '급속충전기', '충전기', '충전소', 
            '주차장', '지하주차장', '옥외주차장',
            '입구', '앞', '옆', '뒤', '뒤편', '맞은편', '건너편',
            '우측', '좌측', '방면', '방향', '인근', '근처',
            '1층', '2층', '지하', 'B1', 'B2', '층'
        ]
        for keyword in noise_keywords:
            addr = addr.replace(keyword, ' ')
        
        addr = addr.replace('강원특별자치도', '강원도')
        addr = addr.replace('전북특별자치도', '전라북도')
        addr = addr.replace('전남특별자치도', '전라남도')
        addr = addr.replace('제주특별자치도', '제주도')
        
        corrections = {
            "홍덕구": "흥덕구", "원롱면": "월롱면", "원삭로": "원당로",
            "송백로로": "송백로", "달성군 현풍면": "달성군 현풍읍",
            "예천군 호명면": "예천군 호명읍", "현풍동로": "현풍중앙로",
            "현풍서로": "현풍중앙로", "도청대로": "충효로",
            "행복로": "충효로", "청석로": "경충대로",
            "수타사로": "수타사길", "덕풍로": "삼척로",
        }
        for wrong, right in corrections.items():
            addr = addr.replace(wrong, right)

        addr = re.sub(r'([가-힣])(\d)', r'\1 \2', addr)
        addr = re.sub(r'\s+', ' ', addr).strip()
        return addr

    def extract_address_candidates(self, address: str) -> list:
        """다양한 레벨의 주소 후보를 생성"""
        candidates = [address]
        
        no_number = re.sub(r'\s+\d+(-\d+)?(\s|$)', ' ', address).strip()
        if no_number != address and len(no_number) > 5:
            candidates.append(no_number)
        
        road_match = re.match(r'(.+?[시도군구]\s+.+?[읍면동]?\s*.+?[로길대로가])\s*', address)
        if road_match:
            road_only = road_match.group(1).strip()
            if road_only not in candidates and len(road_only) > 5:
                candidates.append(road_only)
        
        dong_match = re.match(r'(.+?[시도군구]\s+.+?[읍면동리])', address)
        if dong_match:
            dong_only = dong_match.group(1).strip()
            if dong_only not in candidates and len(dong_only) > 5:
                candidates.append(dong_only)
        
        return candidates

    def geocode_address(self, address: str, jibun_address: str = None) -> Tuple[Optional[float], Optional[float], str]:
        """주소를 좌표로 변환"""
        if self.today_count >= self.daily_limit:
            return None, None, 'limit_reached'

        addresses_to_try = []
        
        if address and not pd.isna(address):
            cleaned = self.clean_address(address)
            if cleaned and len(cleaned) > 3:
                addresses_to_try.append((cleaned, 'road', '도로명'))
        
        if jibun_address and not pd.isna(jibun_address):
            cleaned_jibun = self.clean_address(jibun_address)
            if cleaned_jibun and len(cleaned_jibun) > 3:
                addresses_to_try.append((cleaned_jibun, 'parcel', '지번'))

        if not addresses_to_try:
            return None, None, 'empty'

        for addr, addr_type, type_name in addresses_to_try:
            candidates = self.extract_address_candidates(addr)
            
            for i, candidate in enumerate(candidates):
                if len(candidate) < 3:
                    continue
                lat, lon, status = self._try_geocode(candidate, addr_type)
                if status == 'success':
                    level_names = ['정확주소', '도로명', '도로기준', '읍면동']
                    level = level_names[i] if i < len(level_names) else '광역'
                    logging.info(f"✅ 성공 [{level}·{type_name}]: {candidate[:40]} → ({lat:.6f}, {lon:.6f})")
                    return lat, lon, status
                time.sleep(0.08)

        return None, None, 'failed'

    def _try_geocode(self, address: str, addr_type: str = 'road') -> Tuple[Optional[float], Optional[float], str]:
        """VWorld API 요청"""
        params = {
            'service': 'address', 'request': 'getcoord', 'version': '2.0',
            'crs': 'epsg:4326', 'address': address, 'refine': 'true',
            'simple': 'false', 'format': 'json', 'type': addr_type,
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
        except:
            return None, None, 'failed'

def check_today_usage(df: pd.DataFrame, today_str: str) -> int:
    """오늘 이미 처리한 건수 확인"""
    if '처리일시' not in df.columns:
        return 0

    # NaN → 빈 문자열, 그리고 모든 값 문자열로 변환
    df['처리일시'] = df['처리일시'].fillna("").astype(str)

    # 혹시 '처리일시'가 datetime 타입으로 저장되어 있는 경우 대비
    if pd.api.types.is_datetime64_any_dtype(df['처리일시']):
        mask = df['처리일시'].dt.strftime("%Y-%m-%d").eq(today_str)
    else:
        mask = df['처리일시'].str.startswith(today_str)

    today_processed = df[mask]
    return len(today_processed)

def analyze_situation(df: pd.DataFrame, today_str: str):
    """현재 상황 분석 및 안내"""
    total = len(df)
    success = (df['처리상태'] == 'success').sum()
    pending = (df['처리상태'] == 'pending').sum()
    today_processed = check_today_usage(df, today_str)
    
    logging.info("=" * 70)
    logging.info("📊 현재 상황 분석")
    logging.info("=" * 70)
    logging.info(f"전체:          {total:>10,}건")
    logging.info(f"✅ 완료:       {success:>10,}건 ({success/total*100:>5.1f}%)")
    logging.info(f"⏳ 대기:       {pending:>10,}건 ({pending/total*100:>5.1f}%)")
    logging.info(f"📅 오늘 처리:  {today_processed:>10,}건")
    logging.info("=" * 70)
    
    # 상황 판단
    if today_processed >= 39000:
        logging.warning("\n⚠️  오늘은 이미 거의 한도(40,000건)를 사용했습니다!")
        logging.warning(f"   오늘 처리: {today_processed:,}건")
        logging.warning(f"   남은 한도: {40000 - today_processed:,}건")
        logging.info("\n💡 권장사항:")
        logging.info("   → 내일 다시 실행하세요")
        logging.info("   → 오늘 더 실행하려면 그대로 진행하면 됩니다")
        
        proceed = input("\n계속 진행하시겠습니까? (y/n): ").strip().lower()
        if proceed != 'y':
            logging.info("❌ 종료합니다. 내일 다시 실행하세요!")
            return False
    
    elif today_processed > 0:
        logging.info(f"\n💡 오늘 이미 {today_processed:,}건 처리했습니다.")
        logging.info(f"   남은 한도: {40000 - today_processed:,}건")
        logging.info("   → 이어서 처리합니다")
    
    else:
        logging.info("\n💡 오늘 처음 실행합니다.")
        logging.info(f"   한도: 40,000건")
    
    if pending == 0:
        logging.info("\n🎉 모든 데이터 처리 완료!")
        return False
    
    return True


def load_progress(progress_file: Path) -> Optional[pd.DataFrame]:
    """진행 파일 로드"""
    if progress_file.exists():
        try:
            logging.info(f"📂 진행 파일 로드: {progress_file}")
            return pd.read_csv(progress_file, encoding='utf-8-sig', low_memory=False)
        except Exception as e:
            logging.warning(f"⚠️ 파일 읽기 실패: {e}")
    return None


def save_progress_safe(df: pd.DataFrame, progress_file: Path):
    """안전한 저장"""
    try:
        temp_file = Path(str(progress_file) + '.tmp')
        df.to_csv(temp_file, index=False, encoding='utf-8-sig')
        
        if progress_file.exists():
            backup_file = Path(str(progress_file) + '.backup')
            shutil.copy2(progress_file, backup_file)
        
        shutil.move(str(temp_file), str(progress_file))
        logging.info(f"💾 저장 완료")
    except Exception as e:
        logging.error(f"❌ 저장 실패: {e}")
        raise


def save_daily_backup(df: pd.DataFrame, output_dir: Path, today_str: str):
    """오늘 처리한 데이터만 별도 저장"""
    try:
        today_processed = df[
            df['처리일시'].notna() & 
            df['처리일시'].str.startswith(today_str)
        ].copy()
        
        if len(today_processed) == 0:
            return
        
        today_file = today_str.replace('-', '')
        daily_file = output_dir / f"daily_{today_file}.csv"
        
        # 이미 오늘 백업이 있으면 병합
        if daily_file.exists():
            df_existing = pd.read_csv(daily_file, encoding='utf-8-sig')
            # 인덱스 기준으로 업데이트
            for idx in today_processed.index:
                if idx < len(df_existing):
                    df_existing.iloc[idx] = today_processed.iloc[0]  # 덮어쓰기
            today_processed = df_existing
        
        today_processed.to_csv(daily_file, index=False, encoding='utf-8-sig')
        
        success_count = (today_processed['처리상태'] == 'success').sum()
        logging.info(f"📅 일일 백업: {daily_file.name} ({success_count:,}건 성공)")
        
    except Exception as e:
        logging.error(f"❌ 일일 백업 실패: {e}")


def main():
    API_KEY = "01C14AE8-F90A-312A-92A2-395337FDB8AF"
    INPUT_FILE = "input/charger_v2.csv"
    OUTPUT_DIR = Path("output")
    PROGRESS_FILE = OUTPUT_DIR / "progress.csv"
    DAILY_LIMIT = 40000

    OUTPUT_DIR.mkdir(exist_ok=True)
    today_str = datetime.now().strftime('%Y-%m-%d')

    logging.info("=" * 70)
    logging.info(f"🚀 지오코딩 자동 실행")
    logging.info(f"   📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info("=" * 70)

    # 진행 파일 로드 또는 새로 생성
    progress_df = load_progress(PROGRESS_FILE)
    if progress_df is not None:
        df = progress_df
    else:
        logging.info(f"📂 원본 파일 읽기: {INPUT_FILE}")
        df = pd.read_csv(INPUT_FILE, encoding='utf-8-sig')
        df['위도'] = None
        df['경도'] = None
        df['처리상태'] = 'pending'
        df['처리일시'] = None
        logging.info(f"✅ {len(df):,}건 로드 (신규)")

    # 상황 분석 및 실행 여부 결정
    if not analyze_situation(df, today_str):
        return

    # 오늘 이미 사용한 한도 계산
    already_used = check_today_usage(df, today_str)
    remaining_limit = DAILY_LIMIT - already_used

    logging.info(f"\n⚡ 실행 시작 (남은 한도: {remaining_limit:,}건)")
    logging.info("=" * 70 + "\n")

    geocoder = VWorldGeocoder(API_KEY, remaining_limit)
    geocoder.already_used_today = already_used

    success_count = 0
    fail_count = 0
    skip_count = 0
    start_time = time.time()

    try:
        for idx in df.index:
            if df.at[idx, '처리상태'] == 'success':
                skip_count += 1
                continue

            if geocoder.today_count >= remaining_limit:
                logging.warning("=" * 70)
                logging.warning(f"⚠️  오늘 한도 도달!")
                logging.warning(f"   오늘 총 처리: {already_used + geocoder.today_count:,}건")
                logging.warning("=" * 70)
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

            if geocoder.today_count % 50 == 0:
                rate = (success_count / geocoder.today_count * 100) if geocoder.today_count > 0 else 0
                elapsed = time.time() - start_time
                speed = geocoder.today_count / elapsed if elapsed > 0 else 0
                remaining = remaining_limit - geocoder.today_count
                eta_min = int((remaining / speed / 60)) if speed > 0 else 0
                
                logging.info(f"📊 {already_used + geocoder.today_count:>5,}건 (오늘) | "
                             f"성공: {success_count:>4,} ({rate:>4.1f}%) | "
                             f"{speed:>4.1f}건/초 | "
                             f"남은시간: {eta_min}분")

            if geocoder.today_count % 1000 == 0:
                save_progress_safe(df, PROGRESS_FILE)

            time.sleep(0.12)

        save_progress_safe(df, PROGRESS_FILE)
        
        if geocoder.today_count > 0:
            save_daily_backup(df, OUTPUT_DIR, today_str)

    except KeyboardInterrupt:
        logging.warning("\n⚠️  중단됨. 저장 중...")
        save_progress_safe(df, PROGRESS_FILE)
        if geocoder.today_count > 0:
            save_daily_backup(df, OUTPUT_DIR, today_str)
        return

    # 최종 통계
    elapsed_time = time.time() - start_time
    total_today = already_used + geocoder.today_count

    logging.info("\n" + "=" * 70)
    logging.info("🎯 오늘 작업 완료")
    logging.info("=" * 70)
    logging.info(f"오늘 총 처리:  {total_today:>10,}건")
    logging.info(f"이번 실행:     {geocoder.today_count:>10,}건")
    logging.info(f"  ✅ 성공:     {success_count:>10,}건")
    logging.info(f"  ❌ 실패:     {fail_count:>10,}건")
    logging.info(f"⏱️  소요시간:   {int(elapsed_time//60)}분 {int(elapsed_time%60)}초")
    
    total = len(df)
    total_success = (df['처리상태'] == 'success').sum()
    total_pending = (df['처리상태'] == 'pending').sum()
    
    logging.info("-" * 70)
    logging.info(f"전체 진행률:   {total_success/total*100:>5.1f}% ({total_success:,}/{total:,}건)")
    logging.info(f"남은 작업:     {total_pending:>10,}건")
    
    if total_pending > 0:
        est_days = (total_pending + 39999) // 40000
        logging.info(f"예상 소요:     약 {est_days}일")
        logging.info("\n💡 내일 같은 명령어로 이어서 진행하세요!")
        logging.info("   → py geocode_vworld_smart.py")
    else:
        logging.info("\n🎉 전체 완료!")
    
    logging.info("=" * 70)

if __name__ == "__main__":
    main()
