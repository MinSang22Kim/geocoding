import pandas as pd
import requests
import time
from typing import Optional, Tuple
import logging
from pathlib import Path
from datetime import datetime, timedelta
import re
import shutil

"""
════════════════════════════════════════════════════════════════════════════════
⭐⭐⭐ 설정 변경 필요시 아래 CONFIG 섹션만 수정하세요! ⭐⭐⭐
════════════════════════════════════════════════════════════════════════════════
"""

# ============================================================================
# ⚙️ CONFIG - 여기만 수정하세요!
# ============================================================================

# 🔑 API 키 설정 (계정 바꿀 때마다 여기만 수정!)
API_KEY = "CBDA8338-FEF2-34AE-9B04-D31B3597153F"  # ⭐ 여기 수정
# API_KEY = "01C14AE8-F90A-312A-92A2-395337FDB8AF"

# 📅 날짜 설정 (자동 vs 수동)
USE_AUTO_DATE = False  # True: 오늘 날짜 자동, False: 아래 날짜 사용
MANUAL_DATE = "2025-10-30"  # USE_AUTO_DATE가 False일 때 사용

# 📊 하루 한도
DAILY_LIMIT = 80000  # API 하루 한도

# 📂 파일 경로
INPUT_FILE = "input/charger_v2.csv"
OUTPUT_DIR = "output"

# ============================================================================
# ⚙️ CONFIG 끝 - 아래는 수정 금지!
# ============================================================================

"""
════════════════════════════════════════════════════════════════════════════════
이 코드의 특징:
1. ✅ pending만 처리 (success, failed는 건너뜀)
2. ✅ daily CSV 파일을 안전하게 누적 저장 (덮어쓰기 방지)
3. ✅ API 키 바뀌면 처음부터 다시 시작 가능
4. ✅ 4만 건마다 별도 파일 생성 가능
════════════════════════════════════════════════════════════════════════════════
"""

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

    df['처리일시'] = df['처리일시'].fillna("").astype(str)

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
    failed = (df['처리상태'] == 'failed').sum()
    pending = (df['처리상태'] == 'pending').sum()
    today_processed = check_today_usage(df, today_str)
    
    logging.info("=" * 70)
    logging.info("📊 현재 상황 분석")
    logging.info("=" * 70)
    logging.info(f"전체:          {total:>10,}건")
    logging.info(f"✅ 완료:       {success:>10,}건 ({success/total*100:>5.1f}%)")
    logging.info(f"❌ 실패:       {failed:>10,}건 ({failed/total*100:>5.1f}%)")
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
        logging.info("\n🎉 모든 대기 중인 데이터 처리 완료!")
        if failed > 0:
            logging.info(f"💡 실패한 {failed:,}건은 다시 시도하지 않습니다.")
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
        logging.info(f"💾 progress.csv 저장 완료")
    except Exception as e:
        logging.error(f"❌ 저장 실패: {e}")
        raise


def save_daily_backup_safe(df: pd.DataFrame, output_dir: Path, today_str: str):
    """
    오늘 처리한 데이터를 안전하게 누적 저장
    - 기존 파일이 있으면 이어붙임 (덮어쓰기 방지)
    - 여러 번 실행해도 안전
    """
    try:
        # 오늘 처리한 데이터 필터링
        df['처리일시'] = df['처리일시'].fillna("").astype(str)
        
        today_processed = df[
            df['처리일시'].str.startswith(today_str)
        ].copy()
        
        if len(today_processed) == 0:
            logging.warning(f"⚠️ 오늘({today_str}) 처리된 데이터가 없습니다.")
            return
        
        # 파일명 생성
        today_file = today_str.replace('-', '')
        daily_file = output_dir / f"daily_{today_file}.csv"
        
        # 기존 파일이 있는지 확인
        if daily_file.exists():
            logging.info(f"📂 기존 daily 파일 발견: {daily_file.name}")
            
            # 기존 파일 읽기
            existing_df = pd.read_csv(daily_file, encoding='utf-8-sig', low_memory=False)
            existing_count = len(existing_df)
            
            # 새로운 데이터와 병합 (중복 제거)
            # 인덱스 기준으로 최신 데이터로 업데이트
            combined = existing_df.set_index(existing_df.index)
            for idx in today_processed.index:
                if idx < len(combined):
                    combined.iloc[idx] = today_processed.loc[idx]
                else:
                    # 새로운 인덱스면 추가
                    combined = pd.concat([combined, today_processed.loc[[idx]]])
            
            combined.reset_index(drop=True, inplace=True)
            
            # 저장
            combined.to_csv(daily_file, index=False, encoding='utf-8-sig')
            
            new_count = len(combined)
            added_count = new_count - existing_count
            
            logging.info("=" * 70)
            logging.info(f"📅 일일 백업 업데이트 완료!")
            logging.info(f"   파일: {daily_file.name}")
            logging.info(f"   기존: {existing_count:,}건 → 현재: {new_count:,}건 (+{added_count:,}건)")
            
            success_count = (combined['처리상태'] == 'success').sum()
            failed_count = (combined['처리상태'] == 'failed').sum()
            logging.info(f"   성공: {success_count:,}건, 실패: {failed_count:,}건")
            logging.info("=" * 70)
            
        else:
            # 새 파일 생성
            today_processed.to_csv(daily_file, index=False, encoding='utf-8-sig')
            
            success_count = (today_processed['처리상태'] == 'success').sum()
            failed_count = (today_processed['처리상태'] == 'failed').sum()
            
            logging.info("=" * 70)
            logging.info(f"📅 일일 백업 생성 완료!")
            logging.info(f"   파일: {daily_file.name}")
            logging.info(f"   총 {len(today_processed):,}건 (성공: {success_count:,}, 실패: {failed_count:,})")
            logging.info("=" * 70)
        
    except Exception as e:
        logging.error(f"❌ 일일 백업 실패: {e}")
        import traceback
        logging.error(traceback.format_exc())


def main():
    # CONFIG에서 설정 가져오기
    output_dir = Path(OUTPUT_DIR)
    progress_file = output_dir / "progress.csv"
    
    output_dir.mkdir(exist_ok=True)
    
    # 날짜 설정
    if USE_AUTO_DATE:
        today_str = datetime.now().strftime('%Y-%m-%d')
        logging.info(f"🕒 today_str 계산값: {today_str}")
    else:
        today_str = MANUAL_DATE
    
    logging.info("=" * 70)
    logging.info(f"🚀 지오코딩 자동 실행")
    logging.info(f"   📅 현재 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info(f"   🗓️  처리 날짜: {today_str}")
    logging.info(f"   🔑 API 키: {API_KEY[:10]}...{API_KEY[-10:]}")
    logging.info("=" * 70)

    # 진행 파일 로드 또는 새로 생성
    progress_df = load_progress(progress_file)
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

    # ⭐⭐⭐ 핵심: pending 상태만 처리 (success와 failed는 건너뜀) ⭐⭐⭐
    pending_indices = df[df['처리상태'] == 'pending'].index
    total_success = (df['처리상태'] == 'success').sum()
    total_failed = (df['처리상태'] == 'failed').sum()
    
    logging.info(f"📝 처리 대상: {len(pending_indices):,}건 (pending만)")
    logging.info(f"🔄 건너뜀: {total_success + total_failed:,}건 (완료 {total_success:,} + 실패 {total_failed:,})\n")

    try:
        for idx in pending_indices:
            # 한도 체크
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
                save_progress_safe(df, progress_file)

            time.sleep(0.12)

        # ⭐ 최종 저장
        save_progress_safe(df, progress_file)
        
        # ⭐⭐⭐ 반드시 daily 백업 저장 (안전한 누적 방식) ⭐⭐⭐
        if geocoder.today_count > 0:
            logging.info("\n💾 일일 백업 파일 저장 중...")
            save_daily_backup_safe(df, output_dir, today_str)
        else:
            logging.warning("\n⚠️ 오늘 처리한 데이터가 없어 daily 파일을 생성하지 않습니다.")

    except KeyboardInterrupt:
        logging.warning("\n⚠️  중단됨. 저장 중...")
        save_progress_safe(df, progress_file)
        if geocoder.today_count > 0:
            save_daily_backup_safe(df, output_dir, today_str)
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
    total_failed = (df['처리상태'] == 'failed').sum()
    total_pending = (df['처리상태'] == 'pending').sum()
    
    logging.info("-" * 70)
    logging.info(f"전체 진행률:   {total_success/total*100:>5.1f}% ({total_success:,}/{total:,}건)")
    logging.info(f"실패:          {total_failed:>10,}건 (재시도 안 함)")
    logging.info(f"남은 작업:     {total_pending:>10,}건")
    
    if total_pending > 0:
        est_days = (total_pending + 39999) // 40000
        logging.info(f"예상 소요:     약 {est_days}일")
        logging.info("\n💡 같은 명령어로 이어서 진행하세요!")
        logging.info("   → py geocode_vworld_smart_v2.py")
    else:
        logging.info("\n🎉 전체 완료!")
    
    logging.info("=" * 70)

if __name__ == "__main__":
    main()
    