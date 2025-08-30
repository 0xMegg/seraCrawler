import pandas as pd
import time
import re
import urllib.parse
import platform
import os
import random
import csv
import glob
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from webdriver_manager.chrome import ChromeDriverManager
import logging

# 설정 파일 import
from config import CSV_CONFIG

class FlexibleCrawler:
    def __init__(self):
        self.setup_driver()
        self.setup_logging()
        self.processed_count = 0
        self.result_file = None
        self.config = CSV_CONFIG
        
    def setup_driver(self):
        """Chrome WebDriver 설정"""
        print("Chrome WebDriver 설정 중...")
        chrome_options = Options()
        
        # 맥OS 호환성 설정
        if platform.system() == "Darwin":
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--remote-debugging-port=9222")
            if platform.machine() == "arm64":
                chrome_options.add_argument("--disable-background-timer-throttling")
                chrome_options.add_argument("--disable-backgrounding-occluded-windows")
                chrome_options.add_argument("--disable-renderer-backgrounding")
                chrome_options.add_argument("--disable-features=TranslateUI")
                chrome_options.add_argument("--disable-ipc-flooding-protection")
        else:
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
        
        # 공통 설정
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-plugins")
        chrome_options.add_argument("--disable-images")
        
        # 봇 탐지 회피 설정
        chrome_options.add_argument("--no-first-run")
        chrome_options.add_argument("--no-default-browser-check")
        chrome_options.add_argument("--disable-default-apps")
        chrome_options.add_argument("--disable-popup-blocking")
        chrome_options.add_argument("--disable-notifications")
        chrome_options.add_argument("--disable-web-security")
        chrome_options.add_argument("--allow-running-insecure-content")
        chrome_options.add_argument("--disable-features=VizDisplayCompositor")
        
        # User-Agent 설정
        chrome_options.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        
        try:
            if platform.system() == "Darwin" and platform.machine() == "arm64":
                print("맥OS ARM64 환경 감지, 직접 Chrome 경로 사용")
                chrome_options.binary_location = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
                self.driver = webdriver.Chrome(options=chrome_options)
                print("맥OS ARM64 Chrome으로 WebDriver 설정 완료!")
            else:
                service = Service(ChromeDriverManager().install())
                self.driver = webdriver.Chrome(service=service, options=chrome_options)
                print("Chrome WebDriver 설정 완료!")
            
            # 봇 탐지 회피를 위한 JavaScript 실행
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            self.driver.execute_script("Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]})")
            self.driver.execute_script("Object.defineProperty(navigator, 'languages', {get: () => ['ko-KR', 'ko', 'en-US', 'en']})")
            self.driver.execute_script("Object.defineProperty(navigator, 'platform', {get: () => 'MacIntel'})")
            
            self.driver.delete_all_cookies()
            
        except Exception as e:
            print(f"Chrome WebDriver 설정 실패: {e}")
            if platform.system() == "Darwin":
                try:
                    print("대안 방법으로 Chrome 설정 시도...")
                    chrome_options.binary_location = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
                    self.driver = webdriver.Chrome(options=chrome_options)
                    print("맥OS 기본 Chrome으로 WebDriver 설정 완료!")
                except Exception as e2:
                    print(f"맥OS 기본 Chrome 설정도 실패: {e2}")
                    raise e2
            else:
                raise e
        
    def setup_logging(self):
        """로깅 설정"""
        timestamp = datetime.now().strftime("%y%m%d%H%M%S")
        log_filename = f"flexible_crawling_{timestamp}.log"
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_filename, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        print(f"로깅 설정 완료: {log_filename}")
        
    def extract_dong_name(self, address):
        """주소에서 동이름 추출"""
        if pd.isna(address) or address == '':
            return None
            
        # 동/리 추출 (현재 방식 유지)
        address_parts = address.split()
        for part in address_parts:
            if part.endswith('동') or part.endswith('리'):
                return part
                
        return None
        
    def check_existing_results(self):
        """기존 결과 파일 확인"""
        try:
            # 현재 디렉토리에서 flexible_crawling_*.csv 파일들 찾기
            csv_files = glob.glob('flexible_crawling_*.csv')
            
            if not csv_files:
                return None
            
            # 가장 최근 파일 선택
            latest_file = max(csv_files, key=os.path.getctime)
            print(f"📋 기존 결과 파일 발견: {latest_file}")
            self.logger.info(f"📋 기존 결과 파일 발견: {latest_file}")
            
            # 파일 읽기
            df = pd.read_csv(latest_file)
            if len(df) > 0:
                print(f"📊 기존 결과: {len(df)}개 행")
                self.logger.info(f"📊 기존 결과: {len(df)}개 행")
                return df.to_dict('records')
            
            return None
            
        except Exception as e:
            print(f"기존 결과 확인 중 오류: {e}")
            self.logger.error(f"기존 결과 확인 중 오류: {e}")
            return None
    
    def initialize_result_file(self, append_mode=False):
        """결과 파일 초기화"""
        try:
            if append_mode:
                # 기존 파일에 추가 모드
                csv_files = glob.glob('flexible_crawling_*.csv')
                if csv_files:
                    self.result_file = max(csv_files, key=os.path.getctime)
                    print(f"📁 기존 결과 파일에 추가: {self.result_file}")
                    self.logger.info(f"📁 기존 결과 파일에 추가: {self.result_file}")
                    return
                else:
                    print("⚠️ 기존 파일을 찾을 수 없어 새 파일 생성")
                    append_mode = False
            
            if not append_mode:
                # 새 파일 생성
                timestamp = datetime.now().strftime("%y%m%d%H%M%S")
                self.result_file = f'flexible_crawling_{timestamp}.csv'
                
                # 설정에서 출력 컬럼 가져오기
                headers = self.config['output_columns']
                
                with open(self.result_file, 'w', encoding='utf-8-sig', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow(headers)
                
                print(f"📁 새 결과 파일 생성: {self.result_file}")
                self.logger.info(f"새 결과 파일 생성: {self.result_file}")
            
        except Exception as e:
            print(f"결과 파일 초기화 중 오류: {e}")
            self.logger.error(f"결과 파일 초기화 중 오류: {e}")
    
    def save_single_result(self, result):
        """단일 결과 저장"""
        try:
            if not self.result_file:
                print("결과 파일이 초기화되지 않았습니다.")
                return False
            
            # 설정에서 출력 컬럼 순서대로 저장
            with open(self.result_file, 'a', encoding='utf-8-sig', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([
                    result['인덱스'], result['사업장명'], result['기존주소'], 
                    result['기존전화번호'], result['새전화번호'], result['업데이트상태'], 
                    result['주소유사도점수'], result['수집된주소']
                ])
            
            return True
            
        except Exception as e:
            print(f"단일 결과 저장 중 오류: {e}")
            self.logger.error(f"단일 결과 저장 중 오류: {e}")
            return False
    
    def search_and_extract_phone(self, business_name, dong_name, original_address=None):
        """검색과 전화번호 추출"""
        try:
            self.current_original_address = original_address
            self.current_collected_address = ""
            self.current_collected_jibun_address = ""  # 구주소 저장용
            
            # 1차 검색: 사업장명 + 동이름
            search_query = f"{business_name} {dong_name}"
            print(f"=== 1차 검색: {search_query} ===")
            self.logger.info(f"=== 1차 검색: {search_query} ===")
            
            encoded_query = urllib.parse.quote(search_query)
            search_url = f"https://map.naver.com/p/search/{encoded_query}"
            self.logger.info(f"1차 검색 URL: {search_url}")
            
            self.driver.get(search_url)
            
            wait_time = random.uniform(3.5, 5.0)  # 3.5초 최소값으로 설정
            print(f"1차 검색 결과 로딩 중... ({wait_time:.1f}초)")
            self.logger.info(f"1차 검색 결과 로딩 대기 중... ({wait_time:.1f}초)")
            time.sleep(wait_time)
            
            phone_number = self._check_and_extract_phone()
            if phone_number:
                return phone_number
            elif phone_number == "MULTIPLE_RESULTS_NO_PHONE":
                return "MULTIPLE_RESULTS_NO_PHONE"
            
            # 2차 검색: 사업장명만
            print(f"=== 2차 검색: {business_name} ===")
            self.logger.info(f"=== 2차 검색: {business_name} ===")
            
            encoded_business = urllib.parse.quote(business_name)
            search_url = f"https://map.naver.com/p/search/{encoded_business}"
            self.logger.info(f"2차 검색 URL: {search_url}")
            
            self.driver.get(search_url)
            
            wait_time = random.uniform(3.5, 5.0)  # 3.5초 최소값으로 설정
            print(f"2차 검색 결과 로딩 중... ({wait_time:.1f}초)")
            self.logger.info(f"2차 검색 결과 로딩 대기 중... ({wait_time:.1f}초)")
            time.sleep(wait_time)
            
            phone_number = self._check_and_extract_phone()
            if phone_number:
                return phone_number
            
            print("전화번호를 찾을 수 없음")
            self.logger.warning("전화번호를 찾을 수 없음")
            return None
            
        except Exception as e:
            self.logger.error(f"검색 및 전화번호 추출 중 오류: {e}")
            print(f"검색 및 전화번호 추출 중 오류: {e}")
            return None
    
    def is_ulsan_donggu_address(self, address):
        """주소가 울산 동구인지 확인"""
        if not address:
            return False
        
        # 울산 동구 관련 키워드 확인
        ulsan_donggu_keywords = [
            "울산광역시 동구", "울산 동구", "울산시 동구",
            "동부동", "서부동", "중앙동", "화진동", "대송동", 
            "일산동", "전하동", "미포동", "주전동", "상대동"
        ]
        
        address_lower = address.lower()
        for keyword in ulsan_donggu_keywords:
            if keyword.lower() in address_lower:
                return True
        
        return False

    def _check_and_extract_phone(self):
        """현재 페이지에서 전화번호 확인 및 추출 (개선된 분기 처리)"""
        try:
            print("=== iframe 처리 시작 ===")
            
            # searchIframe 로딩 대기
            try:
                print("searchIframe 로딩 대기 중... (최대 10초)")
                iframe = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.ID, "searchIframe"))
                )
                print("searchIframe 발견")
                
                print("searchIframe으로 전환 중...")
                self.driver.switch_to.frame(iframe)
                print("searchIframe 전환 완료")
                
                # 검색 결과 로딩 대기
                print("검색 결과 로딩 대기 중...")
                time.sleep(3)
                
                # 검색 결과 개수 확인
                result_count = self._get_search_result_count()
                print(f"🔍 검색 결과 개수: {result_count}")
                
                # 1. 검색 결과 0개
                if result_count == 0:
                    print("❌ 검색 결과가 없습니다.")
                    self.driver.switch_to.default_content()
                    return None
                
                # 2. 검색 결과 1개
                elif result_count == 1:
                    print("📱 단일 결과 처리 시작")
                    return self._process_single_result_improved()
                
                # 3. 검색 결과 2개 이상
                elif result_count >= 2:
                    print(f"📱 다중 결과 처리 시작 ({result_count}개)")
                    return self._process_multiple_results_improved()
                
                else:
                    print("❌ 예상치 못한 결과 개수")
                    self.driver.switch_to.default_content()
                    return None
                
            except Exception as e:
                print(f"❌ searchIframe 처리 중 오류: {e}")
                self.driver.switch_to.default_content()
            
            # searchIframe에서 결과가 없으면 entryIframe 확인
            print("entryIframe에서 전화번호 확인...")
            phone_number = self.extract_phone_number_direct()
            if phone_number:
                return phone_number
            
            return None
            
        except Exception as e:
            print(f"전화번호 확인 중 오류: {e}")
            return None

    def _get_search_result_count(self):
        """검색 결과 개수를 정확히 파악"""
        try:
            # 1. 먼저 검색 결과 리스트 요소 찾기
            results = self.driver.find_elements(By.CSS_SELECTOR, "li.VLTHu.OW9LQ")
            
            if results:
                print(f"✅ li.VLTHu.OW9LQ로 {len(results)}개 결과 발견")
                return len(results)
            
            # 2. 다른 CSS 선택자로 시도
            results = self.driver.find_elements(By.CSS_SELECTOR, ".place_bluelink")
            if results:
                print(f"✅ .place_bluelink로 {len(results)}개 결과 발견")
                return len(results)
            
            # 3. 검색 결과가 없는 경우 확인
            # "검색 결과가 없습니다" 메시지 확인
            no_result_elements = self.driver.find_elements(By.XPATH, "//*[contains(text(), '검색 결과가 없습니다')]")
            if no_result_elements:
                print("✅ 검색 결과 없음 메시지 발견")
                return 0
            
            # 4. iframe 내부의 전체 텍스트 확인
            page_text = self.driver.page_source
            if "검색 결과가 없습니다" in page_text or "결과가 없습니다" in page_text:
                print("✅ 페이지 소스에서 검색 결과 없음 확인")
                return 0
            
            # 5. 검색 결과 컨테이너 확인
            result_containers = self.driver.find_elements(By.CSS_SELECTOR, "[data-testid*='search'], [class*='search'], [class*='result']")
            if not result_containers:
                print("✅ 검색 결과 컨테이너 없음")
                return 0
            
            # 6. 마지막 확인: 실제 결과 요소가 있는지
            all_links = self.driver.find_elements(By.TAG_NAME, "a")
            place_links = [link for link in all_links if link.get_attribute('href') and 'place.naver.com' in link.get_attribute('href')]
            
            if place_links:
                print(f"✅ 네이버 플레이스 링크 {len(place_links)}개 발견")
                return len(place_links)
            
            print("❌ 검색 결과를 찾을 수 없음")
            return 0
            
        except Exception as e:
            print(f"❌ 검색 결과 개수 확인 중 오류: {e}")
            return 0

    def _process_single_result_improved(self):
        """단일 검색 결과 처리 (개선된 버전)"""
        try:
            print("=== 단일 결과 처리 시작 ===")
            
            # 메인 페이지로 복귀
            self.driver.switch_to.default_content()
            
            # 1단계: 직접 전화번호 추출 시도 (데이터가 표기된 경우)
            phone_number = self.extract_phone_number_direct()
            if phone_number:
                print("✅ 직접 전화번호 추출 성공 (데이터 표기됨)")
                return phone_number
            
            # 2단계: 결과 클릭하여 상세 정보에서 전화번호 추출 시도 (데이터 미표기된 경우)
            print("직접 추출 실패, 결과 클릭하여 상세 정보 확인...")
            phone_number = self._click_single_result_and_extract()
            if phone_number:
                print("✅ 클릭 후 전화번호 추출 성공 (데이터 미표기였음)")
                return phone_number
            
            # 3단계: 주소 확인하여 울산 동구가 맞는지 검증
            print("전화번호 추출 실패, 주소 확인 중...")
            address_info = self._extract_single_result_address()
            if address_info:
                if self.is_ulsan_donggu_address(address_info):
                    print("✅ 주소 확인: 울산 동구 맞음 (전화번호만 없는 경우)")
                    return None
                else:
                    print("❌ 주소 확인: 울산 동구 아님 (잘못된 결과)")
                    return None
            
            print("❌ 단일 결과에서 전화번호를 찾을 수 없고 주소도 확인 불가")
            return None
            
        except Exception as e:
            print(f"❌ 단일 결과 처리 중 오류: {e}")
            self.driver.switch_to.default_content()
            return None

    def _extract_single_result_address(self):
        """단일 결과에서 주소 정보 추출"""
        try:
            # searchIframe으로 다시 전환
            self.driver.switch_to.frame("searchIframe")
            
            # 주소 정보 찾기 (여러 CSS 선택자 시도)
            address_selectors = [
                "span.Pb4bU",  # 일반적인 주소 표시
                "span[class*='address']",  # 주소 관련 클래스
                "div[class*='address']",   # 주소 관련 div
                "span[class*='location']"  # 위치 관련 클래스
            ]
            
            for selector in address_selectors:
                address_elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                if address_elements:
                    address_text = address_elements[0].text.strip()
                    if address_text and len(address_text) > 5:
                        print(f"✅ 주소 정보 발견: {address_text}")
                        self.current_collected_address = address_text
                        self.driver.switch_to.default_content()
                        return address_text
            
            print("❌ 주소 정보를 찾을 수 없음")
            self.driver.switch_to.default_content()
            return None
            
        except Exception as e:
            print(f"주소 추출 중 오류: {e}")
            self.driver.switch_to.default_content()
            return None

    def _process_multiple_results_improved(self):
        """다중 검색 결과 처리 (개선된 버전)"""
        try:
            print("=== 다중 결과 처리 시작 ===")
            
            # searchIframe에서 검색 결과 다시 찾기
            results = self.driver.find_elements(By.CSS_SELECTOR, "li.VLTHu.OW9LQ")
            if not results:
                results = self.driver.find_elements(By.CSS_SELECTOR, ".place_bluelink")
            
            if not results:
                print("❌ 다중 결과에서 검색 결과 요소를 찾을 수 없음")
                self.driver.switch_to.default_content()
                return None
            
            print(f"다중 결과 {len(results)}개 발견")
            
            # 상위 3개 결과만 확인
            top_results = results[:3]
            ulsan_donggu_results = []
            other_location_results = []
            
            for i, result in enumerate(top_results):
                try:
                    print(f"결과 {i+1} 주소 확인 중...")
                    
                    # searchIframe으로 다시 전환
                    self.driver.switch_to.frame("searchIframe")
                    
                    # 해당 결과 내에서 주소 정보 찾기
                    address_elements = result.find_elements(By.CSS_SELECTOR, "span.Pb4bU")
                    
                    if address_elements:
                        search_address = address_elements[0].text.strip()
                        print(f"결과 {i+1} 주소: {search_address}")
                        
                        # 울산 동구 여부 확인
                        if self.is_ulsan_donggu_address(search_address):
                            print(f"✅ 결과 {i+1}: 울산 동구 맞음")
                            ulsan_donggu_results.append({
                                'index': i,
                                'address': search_address,
                                'element': result
                            })
                        else:
                            print(f"❌ 결과 {i+1}: 울산 동구 아님")
                            other_location_results.append({
                                'index': i,
                                'address': search_address,
                                'element': result
                            })
                        
                        # 메인 페이지로 복귀
                        self.driver.switch_to.default_content()
                    else:
                        print(f"결과 {i+1}에서 주소 정보를 찾을 수 없음")
                        self.driver.switch_to.default_content()
                        
                except Exception as e:
                    print(f"결과 {i+1} 처리 중 오류: {e}")
                    self.driver.switch_to.default_content()
                    continue
            
            # 울산 동구 결과가 있는 경우
            if ulsan_donggu_results:
                print(f"울산 동구 결과 {len(ulsan_donggu_results)}개 발견, 최적 결과 선택 중...")
                
                # 주소 유사도 점수로 최적 결과 선택
                best_result = None
                best_score = -1
                
                for result_info in ulsan_donggu_results:
                    score = self.compare_address_similarity(result_info['address'])
                    print(f"결과 {result_info['index']+1} 유사도 점수: {score}")
                    
                    if score > best_score:
                        best_score = score
                        best_result = result_info
                
                if best_result:
                    print(f"최적 결과 선택: {best_result['index']+1}번째 (점수: {best_score})")
                    self.current_collected_address = best_result['address']
                    
                    # 최적 결과 클릭하여 전화번호 추출
                    return self._click_best_result_and_extract(best_result)
            
            # 울산 동구 결과가 없는 경우
            else:
                print("❌ 울산 동구 결과가 없음 - 식당을 찾지 못한 것으로 판단")
                return None
            
        except Exception as e:
            print(f"다중 결과 처리 중 오류: {e}")
            return None

    def _click_best_result_and_extract(self, best_result):
        """최적 결과 클릭하여 전화번호 추출"""
        try:
            print("최적 결과 클릭 중...")
            self.driver.switch_to.frame("searchIframe")
            
            # 인덱스로 해당 결과를 다시 찾아서 클릭
            clickable_results = self.driver.find_elements(By.CSS_SELECTOR, "li.VLTHu.OW9LQ a.place_bluelink")
            if not clickable_results:
                clickable_results = self.driver.find_elements(By.CSS_SELECTOR, ".place_bluelink")
            
            if best_result['index'] < len(clickable_results):
                print(f"클릭할 요소 찾음: {clickable_results[best_result['index']].text}")
                clickable_results[best_result['index']].click()
                
                # 로딩 대기
                wait_time = 2.0 if platform.system() == "Darwin" else 3.0
                time.sleep(wait_time)
                
                # 메인 페이지로 복귀
                self.driver.switch_to.default_content()
                
                # 새로 생긴 iframe에서 전화번호 찾기
                print("새로 생긴 iframe에서 전화번호 찾기...")
                return self.extract_phone_number_from_detail()
            else:
                print(f"인덱스 {best_result['index']}에 해당하는 클릭 가능한 요소를 찾을 수 없음")
                self.driver.switch_to.default_content()
                return None
                
        except Exception as e:
            print(f"최적 결과 클릭 중 오류: {e}")
            self.driver.switch_to.default_content()
            return None

    def _process_single_result(self):
        """단일 검색 결과 처리"""
        try:
            print("=== 단일 결과 처리 시작 ===")
            
            # 메인 페이지로 복귀
            self.driver.switch_to.default_content()
            
            # 단일 결과에서 전화번호 추출 시도
            phone_number = self.extract_phone_number_direct()
            if phone_number:
                return phone_number
            
            # 단일 결과 클릭하여 상세 정보 로드 시도
            phone_number = self._click_single_result_and_extract()
            if phone_number:
                return phone_number
            
            print("❌ 단일 결과에서 전화번호를 찾을 수 없음")
            return None
            
        except Exception as e:
            print(f"❌ 단일 결과 처리 중 오류: {e}")
            self.driver.switch_to.default_content()
            return None

    def _click_single_result_and_extract(self):
        """단일 결과 클릭하여 전화번호 추출"""
        try:
            print("단일 결과 클릭 시도...")
            
            # searchIframe으로 다시 전환
            self.driver.switch_to.frame("searchIframe")
            
            # 클릭 가능한 링크 찾기
            clickable_links = self.driver.find_elements(By.CSS_SELECTOR, "a.place_bluelink")
            if not clickable_links:
                clickable_links = self.driver.find_elements(By.CSS_SELECTOR, "li.VLTHu.OW9LQ a")
            
            if clickable_links:
                print(f"클릭 가능한 링크 {len(clickable_links)}개 발견")
                clickable_links[0].click()
                print("단일 결과 클릭 완료")
                
                # 로딩 대기
                time.sleep(2)
                
                # 메인 페이지로 복귀
                self.driver.switch_to.default_content()
                
                # 상세 페이지에서 전화번호 추출
                return self.extract_phone_number_from_detail()
            else:
                print("❌ 클릭 가능한 링크를 찾을 수 없음")
                self.driver.switch_to.default_content()
                return None
                
        except Exception as e:
            print(f"❌ 단일 결과 클릭 중 오류: {e}")
            self.driver.switch_to.default_content()
            return None

    def _process_multiple_results(self):
        """다중 검색 결과 처리"""
        try:
            print("=== 다중 결과 처리 시작 ===")
            
            # searchIframe에서 검색 결과 다시 찾기
            results = self.driver.find_elements(By.CSS_SELECTOR, "li.VLTHu.OW9LQ")
            if not results:
                results = self.driver.find_elements(By.CSS_SELECTOR, ".place_bluelink")
            
            if not results:
                print("❌ 다중 결과에서 검색 결과 요소를 찾을 수 없음")
                self.driver.switch_to.default_content()
                return None
            
            print(f"다중 결과 {len(results)}개 발견")
            
            # 상위 3개 결과만 확인
            top_results = results[:3]
            best_result_index = None
            best_score = -1
            best_address = ""
            
            for i, result in enumerate(top_results):
                try:
                    print(f"결과 {i+1} 확인 중...")
                    
                    # searchIframe 내부에서 해당 결과의 주소 정보 찾기
                    try:
                        # searchIframe으로 다시 전환
                        self.driver.switch_to.frame("searchIframe")
                        
                        # 해당 결과 내에서 span.Pb4bU 찾기
                        address_elements = result.find_elements(By.CSS_SELECTOR, "span.Pb4bU")
                        
                        if address_elements:
                            search_address = address_elements[0].text.strip()
                            print(f"결과 {i+1}에서 span.Pb4bU 발견: {search_address}")
                            
                            # 메인 페이지로 복귀
                            self.driver.switch_to.default_content()
                            
                            # 주소 유사도 비교
                            score = self.compare_address_similarity(search_address)
                            print(f"주소 유사도 점수: {score}")
                            
                            if score > best_score:
                                best_score = score
                                best_result_index = i
                                best_address = search_address
                        else:
                            print(f"결과 {i+1}에서 span.Pb4bU를 찾을 수 없음")
                            # 메인 페이지로 복귀
                            self.driver.switch_to.default_content()
                        
                    except Exception as e:
                        print(f"결과 {i+1} 주소 확인 중 오류: {e}")
                        self.driver.switch_to.default_content()
                        continue
                    
                except Exception as e:
                    print(f"결과 {i+1} 처리 중 오류: {e}")
                    self.driver.switch_to.default_content()
                    continue
            
            # 최적 결과에서 전화번호 추출
            if best_result_index is not None:
                print(f"최적 결과 선택 (인덱스: {best_result_index}, 점수: {best_score})")
            else:
                # 모든 결과의 주소 유사도 점수가 0인 경우, 첫 번째 결과를 선택
                print("모든 결과의 주소 유사도 점수가 0입니다. 첫 번째 결과를 선택합니다.")
                best_result_index = 0
                best_score = 0
                
                # 첫 번째 결과의 주소 정보 가져오기
                try:
                    self.driver.switch_to.frame("searchIframe")
                    first_result = top_results[0]
                    address_elements = first_result.find_elements(By.CSS_SELECTOR, "span.Pb4bU")
                    if address_elements:
                        best_address = address_elements[0].text.strip()
                        print(f"첫 번째 결과 주소: {best_address}")
                    self.driver.switch_to.default_content()
                except Exception as e:
                    print(f"첫 번째 결과 주소 가져오기 중 오류: {e}")
                    self.driver.switch_to.default_content()
            
            if best_result_index is not None:
                print(f"최적 결과 선택 (인덱스: {best_result_index}, 점수: {best_score})")
                
                # 최적 결과의 주소를 current_collected_address에 저장
                if best_address:
                    self.current_collected_address = best_address
                    print(f"최적 결과 주소 저장: {best_address}")
                
                # 최적 결과 클릭하여 상세 정보 로드
                try:
                    print("최적 결과 클릭 중...")
                    self.driver.switch_to.frame("searchIframe")
                    
                    # 인덱스로 해당 결과를 다시 찾아서 클릭
                    clickable_results = self.driver.find_elements(By.CSS_SELECTOR, "li.VLTHu.OW9LQ a.place_bluelink")
                    if not clickable_results:
                        clickable_results = self.driver.find_elements(By.CSS_SELECTOR, ".place_bluelink")
                    
                    if best_result_index < len(clickable_results):
                        print(f"클릭할 요소 찾음: {clickable_results[best_result_index].text}")
                        clickable_results[best_result_index].click()
                        # 네이버 차단 방지를 위한 대기 시간
                        wait_time = 2.0 if platform.system() == "Darwin" else 3.0
                        time.sleep(wait_time)  # 로딩 대기
                        
                        # 메인 페이지로 복귀
                        self.driver.switch_to.default_content()
                        
                        # 새로 생긴 iframe에서 전화번호 찾기
                        print("새로 생긴 iframe에서 전화번호 찾기...")
                        return self.extract_phone_number_from_detail()
                    else:
                        print(f"인덱스 {best_result_index}에 해당하는 클릭 가능한 요소를 찾을 수 없음")
                        self.driver.switch_to.default_content()
                        return None
                    
                except Exception as e:
                    print(f"최적 결과 클릭 중 오류: {e}")
                    self.driver.switch_to.default_content()
                    return None
            else:
                print("적절한 결과를 찾을 수 없음")
                return None
                
        except Exception as e:
            print(f"다중 결과 처리 중 오류: {e}")
            return None
    
    def compare_address_similarity_with_jibun(self, jibun_address):
        """구주소(지번) 기반 주소 유사도 비교"""
        try:
            original_address = self.current_original_address
            if not original_address:
                return 0
            
            print(f"원본 주소: {original_address}")
            print(f"검색 구주소: {jibun_address}")
            
            # 원본 주소에서 동/리 추출
            orig_parts = original_address.split()
            orig_dong_ri = None
            for part in orig_parts:
                if part.endswith('동') or part.endswith('리'):
                    orig_dong_ri = part
                    break
            
            # 검색 구주소에서 동/리 추출
            jibun_parts = jibun_address.split()
            jibun_dong_ri = None
            for part in jibun_parts:
                if part.endswith('동') or part.endswith('리'):
                    jibun_dong_ri = part
                    break
            
            score = 0
            
            # 동/리 정확히 일치 (가장 중요)
            if orig_dong_ri and jibun_dong_ri and orig_dong_ri == jibun_dong_ri:
                score += 10
            
            # 시/도 레벨 비교
            if len(orig_parts) > 0 and len(jibun_parts) > 0:
                if orig_parts[0] in jibun_address:
                    score += 1
            
            # 시/군 레벨 비교
            if len(orig_parts) > 1 and len(jibun_parts) > 1:
                if orig_parts[1] in jibun_address:
                    score += 1
            
            # 번지 번호 비교 (상세 주소)
            orig_bunji = None
            for part in orig_parts:
                if '-' in part and part.replace('-', '').isdigit():
                    orig_bunji = part
                    break
            
            jibun_bunji = None
            for part in jibun_parts:
                if '-' in part and part.replace('-', '').isdigit():
                    jibun_bunji = part
                    break
            
            if orig_bunji and jibun_bunji and orig_bunji == jibun_bunji:
                score += 5
            
            print(f"최종 유사도 점수: {score}")
            return score
            
        except Exception as e:
            print(f"구주소 유사도 비교 중 오류: {e}")
            return 0
    
    def compare_address_similarity(self, search_address):
        """주소 유사도 비교 (기존 방식)"""
        try:
            original_address = self.current_original_address
            if not original_address:
                return 0
            
            print(f"원본 주소: {original_address}")
            print(f"검색 주소: {search_address}")
            
            orig_parts = original_address.split()
            score = 0
            
            # 시/도 레벨 비교
            if len(orig_parts) > 0:
                if orig_parts[0] in search_address:
                    score += 1
            
            # 시/군 레벨 비교
            if len(orig_parts) > 1:
                if orig_parts[1] in search_address:
                    score += 1
            
            # 동/리 레벨 비교 (가장 중요)
            orig_dong_ri = None
            for part in orig_parts:
                if part.endswith('동') or part.endswith('리'):
                    orig_dong_ri = part
                    break
            
            if orig_dong_ri:
                if orig_dong_ri in search_address:
                    score += 5
                else:
                    dong_ri_base = orig_dong_ri.replace('동', '').replace('리', '')
                    if dong_ri_base in search_address:
                        score += 3
            
            # 상세 주소 비교
            if len(orig_parts) > 3:
                detail_parts = orig_parts[3:]
                for detail in detail_parts:
                    if detail in search_address:
                        score += 1
            
            print(f"최종 유사도 점수: {score}")
            return score
            
        except Exception as e:
            print(f"주소 비교 중 오류: {e}")
            return 0
    
    def extract_phone_number(self, result_element):
        """검색 결과에서 전화번호 추출"""
        try:
            # entryIframe에서 전화번호 찾기
            try:
                entry_iframes = self.driver.find_elements(By.ID, "entryIframe")
                
                if entry_iframes:
                    self.driver.switch_to.frame("entryIframe")
                    phone_elements = self.driver.find_elements(By.CSS_SELECTOR, "span.xlx7Q")
                    
                    if phone_elements:
                        phone_number = phone_elements[0].text.strip()
                        if phone_number and len(phone_number) > 5:
                            print(f"✅ entryIframe에서 전화번호 발견: {phone_number}")
                            
                            # 주소 정보 수집 (구주소와 신주소)
                            try:
                                # a.PkgBl 클릭하여 상세 주소 정보 가져오기
                                pkgbl_elements = self.driver.find_elements(By.CSS_SELECTOR, "a.PkgBl")
                                if pkgbl_elements:
                                    pkgbl_elements[0].click()
                                    time.sleep(1.0)
                                    
                                                                        # 구주소(지번) 정보만 가져오기
                                    jibun_elements = self.driver.find_elements(By.CSS_SELECTOR, "div.nQ7Lh")
                                    jibun_address = ""
                                    
                                    for element in jibun_elements:
                                        text = element.text
                                        if "지번" in text:
                                            jibun_address = text.replace("지번", "").strip()
                                            break
                                    
                                    if jibun_address:
                                        self.current_collected_jibun_address = jibun_address
                                        self.current_collected_address = jibun_address  # 수집된 주소도 구주소로 설정
                                        print(f"구주소 정보 수집: {jibun_address}")
                                    else:
                                        # a.PkgBl이 없는 경우 기존 방식 사용
                                        address_selectors = [
                                            "span.LDgIH", "span.address", "span.Pb4bU",
                                            "div.address", "span[data-testid='address']", ".address"
                                        ]
                                        
                                        collected_address = ""
                                        for selector in address_selectors:
                                            address_elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                                            if address_elements:
                                                collected_address = address_elements[0].text.strip()
                                                print(f"주소 정보 수집: {collected_address}")
                                                break
                                        
                                        if collected_address:
                                            self.current_collected_address = collected_address
                                        
                            except Exception as addr_e:
                                print(f"주소 정보 수집 중 오류: {addr_e}")
                            
                            self.driver.switch_to.default_content()
                            return phone_number
                    
                    self.driver.switch_to.default_content()
                
            except Exception as e:
                print(f"entryIframe 처리 중 오류: {e}")
                self.driver.switch_to.default_content()
            
            # searchIframe에서 전화번호 찾기
            try:
                self.driver.switch_to.frame("searchIframe")
                phone_elements = self.driver.find_elements(By.CSS_SELECTOR, "span.xlx7Q")
                
                if phone_elements:
                    phone_number = phone_elements[0].text.strip()
                    if phone_number and len(phone_number) > 5:
                        print(f"✅ 전화번호 발견: {phone_number}")
                        self.logger.info(f"✅ searchIframe에서 전화번호 발견: {phone_number}")
                        
                        # 주소 정보 수집
                        try:
                            address_selectors = [
                                "span.LDgIH", "span.address", "span.Pb4bU",
                                "div.address", "span[data-testid='address']", ".address"
                            ]
                            
                            collected_address = ""
                            for selector in address_selectors:
                                address_elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                                if address_elements:
                                    collected_address = address_elements[0].text.strip()
                                    self.logger.info(f"주소 정보 수집: {collected_address}")
                                    break
                            
                            if collected_address:
                                self.current_collected_address = collected_address
                                
                        except Exception as addr_e:
                            self.logger.error(f"주소 정보 수집 중 오류: {addr_e}")
                        
                        self.driver.switch_to.default_content()
                        return phone_number
                
                self.driver.switch_to.default_content()
                
            except Exception as e:
                print(f"searchIframe 처리 중 오류: {e}")
                self.logger.error(f"searchIframe 처리 중 오류: {e}")
                self.driver.switch_to.default_content()
            
            # 메인 페이지에서 직접 찾기
            phone_elements = self.driver.find_elements(By.CSS_SELECTOR, "span.xlx7Q")
            
            if phone_elements:
                phone_number = phone_elements[0].text.strip()
                if phone_number and len(phone_number) > 5:
                    print(f"✅ 전화번호 발견: {phone_number}")
                    self.logger.info(f"✅ 메인 페이지에서 전화번호 발견: {phone_number}")
                    
                    # 주소 정보 수집
                    try:
                        address_selectors = [
                            "span.LDgIH", "span.address", "span.Pb4bU",
                            "div.address", "span[data-testid='address']", ".address"
                        ]
                        
                        collected_address = ""
                        for selector in address_selectors:
                            address_elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                            if address_elements:
                                collected_address = address_elements[0].text.strip()
                                self.logger.info(f"주소 정보 수집: {collected_address}")
                                break
                        
                        if collected_address:
                            self.current_collected_address = collected_address
                            
                    except Exception as addr_e:
                        self.logger.error(f"주소 정보 수집 중 오류: {addr_e}")
                    
                    return phone_number
            
            print("전화번호를 찾을 수 없음")
            self.logger.warning("전화번호를 찾을 수 없음")
            return None
            
        except Exception as e:
            self.logger.error(f"전화번호 추출 중 오류: {e}")
            print(f"전화번호 추출 중 오류: {e}")
            return None
    
    def extract_phone_number_direct(self):
        """직접 전화번호 추출"""
        try:
            # entryIframe에서 전화번호 찾기
            try:
                entry_iframes = self.driver.find_elements(By.ID, "entryIframe")
                
                if entry_iframes:
                    self.driver.switch_to.frame("entryIframe")
                    phone_elements = self.driver.find_elements(By.CSS_SELECTOR, "span.xlx7Q")
                    
                    if phone_elements:
                        phone_number = phone_elements[0].text.strip()
                        if phone_number and len(phone_number) > 5:
                            print(f"✅ 전화번호 발견: {phone_number}")
                            self.logger.info(f"✅ entryIframe에서 전화번호 발견: {phone_number}")
                            
                            # 주소 정보 수집
                            try:
                                address_elements = self.driver.find_elements(By.CSS_SELECTOR, "span.address")
                                if address_elements:
                                    collected_address = address_elements[0].text.strip()
                                    self.current_collected_address = collected_address
                                    self.logger.info(f"주소 정보 수집: {collected_address}")
                            except Exception as addr_e:
                                self.logger.error(f"주소 정보 수집 중 오류: {addr_e}")
                            
                            self.driver.switch_to.default_content()
                            return phone_number
                    
                    self.driver.switch_to.default_content()
                
            except Exception as e:
                print(f"entryIframe 처리 중 오류: {e}")
                self.driver.switch_to.default_content()
            
            # 메인 페이지에서 직접 찾기
            phone_elements = self.driver.find_elements(By.CSS_SELECTOR, "span.xlx7Q")
            
            if phone_elements:
                phone_number = phone_elements[0].text.strip()
                if phone_number and len(phone_number) > 5:
                    print(f"✅ 메인 페이지에서 전화번호 발견: {phone_number}")
                    
                    # 주소 정보 수집
                    try:
                        address_elements = self.driver.find_elements(By.CSS_SELECTOR, "span.address")
                        if address_elements:
                            collected_address = address_elements[0].text.strip()
                            self.current_collected_address = collected_address
                            print(f"주소 정보 수집: {collected_address}")
                    except Exception as addr_e:
                        print(f"주소 정보 수집 중 오류: {addr_e}")
                    
                    return phone_number
            
            print("전화번호를 찾을 수 없음")
            return None
            
        except Exception as e:
            self.logger.error(f"직접 전화번호 추출 중 오류: {e}")
            print(f"직접 전화번호 추출 중 오류: {e}")
            return None
    

    
    def extract_phone_number(self, result_element):
        """검색 결과에서 전화번호 추출"""
        try:
            # 전화번호 버튼 찾기
            phone_buttons = result_element.find_elements(By.CSS_SELECTOR, "a[href*='tel:']")
            if phone_buttons:
                phone_href = phone_buttons[0].get_attribute('href')
                phone_number = phone_href.replace('tel:', '').strip()
                print(f"전화번호 발견: {phone_number}")
                return phone_number
            
            return None
        except Exception as e:
            print(f"전화번호 추출 중 오류: {e}")
            return None

    def extract_phone_number_direct(self):
        """직접 전화번호 추출 (entryIframe에서)"""
        try:
            # entryIframe으로 전환
            try:
                iframe = WebDriverWait(self.driver, 5).until(
                    EC.presence_of_element_located((By.ID, "entryIframe"))
                )
                self.driver.switch_to.frame(iframe)
                
                # 전화번호 버튼 찾기
                phone_buttons = self.driver.find_elements(By.CSS_SELECTOR, "a[href*='tel:']")
                if phone_buttons:
                    phone_href = phone_buttons[0].get_attribute('href')
                    phone_number = phone_href.replace('tel:', '').strip()
                    print(f"직접 전화번호 발견: {phone_number}")
                    self.driver.switch_to.default_content()
                    return phone_number
                
                self.driver.switch_to.default_content()
                return None
                
            except Exception as e:
                print(f"entryIframe 처리 중 오류: {e}")
                self.driver.switch_to.default_content()
                return None
                
        except Exception as e:
            print(f"직접 전화번호 추출 중 오류: {e}")
            return None

    def extract_phone_number_from_detail(self):
        """상세 페이지에서 전화번호 추출"""
        try:
            # 메인 페이지로 복귀
            self.driver.switch_to.default_content()
            print("메인 페이지로 복귀 완료")
            
            # 다중 결과에서 클릭한 후 새로운 iframe이 로드되었을 수 있으므로
            # 여러 iframe에서 a.BfF3H를 찾아보기
            print("다중 iframe에서 a.BfF3H 찾기 시도...")
            
            # 1단계: entryIframe에서 a.BfF3H 찾기
            try:
                print("entryIframe에서 a.BfF3H 찾기...")
                entry_iframes = self.driver.find_elements(By.ID, "entryIframe")
                if entry_iframes:
                    self.driver.switch_to.frame("entryIframe")
                    print("entryIframe으로 전환 완료")
                    
                    bf3h_elements = self.driver.find_elements(By.CSS_SELECTOR, "a.BfF3H")
                    if bf3h_elements:
                        print(f"✅ entryIframe에서 a.BfF3H 발견: {len(bf3h_elements)}개")
                        bf3h_elements[0].click()
                        print("a.BfF3H 클릭 완료")
                        # 네이버 차단 방지를 위한 대기 시간
                        wait_time = 1.5 if platform.system() == "Darwin" else 2.0
                        print(f"{wait_time}초 대기 중... (네이버 차단 방지)")
                        time.sleep(wait_time)  # 대기
                        
                        # entryIframe 내에서 바로 div._YI7T.kH0zp 찾기 (iframe 전환하지 않음)
                        print("entryIframe 내에서 div._YI7T.kH0zp 안의 em 태그에서 전화번호 찾기...")
                        div_elements = self.driver.find_elements(By.CSS_SELECTOR, "div._YI7T.kH0zp")
                        print(f"entryIframe에서 div._YI7T.kH0zp 찾은 개수: {len(div_elements)}")
                        
                        if div_elements:
                            for div in div_elements:
                                em_elements = div.find_elements(By.CSS_SELECTOR, "em")
                                print(f"em 태그 찾은 개수: {len(em_elements)}")
                                
                                for em in em_elements:
                                    phone_text = em.text.strip()
                                    print(f"🔍 em 태그에서 찾은 텍스트: '{phone_text}'")
                                    if phone_text and len(phone_text) > 8 and '-' in phone_text:
                                        print(f"✅ em 태그에서 전화번호 발견: {phone_text}")
                                        self.driver.switch_to.default_content()
                                        return phone_text
                        else:
                            print("❌ entryIframe에서 div._YI7T.kH0zp를 찾을 수 없음")
                        
                        # 메인 페이지로 복귀
                        self.driver.switch_to.default_content()
                        print("entryIframe에서 div._YI7T.kH0zp 안의 em 태그에서 전화번호를 찾을 수 없음")
                        return None
                    
                    # 메인 페이지로 복귀
                    self.driver.switch_to.default_content()
                    print("entryIframe에서 a.BfF3H를 찾을 수 없음")
                
            except Exception as e:
                print(f"entryIframe 처리 중 오류: {e}")
                self.driver.switch_to.default_content()
            
            # 2단계: searchIframe에서 a.BfF3H 찾기
            try:
                print("searchIframe에서 a.BfF3H 찾기...")
                self.driver.switch_to.frame("searchIframe")
                print("searchIframe으로 전환 완료")
                
                bf3h_elements = self.driver.find_elements(By.CSS_SELECTOR, "a.BfF3H")
                if bf3h_elements:
                    print(f"✅ searchIframe에서 a.BfF3H 발견: {len(bf3h_elements)}개")
                    bf3h_elements[0].click()
                    print("a.BfF3H 클릭 완료")
                    # 네이버 차단 방지를 위한 대기 시간
                    wait_time = 1.5 if platform.system() == "Darwin" else 2.0
                    print(f"{wait_time}초 대기 중... (네이버 차단 방지)")
                    time.sleep(wait_time)  # 대기
                    
                    # searchIframe 내에서 바로 div._YI7T.kH0zp 찾기 (iframe 전환하지 않음)
                    print("searchIframe 내에서 div._YI7T.kH0zp 안의 em 태그에서 전화번호 찾기...")
                    div_elements = self.driver.find_elements(By.CSS_SELECTOR, "div._YI7T.kH0zp")
                    print(f"searchIframe에서 div._YI7T.kH0zp 찾은 개수: {len(div_elements)}")
                    
                    if div_elements:
                        for div in div_elements:
                            em_elements = div.find_elements(By.CSS_SELECTOR, "em")
                            print(f"em 태그 찾은 개수: {len(em_elements)}")
                            
                            for em in em_elements:
                                phone_text = em.text.strip()
                                print(f"🔍 em 태그에서 찾은 텍스트: '{phone_text}'")
                                if phone_text and len(phone_text) > 8 and '-' in phone_text:
                                    print(f"✅ em 태그에서 전화번호 발견: {phone_text}")
                                    self.driver.switch_to.default_content()
                                    return phone_text
                    else:
                        print("❌ searchIframe에서 div._YI7T.kH0zp를 찾을 수 없음")
                    
                    # 메인 페이지로 복귀
                    self.driver.switch_to.default_content()
                    print("searchIframe에서 div._YI7T.kH0zp 안의 em 태그에서 전화번호를 찾을 수 없음")
                    return None
                
                # 메인 페이지로 복귀
                self.driver.switch_to.default_content()
                print("searchIframe에서 a.BfF3H를 찾을 수 없음")
                
            except Exception as e:
                print(f"searchIframe 처리 중 오류: {e}")
                self.driver.switch_to.default_content()
            
            # 3단계: 메인 페이지에서 a.BfF3H 찾기
            try:
                print("메인 페이지에서 a.BfF3H 찾기...")
                bf3h_elements = self.driver.find_elements(By.CSS_SELECTOR, "a.BfF3H")
                if bf3h_elements:
                    print(f"✅ 메인 페이지에서 a.BfF3H 발견: {len(bf3h_elements)}개")
                    bf3h_elements[0].click()
                    print("a.BfF3H 클릭 완료")
                    # 네이버 차단 방지를 위한 대기 시간
                    wait_time = 1.5 if platform.system() == "Darwin" else 2.0
                    print(f"{wait_time}초 대기 중... (네이버 차단 방지)")
                    time.sleep(wait_time)  # 대기
                    
                    # BfF3H 클릭 후 나타나는 div에서 em 태그 찾기
                    print("BfF3H 클릭 후 div._YI7T.kH0zp 안의 em 태그에서 전화번호 찾기...")
                    div_elements = self.driver.find_elements(By.CSS_SELECTOR, "div._YI7T.kH0zp")
                    print(f"div._YI7T.kH0zp 찾은 개수: {len(div_elements)}")
                    
                    if div_elements:
                        for div in div_elements:
                            em_elements = div.find_elements(By.CSS_SELECTOR, "em")
                            print(f"em 태그 찾은 개수: {len(em_elements)}")
                            
                            for em in em_elements:
                                phone_text = em.text.strip()
                                print(f"🔍 em 태그에서 찾은 텍스트: '{phone_text}'")
                                if phone_text and len(phone_text) > 8 and '-' in phone_text:
                                    print(f"✅ em 태그에서 전화번호 발견: {phone_text}")
                                    return phone_text
                    else:
                        print("❌ div._YI7T.kH0zp를 찾을 수 없음")
                    
                    print("div._YI7T.kH0zp 안의 em 태그에서 전화번호를 찾을 수 없음")
                    return None
                else:
                    print("메인 페이지에서 a.BfF3H를 찾을 수 없음")
                    
            except Exception as e:
                print(f"메인 페이지 처리 중 오류: {e}")
            
            # a.BfF3H를 찾지 못한 경우 기존 방식으로 전화번호 찾기
            print("a.BfF3H를 찾지 못했으므로 기존 방식으로 전화번호 찾기...")
            
            # entryIframe에서 전화번호 찾기
            try:
                iframe = WebDriverWait(self.driver, 5).until(
                    EC.presence_of_element_located((By.ID, "entryIframe"))
                )
                self.driver.switch_to.frame(iframe)
                
                # 전화번호 버튼 찾기
                phone_buttons = self.driver.find_elements(By.CSS_SELECTOR, "a[href*='tel:']")
                if phone_buttons:
                    phone_href = phone_buttons[0].get_attribute('href')
                    phone_number = phone_href.replace('tel:', '').strip()
                    print(f"상세 페이지에서 전화번호 발견: {phone_number}")
                    self.driver.switch_to.default_content()
                    return phone_number
                
                self.driver.switch_to.default_content()
                
            except Exception as e:
                print(f"entryIframe에서 전화번호 찾기 중 오류: {e}")
                self.driver.switch_to.default_content()
            
            # span.xlx7Q에서 전화번호 찾기
            try:
                span_elements = self.driver.find_elements(By.CSS_SELECTOR, "span.xlx7Q")
                for span in span_elements:
                    phone_text = span.text.strip()
                    if phone_text and len(phone_text) > 8 and '-' in phone_text:
                        print(f"span.xlx7Q에서 전화번호 발견: {phone_text}")
                        return phone_text
                
            except Exception as e:
                print(f"span.xlx7Q에서 전화번호 찾기 중 오류: {e}")
            
            print("전화번호를 찾을 수 없음")
            return None
            
        except Exception as e:
            self.logger.error(f"상세 페이지에서 전화번호 추출 중 오류: {e}")
            print(f"상세 페이지에서 전화번호 추출 중 오류: {e}")
            return None

    def get_update_status(self, original_phone, new_phone, update_status):
        """업데이트 상태 및 코멘트 생성"""
        try:
            if not original_phone and new_phone:
                return f"기존에 전화번호가 없었는데 새로 발견: {new_phone}"
            elif original_phone and new_phone:
                if original_phone == new_phone:
                    return "기존 전화번호와 동일합니다"
                else:
                    return f"전화번호가 변경되었습니다 (기존: {original_phone} → 새: {new_phone})"
            elif not new_phone:
                if update_status == "결과없음":
                    return "네이버 지도에서 해당 업체를 찾을 수 없었습니다"
                elif update_status == "MULTIPLE_RESULTS_NO_PHONE":
                    return "네이버 지도에서 여러 결과가 나왔지만 전화번호 정보가 없었습니다"
                else:
                    return f"전화번호 수집 실패: {update_status}"
            else:
                return "처리 중 오류가 발생했습니다"
        except Exception as e:
            return f"상태 생성 중 오류: {str(e)}"
    
    def get_address_similarity_score(self, original_address, new_phone):
        """주소 유사도 점수 계산"""
        try:
            # MULTIPLE_RESULTS_NO_PHONE 케이스에서도 수집된 주소가 있으면 점수 계산
            if hasattr(self, 'current_collected_address') and self.current_collected_address:
                return self.compare_address_similarity(self.current_collected_address)
            else:
                return 0
        except Exception as e:
            print(f"주소 유사도 점수 계산 중 오류: {e}")
            return 0
    
    def get_collected_address(self, new_phone):
        """수집된 주소 반환 (구주소만)"""
        try:
            # MULTIPLE_RESULTS_NO_PHONE 케이스에서도 수집된 주소 반환
            # 구주소가 있으면 구주소 반환
            if hasattr(self, 'current_collected_jibun_address') and self.current_collected_jibun_address:
                return self.current_collected_jibun_address
            
            # 구주소가 없으면 수집된 주소 반환
            if hasattr(self, 'current_collected_address') and self.current_collected_address:
                return self.current_collected_address
            
            # 전화번호가 있는 경우에만 이 메시지 반환
            if new_phone:
                return "전화번호 수집 성공 (주소 정보 없음)"
            else:
                return ""
        except Exception as e:
            return f"주소 수집 중 오류: {str(e)}"
    
    def crawl_phone_numbers(self, test_count=None, start_from_index=None):
        """전화번호 크롤링 메인 함수"""
        try:
            # CSV 파일 읽기
            csv_file = self.config['target_file']
            print(f"CSV 파일 읽기: {csv_file}")
            df = pd.read_csv(csv_file)
            
            # 설정에서 컬럼명 가져오기
            business_name_col = self.config['columns']['business_name']
            address_col = self.config['columns']['address']
            phone_col = self.config['columns'].get('phone', None)
            
            # 컬럼 존재 확인
            if business_name_col not in df.columns:
                raise ValueError(f"사업장명 컬럼 '{business_name_col}'을 찾을 수 없습니다.")
            if address_col not in df.columns:
                raise ValueError(f"주소 컬럼 '{address_col}'을 찾을 수 없습니다.")
            
            print(f"✅ CSV 구조 확인 완료")
            print(f"   사업장명 컬럼: {business_name_col}")
            print(f"   주소 컬럼: {address_col}")
            if phone_col:
                print(f"   전화번호 컬럼: {phone_col}")
            
            # 순번 추가
            df['인덱스'] = range(1, len(df) + 1)
            
            # 전체 데이터 또는 테스트 데이터 선택
            if test_count:
                test_df = df.head(test_count).copy()
                print(f"테스트 데이터 {test_count}개 선택")
            else:
                test_df = df.copy()
                print(f"전체 데이터 {len(df)}개 선택")
            
            # 기존 결과 파일 확인 및 재시작 처리
            existing_results = self.check_existing_results()
            start_index = 0
            
            if start_from_index is not None:
                # 사용자가 지정한 인덱스부터 시작
                start_index = start_from_index - 1  # 0-based 인덱스로 변환
                print(f"🚀 지정된 인덱스 {start_from_index}부터 크롤링 시작")
                self.logger.info(f"🚀 지정된 인덱스 {start_from_index}부터 크롤링 시작")
            elif existing_results and len(existing_results) > 0:
                # 기존 결과가 있으면 마지막 처리된 인덱스 다음부터 시작
                last_processed_index = max(int(row['인덱스']) for row in existing_results if pd.notna(row['인덱스']))
                start_index = last_processed_index
                print(f"🔄 기존 결과 발견! 인덱스 {last_processed_index + 1}부터 재시작")
                self.logger.info(f"🔄 기존 결과 발견! 인덱스 {last_processed_index + 1}부터 재시작")
            else:
                # 새로 시작
                print(f"🆕 새로운 크롤링 시작")
                self.logger.info(f"🆕 새로운 크롤링 시작")
            
            # 결과 파일 초기화 (기존 결과가 있으면 append 모드)
            self.initialize_result_file(append_mode=start_index > 0)
            
            # 시작 인덱스부터 처리
            for index, row in test_df.iloc[start_index:].iterrows():
                try:
                    print(f"\n{'='*50}")
                    total_count = len(test_df)
                    business_name = row[business_name_col]
                    print(f"처리 중: {index+1}/{total_count} - {business_name}")
                    self.logger.info(f"처리 중: {index+1}/{total_count} - {business_name}")
                    
                    # 주소에서 동이름 추출
                    address = row[address_col]
                    if pd.isna(address) or address == '':
                        print("주소 정보 없음")
                        result_data = {
                            '인덱스': row['인덱스'],
                            '사업장명': business_name,
                            '기존주소': address,
                            '기존전화번호': row[phone_col] if phone_col else '',
                            '새전화번호': None,
                            '업데이트상태': '주소정보없음',
                            '주소유사도점수': 0,
                            '수집된주소': ""
                        }
                        self.save_single_result(result_data)
                        continue
                    
                    # 동이름 추출
                    dong_name = self.extract_dong_name(address)
                    if not dong_name:
                        print("동이름 추출 실패")
                        result_data = {
                            '인덱스': row['인덱스'],
                            '사업장명': business_name,
                            '기존주소': address,
                            '기존전화번호': row[phone_col] if phone_col else '',
                            '새전화번호': None,
                            '업데이트상태': '동이름추출실패',
                            '주소유사도점수': 0,
                            '수집된주소': ""
                        }
                        self.save_single_result(result_data)
                        continue
                    
                    print(f"동이름: {dong_name}")
                    
                    # 네이버 지도 검색 및 전화번호 추출
                    new_phone = self.search_and_extract_phone(business_name, dong_name, original_address=address)
                    
                    # 결과 처리
                    if new_phone == "MULTIPLE_RESULTS_NO_PHONE":
                        update_status = "MULTIPLE_RESULTS_NO_PHONE"
                        new_phone_for_save = None
                        # MULTIPLE_RESULTS_NO_PHONE 케이스에서도 수집된 주소 정보 유지
                        # current_collected_address는 이미 설정되어 있음
                    elif new_phone:
                        update_status = "true"
                        new_phone_for_save = new_phone
                    else:
                        update_status = "결과없음"
                        new_phone_for_save = None
                    
                    # 결과 데이터 생성
                    result_data = {
                        '인덱스': row['인덱스'],
                        '사업장명': business_name,
                        '기존주소': address,
                        '기존전화번호': row[phone_col] if phone_col else '',
                        '새전화번호': new_phone_for_save,
                        '업데이트상태': self.get_update_status(row[phone_col] if phone_col else '', new_phone_for_save, update_status),
                        '주소유사도점수': self.get_address_similarity_score(address, new_phone_for_save),
                        '수집된주소': self.get_collected_address(new_phone_for_save)
                    }
                    
                    # 결과 저장
                    if self.save_single_result(result_data):
                        print(f"✅ 저장 완료")
                        self.logger.info(f"✅ 결과 저장 완료: {business_name}")
                    else:
                        print(f"❌ 저장 실패")
                        self.logger.error(f"❌ 결과 저장 실패: {business_name}")
                    
                    self.logger.info(f"결과: {update_status}")
                    if new_phone:
                        self.logger.info(f"새 전화번호: {new_phone}")
                    
                    # 네이버 차단 방지를 위한 랜덤 대기 시간 (전체 대기 시간 10초 이하로 제한)
                    base_wait = 1.5 if platform.system() == "Darwin" else 2.0
                    random_wait = random.uniform(0.3, 1.0)
                    wait_time = base_wait + random_wait
                    print(f"{wait_time:.1f}초 대기 중...")
                    self.logger.info(f"{wait_time:.1f}초 대기 중... (네이버 차단 방지 + 랜덤)")
                    time.sleep(wait_time)
                    
                    # 처리 카운트 증가
                    self.processed_count += 1
                    
                    # 진행 상황 표시 (10개마다)
                    if self.processed_count % 10 == 0:
                        print(f"🎯 {self.processed_count}개 처리됨")
                        self.logger.info(f"🎯 진행 상황: {self.processed_count}개 처리됨")
                    
                except Exception as e:
                    self.logger.error(f"행 처리 중 오류 발생: {e}")
                    print(f"행 처리 중 오류 발생: {e}")
                    
                    # 오류 데이터도 저장
                    error_data = {
                        '인덱스': row.get('인덱스', index+1),
                        '사업장명': row.get(business_name_col, '알 수 없음'),
                        '기존주소': row.get(address_col, ''),
                        '기존전화번호': row.get(phone_col, '') if phone_col else '',
                        '새전화번호': None,
                        '업데이트상태': f'오류 발생: {str(e)}',
                        '주소유사도점수': 0,
                        '수집된주소': ""
                    }
                    
                    if self.save_single_result(error_data):
                        print(f"✅ 오류 데이터 저장 완료: {row.get(business_name_col, '알 수 없음')}")
                        self.logger.info(f"✅ 오류 데이터 저장 완료: {row.get(business_name_col, '알 수 없음')}")
                    else:
                        print(f"❌ 오류 데이터 저장 실패: {row.get(business_name_col, '알 수 없음')}")
                        self.logger.error(f"❌ 오류 데이터 저장 실패: {row.get(business_name_col, '알 수 없음')}")
            
            # 크롤링 완료
            self.logger.info(f"전체 처리 완료: 총 {self.processed_count}개 처리됨")
            print(f"🎉 전체 처리 완료: 총 {self.processed_count}개 처리됨")
            print(f"📁 결과 파일: {self.result_file}")
            return f"총 {self.processed_count}개 처리 완료"
            
        except Exception as e:
            self.logger.error(f"전체 처리 중 오류: {e}")
            print(f"전체 처리 중 오류: {e}")
            return None
    
    def close(self):
        """브라우저 종료"""
        if self.driver:
            self.driver.quit()
            print("브라우저 종료")

    def analyze_failed_data(self, csv_file):
        """실패한 데이터 분석"""
        try:
            df = pd.read_csv(csv_file)
            
            # 실패 사유별 분류
            failed_categories = {
                '주소유사도_0': [],      # 주소유사도점수가 0인 경우
                '전화번호_수집실패': [],   # 전화번호를 찾을 수 없거나 수집 실패
                '잘못된_지역매칭': [],   # 울산 동구가 아닌 다른 지역으로 매칭
                '기타_실패': []          # 기타 실패 케이스
            }
            
            for index, row in df.iterrows():
                business_name = row['사업장명']
                address = row['기존주소']
                new_phone = row['새전화번호']
                similarity_score = row['주소유사도점수']
                collected_address = row['수집된주소']
                update_status = row['업데이트상태']
                
                # 실패 케이스 분류
                if pd.isna(new_phone) or new_phone == '':
                    # 전화번호 수집 실패
                    failed_categories['전화번호_수집실패'].append({
                        'index': row['인덱스'],
                        'business_name': business_name,
                        'address': address,
                        'reason': update_status
                    })
                elif similarity_score == 0:
                    # 주소 유사도 0 (잘못된 지역 매칭)
                    if '울산' not in str(collected_address) or '동구' not in str(collected_address):
                        failed_categories['잘못된_지역매칭'].append({
                            'index': row['인덱스'],
                            'business_name': business_name,
                            'address': address,
                            'collected_address': collected_address,
                            'reason': f"잘못된 지역 매칭: {collected_address}"
                        })
                    else:
                        failed_categories['주소유사도_0'].append({
                            'index': row['인덱스'],
                            'business_name': business_name,
                            'address': address,
                            'collected_address': collected_address,
                            'reason': f"주소 유사도 0: {collected_address}"
                        })
                elif similarity_score < 5:
                    # 주소 유사도가 낮은 경우
                    failed_categories['기타_실패'].append({
                        'index': row['인덱스'],
                        'business_name': business_name,
                        'address': address,
                        'similarity_score': similarity_score,
                        'collected_address': collected_address,
                        'reason': f"낮은 주소 유사도: {similarity_score}"
                    })
            
            # 결과 출력
            print("\n" + "="*60)
            print("📊 실패 데이터 분석 결과")
            print("="*60)
            
            total_failed = 0
            for category, items in failed_categories.items():
                print(f"\n🔴 {category}: {len(items)}개")
                total_failed += len(items)
                
                if len(items) > 0:
                    print("   상세 내역:")
                    for item in items[:5]:  # 처음 5개만 표시
                        print(f"   - {item['index']}: {item['business_name']} ({item['reason']})")
                    
                    if len(items) > 5:
                        print(f"   ... 외 {len(items) - 5}개")
            
            print(f"\n📈 총 실패 건수: {total_failed}개")
            print(f"📈 총 데이터 건수: {len(df)}개")
            print(f"📈 성공률: {((len(df) - total_failed) / len(df) * 100):.1f}%")
            
            return failed_categories
            
        except Exception as e:
            print(f"실패 데이터 분석 중 오류: {e}")
            self.logger.error(f"실패 데이터 분석 중 오류: {e}")
            return None
    
    def retry_failed_data(self, csv_file, category=None):
        """실패한 데이터 재시도"""
        try:
            # 실패 데이터 분석
            failed_categories = self.analyze_failed_data(csv_file)
            if not failed_categories:
                return None
            
            # 재시도할 카테고리 선택
            if category:
                if category not in failed_categories:
                    print(f"❌ 카테고리 '{category}'를 찾을 수 없습니다.")
                    return None
                target_items = failed_categories[category]
            else:
                # 모든 실패 데이터
                target_items = []
                for items in failed_categories.values():
                    target_items.extend(items)
            
            if not target_items:
                print("✅ 재시도할 실패 데이터가 없습니다.")
                return None
            
            print(f"\n🔄 {len(target_items)}개 실패 데이터 재시도 시작")
            
            # 원본 CSV 파일 읽기
            original_df = pd.read_csv(self.config['target_file'])
            
            # 결과 파일 초기화 (재시도용)
            timestamp = datetime.now().strftime("%y%m%d%H%M%S")
            retry_file = f'retry_crawling_{timestamp}.csv'
            
            # 헤더 설정
            headers = self.config['output_columns']
            with open(retry_file, 'w', encoding='utf-8-sig', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(headers)
            
            self.result_file = retry_file
            print(f"📁 재시도 결과 파일: {retry_file}")
            
            # 실패한 데이터만 재처리
            success_count = 0
            for i, item in enumerate(target_items):
                try:
                    print(f"\n{'='*50}")
                    print(f"재시도 중: {i+1}/{len(target_items)} - {item['business_name']}")
                    self.logger.info(f"재시도 중: {i+1}/{len(target_items)} - {item['business_name']}")
                    
                    # 원본 데이터에서 해당 행 찾기
                    original_row = original_df[original_df['인덱스'] == item['index']].iloc[0]
                    
                    # 주소에서 동이름 추출
                    address = original_row[self.config['columns']['address']]
                    if pd.isna(address) or address == '':
                        print("주소 정보 없음")
                        continue
                    
                    # 동이름 추출
                    dong_name = self.extract_dong_name(address)
                    if not dong_name:
                        print("동이름 추출 실패")
                        continue
                    
                    print(f"동이름: {dong_name}")
                    print(f"실패 사유: {item['reason']}")
                    
                    # 네이버 지도 검색 및 전화번호 추출 (재시도)
                    new_phone = self.search_and_extract_phone(
                        item['business_name'], 
                        dong_name, 
                        original_address=address
                    )
                    
                    # 결과 처리
                    if new_phone == "MULTIPLE_RESULTS_NO_PHONE":
                        update_status = "MULTIPLE_RESULTS_NO_PHONE"
                        new_phone_for_save = None
                    elif new_phone:
                        update_status = "재시도_성공"
                        new_phone_for_save = new_phone
                        success_count += 1
                    else:
                        update_status = "재시도_실패"
                        new_phone_for_save = None
                    
                    # 결과 데이터 생성
                    result_data = {
                        '인덱스': item['index'],
                        '사업장명': item['business_name'],
                        '기존주소': address,
                        '기존전화번호': original_row.get(self.config['columns'].get('phone', ''), ''),
                        '새전화번호': new_phone_for_save,
                        '업데이트상태': self.get_update_status(
                            original_row.get(self.config['columns'].get('phone', ''), ''), 
                            new_phone_for_save, 
                            update_status
                        ),
                        '주소유사도점수': self.get_address_similarity_score(address, new_phone_for_save),
                        '수집된주소': self.get_collected_address(new_phone_for_save)
                    }
                    
                    # 결과 저장
                    if self.save_single_result(result_data):
                        print(f"✅ 재시도 결과 저장 완료")
                        self.logger.info(f"✅ 재시도 결과 저장 완료: {item['business_name']}")
                    else:
                        print(f"❌ 재시도 결과 저장 실패")
                        self.logger.error(f"❌ 재시도 결과 저장 실패: {item['business_name']}")
                    
                    # 네이버 차단 방지를 위한 대기
                    wait_time = random.uniform(2.0, 3.0)
                    print(f"{wait_time:.1f}초 대기 중...")
                    time.sleep(wait_time)
                    
                except Exception as e:
                    self.logger.error(f"재시도 중 오류 발생: {e}")
                    print(f"재시도 중 오류 발생: {e}")
                    continue
            
            print(f"\n🎉 재시도 완료!")
            print(f"📊 총 재시도: {len(target_items)}개")
            print(f"📊 성공: {success_count}개")
            print(f"📊 실패: {len(target_items) - success_count}개")
            print(f"📊 성공률: {(success_count / len(target_items) * 100):.1f}%")
            
            return f"재시도 완료: {success_count}/{len(target_items)} 성공"
            
        except Exception as e:
            print(f"재시도 중 오류: {e}")
            self.logger.error(f"재시도 중 오류: {e}")
            return None

    def search_naver_map(self, business_name, dong_name, original_address):
        """네이버 지도에서 검색하여 전화번호와 정보를 수집"""
        try:
            # 원본 주소 저장
            self.current_original_address = original_address
            # 수집된 주소 초기화
            self.current_collected_address = ""
            
            # 1차 검색어: 사업장명 + 동이름
            search_query = f"{business_name} {dong_name}"
            print(f"=== 1차 검색 시작: {search_query} ===")
            
            self.logger.info(f"검색 시작: {search_query}")
            
            # 1차 검색 실행
            encoded_query = urllib.parse.quote(search_query)
            search_url = f"https://map.naver.com/p/search/{encoded_query}"
            print(f"1차 검색 URL: {search_url}")
            
            print("1차 검색 페이지 로딩 중...")
            self.driver.get(search_url)
            print("1차 검색 페이지 로딩 완료")
            
            # 랜덤 대기 시간으로 봇 탐지 회피
            wait_time = random.uniform(4.0, 7.0)
            print(f"1차 검색 결과 로딩 대기 중... ({wait_time:.1f}초)")
            time.sleep(wait_time)
            print("1차 검색 결과 로딩 대기 완료")
            
            # 1차 검색 결과 확인
            phone_number = self._check_and_extract_phone()
            if phone_number and phone_number != "MULTIPLE_RESULTS_NO_PHONE":
                return phone_number, "전화번호 발견", 0, self.current_collected_address, ""
            elif phone_number == "MULTIPLE_RESULTS_NO_PHONE":
                return None, "네이버 지도에서 여러 결과가 나왔지만 전화번호 정보가 없었습니다", 0, self.current_collected_address, ""
            
            # 1차 검색에서 결과가 없거나 단일 결과에서 전화번호를 찾지 못한 경우에만 2차 검색
            print(f"=== 2차 검색 시작: {business_name} ===")
            
            encoded_business = urllib.parse.quote(business_name)
            search_url = f"https://map.naver.com/p/search/{encoded_business}"
            print(f"2차 검색 URL: {search_url}")
            
            print("2차 검색 페이지 로딩 중...")
            self.driver.get(search_url)
            print("2차 검색 페이지 로딩 완료")
            
            # 랜덤 대기 시간으로 봇 탐지 회피
            wait_time = random.uniform(4.0, 7.0)
            print(f"2차 검색 결과 로딩 대기 중... ({wait_time:.1f}초)")
            time.sleep(wait_time)
            print("2차 검색 결과 로딩 대기 완료")
            
            # 2차 검색 결과 확인
            phone_number = self._check_and_extract_phone()
            if phone_number and phone_number != "MULTIPLE_RESULTS_NO_PHONE":
                return phone_number, "전화번호 발견", 0, self.current_collected_address, ""
            
            print("전화번호를 찾을 수 없음")
            return None, "네이버 지도에서 해당 업체를 찾을 수 없었습니다", 0, self.current_collected_address, ""
            
        except Exception as e:
            self.logger.error(f"검색 및 전화번호 추출 중 오류: {e}")
            print(f"검색 및 전화번호 추출 중 오류: {e}")
            return None, "검색 중 오류 발생", 0, "", str(e)

    def crawl_range(self, start_row=95, end_row=340):
        """특정 범위의 행만 크롤링하는 함수"""
        try:
            # CSV 파일 읽기
            csv_file = self.config['target_file']
            print(f"CSV 파일 읽기: {csv_file}")
            df = pd.read_csv(csv_file)
            
            # 설정에서 컬럼명 가져오기
            business_name_col = self.config['columns']['business_name']
            address_col = self.config['columns']['address']
            phone_col = self.config['columns'].get('phone', None)
            
            # 컬럼 존재 확인
            if business_name_col not in df.columns:
                raise ValueError(f"사업장명 컬럼 '{business_name_col}'을 찾을 수 없습니다.")
            if address_col not in df.columns:
                raise ValueError(f"주소 컬럼 '{address_col}'을 찾을 수 없습니다.")
            
            print(f"✅ CSV 구조 확인 완료")
            print(f"   사업장명 컬럼: {business_name_col}")
            print(f"   주소 컬럼: {address_col}")
            if phone_col:
                print(f"   전화번호 컬럼: {phone_col}")
            
            # 순번 추가
            df['인덱스'] = range(1, len(df) + 1)
            
            # 지정된 범위의 데이터만 선택 (1-based 인덱스)
            start_idx = start_row - 1  # 0-based로 변환
            end_idx = end_row
            range_df = df.iloc[start_idx:end_idx].copy()
            
            print(f"🎯 크롤링 범위: {start_row}번째 ~ {end_row}번째 행 ({len(range_df)}개)")
            self.logger.info(f"🎯 크롤링 범위: {start_row}번째 ~ {end_row}번째 행 ({len(range_df)}개)")
            
            # 결과 파일 초기화
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            result_filename = f"flexible_crawling_{timestamp}_range_{start_row}-{end_row}.csv"
            self.result_file = result_filename
            
            # 결과 파일 헤더 작성
            with open(self.result_file, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f)
                writer.writerow([
                    '인덱스', '사업장명', '기존주소', '기존전화번호', '새전화번호', 
                    '업데이트상태', '주소유사도점수', '수집된주소', '에러 사유'
                ])
            
            print(f"📁 결과 파일 생성: {self.result_file}")
            self.logger.info(f"📁 결과 파일 생성: {self.result_file}")
            
            # 범위 내 각 행 처리
            for index, row in range_df.iterrows():
                try:
                    print(f"\n{'='*50}")
                    current_row_num = index + 1
                    business_name = row[business_name_col]
                    print(f"처리 중: {current_row_num}번째 행 - {business_name}")
                    self.logger.info(f"처리 중: {current_row_num}번째 행 - {business_name}")
                    
                    # 주소에서 동이름 추출
                    address = row[address_col]
                    if pd.isna(address) or address == '':
                        print("주소 정보 없음")
                        result_data = {
                            '인덱스': row['인덱스'],
                            '사업장명': business_name,
                            '기존주소': address,
                            '기존전화번호': row[phone_col] if phone_col else '',
                            '새전화번호': None,
                            '업데이트상태': '주소정보없음',
                            '주소유사도점수': 0,
                            '수집된주소': "",
                            '에러 사유': ""
                        }
                        self.save_single_result(result_data)
                        continue
                    
                    # 동이름 추출
                    dong_name = self.extract_dong_name(address)
                    if not dong_name:
                        print("동이름 추출 실패")
                        result_data = {
                            '인덱스': row['인덱스'],
                            '사업장명': business_name,
                            '기존주소': address,
                            '기존전화번호': row[phone_col] if phone_col else '',
                            '새전화번호': None,
                            '업데이트상태': '동이름추출실패',
                            '주소유사도점수': 0,
                            '수집된주소': "",
                            '에러 사유': ""
                        }
                        self.save_single_result(result_data)
                        continue
                    
                    print(f"🔍 검색: {business_name} ({dong_name})")
                    
                    # 네이버 지도에서 검색
                    phone_number, update_status, similarity_score, collected_address, error_reason = self.search_naver_map(business_name, dong_name, address)
                    
                    # 결과 저장
                    result_data = {
                        '인덱스': row['인덱스'],
                        '사업장명': business_name,
                        '기존주소': address,
                        '기존전화번호': row[phone_col] if phone_col else '',
                        '새전화번호': phone_number,
                        '업데이트상태': update_status,
                        '주소유사도점수': similarity_score,
                        '수집된주소': collected_address,
                        '에러 사유': error_reason
                    }
                    
                    self.save_single_result(result_data)
                    self.processed_count += 1
                    
                    # 진행률 출력
                    progress = (current_row_num - start_row + 1) / (end_row - start_row + 1) * 100
                    print(f"📊 진행률: {progress:.1f}% ({current_row_num - start_row + 1}/{end_row - start_row + 1})")
                    
                    # 랜덤 지연
                    delay = random.uniform(2, 4)
                    print(f"⏱️ {delay:.1f}초 대기...")
                    time.sleep(delay)
                    
                except Exception as e:
                    print(f"❌ 행 처리 중 오류: {str(e)}")
                    self.logger.error(f"행 처리 중 오류: {str(e)}")
                    
                    # 오류 결과 저장
                    result_data = {
                        '인덱스': row['인덱스'],
                        '사업장명': business_name,
                        '기존주소': address,
                        '기존전화번호': row[phone_col] if phone_col else '',
                        '새전화번호': None,
                        '업데이트상태': '처리오류',
                        '주소유사도점수': 0,
                        '수집된주소': "",
                        '에러 사유': str(e)
                    }
                    self.save_single_result(result_data)
                    continue
            
            print(f"\n🎉 크롤링 완료!")
            print(f"📊 총 처리된 항목: {self.processed_count}개")
            print(f"📁 결과 파일: {self.result_file}")
            self.logger.info(f"🎉 크롤링 완료! 총 처리된 항목: {self.processed_count}개")
            
        except Exception as e:
            print(f"❌ 크롤링 중 오류: {str(e)}")
            self.logger.error(f"크롤링 중 오류: {str(e)}")
        finally:
            if hasattr(self, 'driver'):
                self.driver.quit()



# 메인 실행
if __name__ == "__main__":
    print("유연한 네이버 지도 크롤러 시작!")
    print(f"운영체제: {platform.system()}")
    if platform.system() == "Darwin":
        print("맥OS 환경에서 실행됩니다.")
    
    # 재시작 옵션 입력
    print("\n" + "="*50)
    print("크롤링 옵션 선택:")
    print("1. 새로 시작")
    print("2. 기존 결과에서 자동 재시작")
    print("3. 특정 인덱스부터 시작")
    print("4. 테스트용 (정해진 갯수만 크롤링)")
    print("5. 실패 데이터 분석")
    print("6. 실패 데이터 재시도")
    print("="*50)
    
    while True:
        try:
            choice = input("선택하세요 (1/2/3/4/5/6): ").strip()
            if choice in ['1', '2', '3', '4', '5', '6']:
                break
            else:
                print("1, 2, 3, 4, 5, 6 중에서 선택해주세요.")
        except KeyboardInterrupt:
            print("\n프로그램 종료")
            exit()
    
    crawler = FlexibleCrawler()
    
    try:
        if choice == '1':
            # 새로 시작
            print("\n🆕 새로운 크롤링 시작")
            result = crawler.crawl_phone_numbers()
        elif choice == '2':
            # 기존 결과에서 자동 재시작
            print("\n🔄 기존 결과에서 자동 재시작")
            result = crawler.crawl_phone_numbers()
        elif choice == '3':
            # 특정 인덱스부터 시작
            while True:
                try:
                    start_index = input("시작할 인덱스를 입력하세요 (1부터): ").strip()
                    start_index = int(start_index)
                    if start_index >= 1:
                        break
                    else:
                        print("1 이상의 숫자를 입력해주세요.")
                except ValueError:
                    print("올바른 숫자를 입력해주세요.")
                except KeyboardInterrupt:
                    print("\n프로그램 종료")
                    exit()
            
            print(f"\n🚀 인덱스 {start_index}부터 크롤링 시작")
            result = crawler.crawl_phone_numbers(start_from_index=start_index)
        elif choice == '4':
            # 테스트용으로 정해진 갯수만 크롤링
            while True:
                try:
                    test_count = input("테스트할 데이터 갯수를 입력하세요 (1부터): ").strip()
                    test_count = int(test_count)
                    if test_count >= 1:
                        break
                    else:
                        print("1 이상의 숫자를 입력해주세요.")
                except ValueError:
                    print("올바른 숫자를 입력해주세요.")
                except KeyboardInterrupt:
                    print("\n프로그램 종료")
                    exit()
            
            print(f"\n🧪 테스트용 크롤링 시작: {test_count}개 데이터")
            result = crawler.crawl_phone_numbers(test_count=test_count)
        elif choice == '5':
            # 실패 데이터 분석
            print("\n📊 실패 데이터 분석 시작")
            csv_files = glob.glob('flexible_crawling_*.csv')
            if not csv_files:
                print("❌ 분석할 결과 파일을 찾을 수 없습니다.")
                print("먼저 크롤링을 실행하여 결과 파일을 생성해주세요.")
            else:
                latest_file = max(csv_files, key=os.path.getctime)
                print(f"📋 분석 대상 파일: {latest_file}")
                crawler.analyze_failed_data(latest_file)
        elif choice == '6':
            # 실패 데이터 재시도
            print("\n🔄 실패 데이터 재시도 시작")
            csv_files = glob.glob('flexible_crawling_*.csv')
            if not csv_files:
                print("❌ 재시도할 결과 파일을 찾을 수 없습니다.")
                print("먼저 크롤링을 실행하여 결과 파일을 생성해주세요.")
            else:
                latest_file = max(csv_files, key=os.path.getctime)
                print(f"📋 재시도 대상 파일: {latest_file}")
                
                # 재시도 카테고리 선택
                print("\n재시도할 카테고리를 선택하세요:")
                print("1. 모든 실패 데이터")
                print("2. 주소유사도 0인 데이터")
                print("3. 전화번호 수집 실패 데이터")
                print("4. 잘못된 지역 매칭 데이터")
                print("5. 기타 실패 데이터")
                
                while True:
                    try:
                        retry_choice = input("선택하세요 (1/2/3/4/5): ").strip()
                        if retry_choice in ['1', '2', '3', '4', '5']:
                            break
                        else:
                            print("1, 2, 3, 4, 5 중에서 선택해주세요.")
                    except KeyboardInterrupt:
                        print("\n프로그램 종료")
                        exit()
                
                # 카테고리 매핑
                category_map = {
                    '1': None,  # 모든 실패 데이터
                    '2': '주소유사도_0',
                    '3': '전화번호_수집실패',
                    '4': '잘못된_지역매칭',
                    '5': '기타_실패'
                }
                
                selected_category = category_map[retry_choice]
                if selected_category:
                    print(f"\n🔄 {selected_category} 카테고리 재시도 시작")
                else:
                    print(f"\n🔄 모든 실패 데이터 재시도 시작")
                
                result = crawler.retry_failed_data(latest_file, selected_category)
        
        if result:
            print(f"\n크롤링 완료! {result}")
        else:
            print("\n크롤링 실패")
            
    except KeyboardInterrupt:
        print("\n사용자에 의해 중단됨")
        print("💡 재시작하려면 프로그램을 다시 실행하고 옵션 2나 3을 선택하세요.")
    except Exception as e:
        print(f"\n예상치 못한 오류: {e}")
    finally:
        crawler.close()
