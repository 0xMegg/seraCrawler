import pandas as pd
import time
import re
import urllib.parse
import platform
import os
import random
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

class NaverMapCrawler:
    def __init__(self):
        self.setup_driver()
        self.setup_logging()
        self.processed_count = 0
        self.batch_size = 100  # 100개마다 새로운 파일 저장
        
    def setup_driver(self):
        """Chrome WebDriver 설정 (맥OS 호환성 고려)"""
        print("Chrome WebDriver 설정 중...")
        chrome_options = Options()
        
        # 맥OS 호환성 설정
        if platform.system() == "Darwin":  # 맥OS
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--remote-debugging-port=9222")
            # M1 맥북 전용 설정
            if platform.machine() == "arm64":
                chrome_options.add_argument("--disable-background-timer-throttling")
                chrome_options.add_argument("--disable-backgrounding-occluded-windows")
                chrome_options.add_argument("--disable-renderer-backgrounding")
                chrome_options.add_argument("--disable-features=TranslateUI")
                chrome_options.add_argument("--disable-ipc-flooding-protection")
        else:  # Windows/Linux
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
        
        # 공통 설정
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-plugins")
        chrome_options.add_argument("--disable-images")  # 이미지 로딩 비활성화로 성능 향상
        
        # 봇 탐지 회피를 위한 추가 설정
        chrome_options.add_argument("--no-first-run")
        chrome_options.add_argument("--no-default-browser-check")
        chrome_options.add_argument("--disable-default-apps")
        chrome_options.add_argument("--disable-popup-blocking")
        chrome_options.add_argument("--disable-notifications")
        chrome_options.add_argument("--disable-web-security")
        chrome_options.add_argument("--allow-running-insecure-content")
        chrome_options.add_argument("--disable-features=VizDisplayCompositor")
        
        # User-Agent 설정 (최신 Chrome 버전)
        chrome_options.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        
        # 맥OS 전용 성능 최적화 (M1 맥북이 아닌 경우)
        if platform.system() == "Darwin" and platform.machine() != "arm64":
            chrome_options.add_argument("--disable-background-timer-throttling")
            chrome_options.add_argument("--disable-backgrounding-occluded-windows")
            chrome_options.add_argument("--disable-renderer-backgrounding")
            chrome_options.add_argument("--disable-features=TranslateUI")
            chrome_options.add_argument("--disable-ipc-flooding-protection")
        
        try:
            # 맥OS ARM64 환경에서는 ChromeDriverManager 대신 직접 경로 사용
            if platform.system() == "Darwin" and platform.machine() == "arm64":
                print("맥OS ARM64 환경 감지, 직접 Chrome 경로 사용")
                chrome_options.binary_location = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
                self.driver = webdriver.Chrome(options=chrome_options)
                print("맥OS ARM64 Chrome으로 WebDriver 설정 완료!")
            else:
                # 일반적인 방법으로 ChromeDriver 설치
                service = Service(ChromeDriverManager().install())
                self.driver = webdriver.Chrome(service=service, options=chrome_options)
                print("Chrome WebDriver 설정 완료!")
            
            # 봇 탐지 회피를 위한 JavaScript 실행
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            self.driver.execute_script("Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]})")
            self.driver.execute_script("Object.defineProperty(navigator, 'languages', {get: () => ['ko-KR', 'ko', 'en-US', 'en']})")
            self.driver.execute_script("Object.defineProperty(navigator, 'platform', {get: () => 'MacIntel'})")
            
            # 쿠키 및 로컬 스토리지 초기화
            self.driver.delete_all_cookies()
            
        except Exception as e:
            print(f"Chrome WebDriver 설정 실패: {e}")
            # 맥OS에서 ChromeDriver 경로 문제 시 대안
            if platform.system() == "Darwin":
                try:
                    print("대안 방법으로 Chrome 설정 시도...")
                    # 맥OS 기본 Chrome 경로 사용
                    chrome_options.binary_location = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
                    self.driver = webdriver.Chrome(options=chrome_options)
                    print("맥OS 기본 Chrome으로 WebDriver 설정 완료!")
                except Exception as e2:
                    print(f"맥OS 기본 Chrome 설정도 실패: {e2}")
                    print("Chrome이 설치되어 있는지 확인해주세요.")
                    print("설치 경로: /Applications/Google Chrome.app/Contents/MacOS/Google Chrome")
                    raise e2
            else:
                raise e
        
    def simulate_human_behavior(self):
        """자연스러운 사용자 행동 시뮬레이션"""
        try:
            # 랜덤한 마우스 움직임
            actions = ActionChains(self.driver)
            x_offset = random.randint(-100, 100)
            y_offset = random.randint(-100, 100)
            actions.move_by_offset(x_offset, y_offset)
            actions.perform()
            
            # 랜덤한 스크롤
            scroll_amount = random.randint(-300, 300)
            self.driver.execute_script(f"window.scrollBy(0, {scroll_amount});")
            
            # 짧은 대기
            time.sleep(random.uniform(0.5, 1.5))
            
        except Exception as e:
            print(f"사용자 행동 시뮬레이션 중 오류: {e}")
    
    def setup_logging(self):
        """로깅 설정"""
        timestamp = datetime.now().strftime("%y%m%d%H%M%S")
        log_filename = f"stores_crawling_{timestamp}.log"
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_filename, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        self.current_log_filename = log_filename
        print(f"로깅 설정 완료: {log_filename}")
        
    def create_new_logging(self):
        """새로운 로그 파일 생성"""
        timestamp = datetime.now().strftime("%y%m%d%H%M%S")
        log_filename = f"stores_crawling_{timestamp}.log"
        
        # 기존 핸들러 제거
        for handler in self.logger.handlers[:]:
            self.logger.removeHandler(handler)
        
        # 새로운 핸들러 추가
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_filename, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        self.current_log_filename = log_filename
        print(f"새로운 로그 파일 생성: {log_filename}")
        
    def save_batch_results(self, results, batch_number):
        """배치별 결과 저장"""
        timestamp = datetime.now().strftime("%y%m%d%H%M%S")
        result_filename = f"stores_crawling_batch{batch_number}_{timestamp}.csv"
        
        result_df = pd.DataFrame(results)
        result_df.to_csv(result_filename, index=False, encoding='utf-8-sig')
        
        self.logger.info(f"배치 {batch_number} 결과 저장 완료: {result_filename}")
        print(f"배치 {batch_number} 결과 저장 완료: {result_filename}")
        return result_filename
        
    def extract_address_parts(self, address):
        """주소에서 시, 구, 동 추출"""
        if pd.isna(address) or address == '':
            return None, None, None
            
        # 경상남도 거제시 아주동 1701-3 1층 형태에서 추출
        parts = address.split()
        if len(parts) >= 3:
            return parts[0], parts[1], parts[2]  # 시, 구, 동
        return None, None, None
        
    def compare_addresses(self, original_address, search_result_address):
        """주소 유사도 비교 (시, 구, 동 레벨)"""
        orig_si, orig_gu, orig_dong = self.extract_address_parts(original_address)
        search_si, search_gu, search_dong = self.extract_address_parts(search_result_address)
        
        if not all([orig_si, orig_gu, orig_dong]):
            return 0
            
        score = 0
        if orig_si == search_si:
            score += 1
        if orig_gu == search_gu:
            score += 1
        if orig_dong == search_dong:
            score += 1
            
        return score
        
    def search_and_extract_phone(self, business_name, dong_name, original_address=None):
        """검색과 전화번호 추출을 한 번에 처리"""
        try:
            # 원본 주소 저장
            self.current_original_address = original_address
            
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
            
            # 자연스러운 사용자 행동 시뮬레이션
            print("자연스러운 사용자 행동 시뮬레이션 중...")
            self.simulate_human_behavior()
            
            # 랜덤 대기 시간으로 봇 탐지 회피
            wait_time = random.uniform(4.0, 7.0)
            print(f"1차 검색 결과 로딩 대기 중... ({wait_time:.1f}초)")
            time.sleep(wait_time)
            print("1차 검색 결과 로딩 대기 완료")
            
            # 1차 검색 결과 확인
            phone_number = self._check_and_extract_phone()
            if phone_number:
                return phone_number
            elif phone_number == "MULTIPLE_RESULTS_NO_PHONE":
                # 다중 결과가 나왔지만 전화번호를 찾지 못한 경우
                return "MULTIPLE_RESULTS_NO_PHONE"
            
            # 1차 검색에서 결과가 없거나 단일 결과에서 전화번호를 찾지 못한 경우에만 2차 검색
            print(f"=== 2차 검색 시작: {business_name} ===")
            
            encoded_business = urllib.parse.quote(business_name)
            search_url = f"https://map.naver.com/p/search/{encoded_business}"
            print(f"2차 검색 URL: {search_url}")
            
            print("2차 검색 페이지 로딩 중...")
            self.driver.get(search_url)
            print("2차 검색 페이지 로딩 완료")
            
            # 자연스러운 사용자 행동 시뮬레이션
            print("자연스러운 사용자 행동 시뮬레이션 중...")
            self.simulate_human_behavior()
            
            # 랜덤 대기 시간으로 봇 탐지 회피
            wait_time = random.uniform(4.0, 7.0)
            print(f"2차 검색 결과 로딩 대기 중... ({wait_time:.1f}초)")
            time.sleep(wait_time)
            print("2차 검색 결과 로딩 대기 완료")
            
            # 2차 검색 결과 확인
            phone_number = self._check_and_extract_phone()
            if phone_number:
                return phone_number
            
            print("전화번호를 찾을 수 없음")
            return None
            
        except Exception as e:
            self.logger.error(f"검색 및 전화번호 추출 중 오류: {e}")
            print(f"검색 및 전화번호 추출 중 오류: {e}")
            return None
    
    def _check_and_extract_phone(self):
        """현재 페이지에서 전화번호 확인 및 추출"""
        try:
            # searchIframe에서 검색 결과 확인
            print("=== iframe 처리 시작 ===")
            try:
                print("searchIframe 로딩 대기 중... (최대 10초)")
                iframe = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.ID, "searchIframe"))
                )
                print("searchIframe 발견")
                
                print("searchIframe으로 전환 중...")
                self.driver.switch_to.frame(iframe)
                print("searchIframe 전환 완료")
                
                print("searchIframe 내부에서 검색 결과 찾는 중...")
                results = self.driver.find_elements(By.CSS_SELECTOR, "li.VLTHu.OW9LQ")
                
                if results:
                    print(f"✅ searchIframe 내부에서 {len(results)}개 검색 결과 발견!")
                    
                    # 메인 페이지로 복귀
                    self.driver.switch_to.default_content()
                    print("메인 페이지로 복귀 완료")
                    
                    # 검색 결과가 2개 이상인 경우 처리
                    if len(results) >= 2:
                        phone_number = self.process_multiple_results(results)
                        # 다중 결과가 나왔으면 2차 검색을 하지 않음
                        if phone_number:
                            return phone_number
                        else:
                            # 다중 결과에서 전화번호를 찾지 못했지만 2차 검색은 하지 않음
                            return "MULTIPLE_RESULTS_NO_PHONE"
                    else:
                        # 단일 결과 처리
                        phone_number = self.extract_phone_number(results[0])
                        if phone_number:
                            return phone_number
                        else:
                            # 단일 결과에서 전화번호를 찾지 못한 경우 None 반환 (2차 검색으로 진행)
                            return None
                
            except Exception as e:
                print(f"❌ iframe 처리 중 오류: {e}")
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

    def process_multiple_results(self, results):
        """검색 결과가 2개 이상일 때 처리"""
        try:
            print(f"=== 다중 검색 결과 처리 시작 ({len(results)}개) ===")
            
            # 상위 3개 결과만 확인
            top_results = results[:3]
            best_result_index = None
            best_score = -1
            
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
                            
                            # 주소 유사도 비교 (stores.csv의 소재지전체주소와 비교)
                            score = self.compare_address_similarity(search_address)
                            print(f"주소 유사도 점수: {score}")
                            
                            if score > best_score:
                                best_score = score
                                best_result_index = i
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
                
                # 최적 결과 클릭하여 상세 정보 로드
                try:
                    print("최적 결과 클릭 중...")
                    self.driver.switch_to.frame("searchIframe")
                    
                    # 인덱스로 해당 결과를 다시 찾아서 클릭
                    clickable_results = self.driver.find_elements(By.CSS_SELECTOR, "li.VLTHu.OW9LQ a.place_bluelink")
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

    def compare_address_similarity(self, search_address):
        """주소 유사도 비교 (동/리 레벨)"""
        try:
            # 현재 처리 중인 원본 주소
            original_address = self.current_original_address
            if not original_address:
                print("원본 주소 정보가 없음")
                return 0
            
            print(f"원본 주소: {original_address}")
            print(f"검색 주소: {search_address}")
            
            # 동/리 추출
            orig_parts = original_address.split()
            search_parts = search_address.split()
            
            score = 0
            
            # 시/도 레벨 비교 (예: 경상남도)
            if len(orig_parts) > 0:
                if orig_parts[0] in search_address:
                    score += 1
                    print(f"시/도 매칭: {orig_parts[0]}")
            
            # 시/군 레벨 비교 (예: 거제시)
            if len(orig_parts) > 1:
                if orig_parts[1] in search_address:
                    score += 1
                    print(f"시/군 매칭: {orig_parts[1]}")
            
            # 동/리 레벨 비교 (가장 중요)
            orig_dong_ri = None
            for part in orig_parts:
                if part.endswith('동') or part.endswith('리'):
                    orig_dong_ri = part
                    break
            
            if orig_dong_ri:
                if orig_dong_ri in search_address:
                    score += 5  # 동/리 매칭에 매우 높은 가중치
                    print(f"동/리 매칭: {orig_dong_ri} (점수 +5)")
                else:
                    # 부분 매칭 확인 (예: "고현동" vs "고현")
                    dong_ri_base = orig_dong_ri.replace('동', '').replace('리', '')
                    if dong_ri_base in search_address:
                        score += 3  # 부분 매칭에도 높은 가중치
                        print(f"동/리 부분 매칭: {dong_ri_base} (점수 +3)")
            
            # 상세 주소 비교 (건물명, 번지 등)
            # 원본 주소에서 상세 정보 추출
            if len(orig_parts) > 3:
                detail_parts = orig_parts[3:]
                for detail in detail_parts:
                    if detail in search_address:
                        score += 1
                        print(f"상세 주소 매칭: {detail}")
            
            print(f"최종 유사도 점수: {score}")
            return score
            
        except Exception as e:
            print(f"주소 비교 중 오류: {e}")
            return 0
            
    def extract_phone_number(self, result_element):
        """검색 결과에서 전화번호 추출"""
        try:
            print("=== 전화번호 추출 시작 ===")
            
            # 1단계: entryIframe에서 전화번호 찾기 (우선순위 높임)
            try:
                print("entryIframe에서 전화번호 찾기...")
                entry_iframes = self.driver.find_elements(By.ID, "entryIframe")
                
                if entry_iframes:
                    self.driver.switch_to.frame("entryIframe")
                    print("entryIframe으로 전환 완료")
                    
                    # entryIframe 내에서 span.xlx7Q 찾기
                    phone_elements = self.driver.find_elements(By.CSS_SELECTOR, "span.xlx7Q")
                    print(f"entryIframe에서 span.xlx7Q 찾은 개수: {len(phone_elements)}")
                    
                    if phone_elements:
                        phone_number = phone_elements[0].text.strip()
                        print(f"찾은 전화번호 텍스트: '{phone_number}'")
                        if phone_number and len(phone_number) > 5:
                            print(f"✅ entryIframe에서 전화번호 발견: {phone_number}")
                            self.driver.switch_to.default_content()
                            return phone_number
                    
                    # 메인 페이지로 복귀
                    self.driver.switch_to.default_content()
                    print("entryIframe에서 메인 페이지로 복귀 완료")
                
            except Exception as e:
                print(f"entryIframe 처리 중 오류: {e}")
                self.driver.switch_to.default_content()
            
            # 2단계: searchIframe에서 전화번호 찾기
            try:
                print("searchIframe에서 전화번호 찾기...")
                self.driver.switch_to.frame("searchIframe")
                
                # searchIframe 내에서 span.xlx7Q 찾기
                phone_elements = self.driver.find_elements(By.CSS_SELECTOR, "span.xlx7Q")
                print(f"searchIframe에서 span.xlx7Q 찾은 개수: {len(phone_elements)}")
                
                if phone_elements:
                    phone_number = phone_elements[0].text.strip()
                    print(f"찾은 전화번호 텍스트: '{phone_number}'")
                    if phone_number and len(phone_number) > 5:
                        print(f"✅ searchIframe에서 전화번호 발견: {phone_number}")
                        self.driver.switch_to.default_content()
                        return phone_number
                    else:
                        print(f"전화번호가 너무 짧거나 비어있음: '{phone_number}'")
                
                # 메인 페이지로 복귀
                self.driver.switch_to.default_content()
                print("searchIframe에서 메인 페이지로 복귀 완료")
                
            except Exception as e:
                print(f"searchIframe 처리 중 오류: {e}")
                self.driver.switch_to.default_content()
            
            # 3단계: 메인 페이지에서 직접 찾기
            print("메인 페이지에서 직접 전화번호 찾기...")
            phone_elements = self.driver.find_elements(By.CSS_SELECTOR, "span.xlx7Q")
            print(f"메인 페이지에서 span.xlx7Q 찾은 개수: {len(phone_elements)}")
            
            if phone_elements:
                phone_number = phone_elements[0].text.strip()
                print(f"찾은 전화번호 텍스트: '{phone_number}'")
                if phone_number and len(phone_number) > 5:
                    print(f"✅ 메인 페이지에서 전화번호 발견: {phone_number}")
                    return phone_number
                else:
                    print(f"전화번호가 너무 짧거나 비어있음: '{phone_number}'")
            else:
                print("메인 페이지에서 span.xlx7Q를 찾을 수 없음")
            
            print("전화번호를 찾을 수 없음")
            return None
            
        except Exception as e:
            self.logger.error(f"전화번호 추출 중 오류: {e}")
            print(f"전화번호 추출 중 오류: {e}")
            return None
            
    def extract_phone_number_direct(self):
        """직접 전화번호 추출 (결과 요소 없이)"""
        try:
            print("=== 직접 전화번호 추출 시작 ===")
            
            # entryIframe에서 전화번호 찾기
            try:
                print("entryIframe에서 전화번호 찾기...")
                entry_iframes = self.driver.find_elements(By.ID, "entryIframe")
                
                if entry_iframes:
                    self.driver.switch_to.frame("entryIframe")
                    print("entryIframe으로 전환 완료")
                    
                    # entryIframe 내에서 span.xlx7Q 찾기
                    phone_elements = self.driver.find_elements(By.CSS_SELECTOR, "span.xlx7Q")
                    print(f"entryIframe에서 span.xlx7Q 찾은 개수: {len(phone_elements)}")
                    
                    if phone_elements:
                        phone_number = phone_elements[0].text.strip()
                        print(f"찾은 전화번호 텍스트: '{phone_number}'")
                        if phone_number and len(phone_number) > 5:
                            print(f"✅ entryIframe에서 전화번호 발견: {phone_number}")
                            self.driver.switch_to.default_content()
                            return phone_number
                    
                    # 메인 페이지로 복귀
                    self.driver.switch_to.default_content()
                    print("entryIframe에서 메인 페이지로 복귀 완료")
                
            except Exception as e:
                print(f"entryIframe 처리 중 오류: {e}")
                self.driver.switch_to.default_content()
            
            # 메인 페이지에서 직접 찾기
            print("메인 페이지에서 직접 전화번호 찾기...")
            phone_elements = self.driver.find_elements(By.CSS_SELECTOR, "span.xlx7Q")
            print(f"메인 페이지에서 span.xlx7Q 찾은 개수: {len(phone_elements)}")
            
            if phone_elements:
                phone_number = phone_elements[0].text.strip()
                print(f"찾은 전화번호 텍스트: '{phone_number}'")
                if phone_number and len(phone_number) > 5:
                    print(f"✅ 메인 페이지에서 전화번호 발견: {phone_number}")
                    return phone_number
                else:
                    print(f"전화번호가 너무 짧거나 비어있음: '{phone_number}'")
            else:
                print("메인 페이지에서 span.xlx7Q를 찾을 수 없음")
            
            print("전화번호를 찾을 수 없음")
            return None
            
        except Exception as e:
            self.logger.error(f"직접 전화번호 추출 중 오류: {e}")
            print(f"직접 전화번호 추출 중 오류: {e}")
            return None
            
    def process_search_results(self, results, business_name, original_address):
        """검색 결과 처리 및 최적 결과 선택"""
        if not results:
            return None, "결과없음"
            
        if len(results) == 1:
            # 단일 결과
            self.logger.info("단일 검색 결과 처리")
            print("단일 검색 결과 처리")
            result = results[0]
            phone_number = self.extract_phone_number(result)
            return phone_number, "true" if phone_number else "전화번호 없음"
            
        else:
            # 다중 결과 - 주소 비교로 최적 결과 선택
            self.logger.info(f"다중 검색 결과 발견: {len(results)}개")
            print(f"다중 검색 결과 발견: {len(results)}개")
            
            best_result = None
            best_score = -1
            
            for i, result in enumerate(results[:5]):  # 상위 5개만 확인
                try:
                    print(f"결과 {i+1} 확인 중...")
                    # 결과 클릭하여 상세 정보 확인
                    result.click()
                    time.sleep(2)
                    
                    # 주소 정보 추출
                    address_element = self.driver.find_element(By.CSS_SELECTOR, "span.address")
                    if address_element:
                        search_address = address_element.text
                        score = self.compare_addresses(original_address, search_address)
                        print(f"주소 유사도 점수: {score} (주소: {search_address})")
                        
                        if score > best_score:
                            best_score = score
                            best_result = result
                            
                except Exception as e:
                    self.logger.error(f"결과 확인 중 오류: {e}")
                    print(f"결과 확인 중 오류: {e}")
                    continue
                    
            if best_result:
                print(f"최적 결과 선택 (점수: {best_score})")
                
                # 최적 결과 클릭하여 상세 정보 로드
                try:
                    print("최적 결과 클릭 중...")
                    self.driver.switch_to.frame("searchIframe")
                    best_result.click()
                    time.sleep(3)  # 로딩 대기
                    
                    # 메인 페이지로 복귀
                    self.driver.switch_to.default_content()
                    
                    # 새로 생긴 iframe에서 전화번호 찾기
                    print("새로 생긴 iframe에서 전화번호 찾기...")
                    return self.extract_phone_number_from_detail()
                    
                except Exception as e:
                    print(f"최적 결과 클릭 중 오류: {e}")
                    self.driver.switch_to.default_content()
                    return None
            else:
                print("적절한 결과를 찾을 수 없음")
                return None, "결과없음"
                
    def update_phone_numbers(self, csv_file, test_count=None):
        """전화번호 업데이트 메인 함수"""
        try:
            # CSV 파일 읽기
            print(f"CSV 파일 읽기: {csv_file}")
            df = pd.read_csv(csv_file)
            
            # 전체 데이터 또는 테스트 데이터 선택
            if test_count:
                test_df = df.head(test_count).copy()
                print(f"테스트 데이터 {test_count}개 선택")
            else:
                test_df = df.copy()
                print(f"전체 데이터 {len(df)}개 선택")
            
            # 결과 저장용 데이터프레임
            results = []
            
            for index, row in test_df.iterrows():
                try:
                    print(f"\n{'='*50}")
                    total_count = len(test_df)
                    print(f"처리 중: {index+1}/{total_count} - {row['사업장명']}")
                    self.logger.info(f"처리 중: {index+1}/{total_count} - {row['사업장명']}")
                    
                    # 주소에서 동이름 추출
                    address = row['소재지전체주소']
                    if pd.isna(address) or address == '':
                        print("주소 정보 없음")
                        results.append({
                            '순번': row['순번'],
                            '사업장명': row['사업장명'],
                            '기존전화번호': row['소재지전화'],
                            '새전화번호': None,
                            '업데이트': '주소정보없음'
                        })
                        continue
                        
                    # 동이름 추출 (예: "경상남도 거제시 아주동" -> "아주동")
                    # 또는 "일운면 지세포리" -> "지세포리"
                    address_parts = address.split()
                    dong_name = None
                    for part in address_parts:
                        if part.endswith('동') or part.endswith('리'):
                            dong_name = part
                            break
                            
                    if not dong_name:
                        print("동이름 추출 실패")
                        results.append({
                            '순번': row['순번'],
                            '사업장명': row['사업장명'],
                            '기존전화번호': row['소재지전화'],
                            '새전화번호': None,
                            '업데이트': '동이름추출실패'
                        })
                        continue
                        
                    print(f"동이름: {dong_name}")
                    
                    # 네이버 지도 검색 및 전화번호 추출
                    new_phone = self.search_and_extract_phone(row['사업장명'], dong_name, original_address=address)
                    
                    # 결과 처리
                    if new_phone == "MULTIPLE_RESULTS_NO_PHONE":
                        update_status = "MULTIPLE_RESULTS_NO_PHONE"
                        new_phone_for_save = None
                    elif new_phone:
                        update_status = "true"
                        new_phone_for_save = new_phone
                    else:
                        update_status = "결과없음"
                        new_phone_for_save = None
                    
                    # 결과 저장
                    results.append({
                        '순번': row['순번'],
                        '사업장명': row['사업장명'],
                        '기존전화번호': row['소재지전화'],
                        '새전화번호': new_phone_for_save,
                        '업데이트': update_status
                    })
                    
                    print(f"결과: {update_status}")
                    if new_phone:
                        print(f"새 전화번호: {new_phone}")
                    
                    # 네이버 차단 방지를 위한 랜덤 대기 시간
                    base_wait = 3.0 if platform.system() == "Darwin" else 4.0
                    random_wait = random.uniform(0.5, 2.0)
                    wait_time = base_wait + random_wait
                    print(f"{wait_time:.1f}초 대기 중... (네이버 차단 방지 + 랜덤)")
                    time.sleep(wait_time)
                    
                    # 100개마다 배치 저장 및 새로운 로그 파일 생성
                    self.processed_count += 1
                    
                    # 진행 상황 표시
                    if self.processed_count % 25 == 0:
                        print(f"진행 상황: {self.processed_count}개 처리됨")
                    
                    if self.processed_count % self.batch_size == 0:
                        batch_number = self.processed_count // self.batch_size
                        print(f"\n{'='*60}")
                        print(f"🎉 배치 {batch_number} 완료! ({self.processed_count}개 처리됨)")
                        print(f"{'='*60}")
                        
                        # 현재 배치 결과 저장
                        saved_file = self.save_batch_results(results, batch_number)
                        print(f"📁 배치 {batch_number} 결과 저장됨: {saved_file}")
                        
                        # 새로운 로그 파일 생성
                        self.create_new_logging()
                        print(f"📝 새로운 로그 파일 생성됨: {self.current_log_filename}")
                        
                        # 결과 리스트 초기화 (다음 배치를 위해)
                        results = []
                        print(f"🔄 배치 {batch_number} 완료 후 결과 리스트 초기화됨")
                        print(f"⏳ 다음 배치 시작 준비 완료...")
                        print(f"{'='*60}\n")
                    
                except Exception as e:
                    self.logger.error(f"행 처리 중 오류 발생: {e}")
                    print(f"행 처리 중 오류 발생: {e}")
                    results.append({
                        '순번': row['순번'],
                        '사업장명': row['사업장명'],
                        '기존전화번호': row['소재지전화'],
                        '새전화번호': None,
                        '업데이트': f'오류: {str(e)}'
                    })
                    
            # 마지막 배치 결과 저장 (100개 미만인 경우)
            if results:
                final_batch_number = (self.processed_count // self.batch_size) + 1
                if self.processed_count % self.batch_size != 0:
                    final_batch_number = (self.processed_count // self.batch_size) + 1
                else:
                    final_batch_number = self.processed_count // self.batch_size
                    
                self.save_batch_results(results, final_batch_number)
            
            self.logger.info(f"전체 처리 완료: 총 {self.processed_count}개 처리됨")
            print(f"전체 처리 완료: 총 {self.processed_count}개 처리됨")
            return f"총 {self.processed_count}개 처리 완료"
            
        except Exception as e:
            self.logger.error(f"전체 처리 중 오류: {e}")
            print(f"전체 처리 중 오류: {e}")
            return None
            
    def extract_phone_number_from_detail(self):
        """상세 페이지에서 전화번호 추출"""
        try:
            print("=== 상세 페이지에서 전화번호 추출 시작 ===")
            
            # 메인 페이지로 복귀 (안전장치)
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
            
            # a.BfF3H를 찾지 못한 경우 span.xlx7Q에서 전화번호 찾기
            print("a.BfF3H를 찾지 못했으므로 span.xlx7Q에서 전화번호 찾기...")
            
            # 2단계: entryIframe에서 전화번호 찾기 (우선순위 높음)
            try:
                print("entryIframe에서 전화번호 찾기...")
                entry_iframes = self.driver.find_elements(By.ID, "entryIframe")
                
                if entry_iframes:
                    self.driver.switch_to.frame("entryIframe")
                    print("entryIframe으로 전환 완료")
                    
                    # entryIframe 내에서 span.xlx7Q 찾기
                    phone_elements = self.driver.find_elements(By.CSS_SELECTOR, "span.xlx7Q")
                    print(f"entryIframe에서 span.xlx7Q 찾은 개수: {len(phone_elements)}")
                    
                    if phone_elements:
                        phone_number = phone_elements[0].text.strip()
                        print(f"찾은 전화번호 텍스트: '{phone_number}'")
                        if phone_number and len(phone_number) > 5:
                            print(f"✅ entryIframe에서 전화번호 발견: {phone_number}")
                            self.driver.switch_to.default_content()
                            return phone_number
                    
                    # 메인 페이지로 복귀
                    self.driver.switch_to.default_content()
                    print("entryIframe에서 메인 페이지로 복귀 완료")
                
            except Exception as e:
                print(f"entryIframe 처리 중 오류: {e}")
                self.driver.switch_to.default_content()
            
            # 3단계: searchIframe에서 전화번호 찾기
            try:
                print("searchIframe에서 전화번호 찾기...")
                self.driver.switch_to.frame("searchIframe")
                
                # searchIframe 내에서 span.xlx7Q 찾기
                phone_elements = self.driver.find_elements(By.CSS_SELECTOR, "span.xlx7Q")
                print(f"searchIframe에서 span.xlx7Q 찾은 개수: {len(phone_elements)}")
                
                if phone_elements:
                    phone_number = phone_elements[0].text.strip()
                    print(f"찾은 전화번호 텍스트: '{phone_number}'")
                    if phone_number and len(phone_number) > 5:
                        print(f"✅ searchIframe에서 전화번호 발견: {phone_number}")
                        self.driver.switch_to.default_content()
                        return phone_number
                    else:
                        print(f"전화번호가 너무 짧거나 비어있음: '{phone_number}'")
                
                # 메인 페이지로 복귀
                self.driver.switch_to.default_content()
                print("searchIframe에서 메인 페이지로 복귀 완료")
                
            except Exception as e:
                print(f"searchIframe 처리 중 오류: {e}")
                self.driver.switch_to.default_content()
            
            # 4단계: 메인 페이지에서 직접 찾기
            print("메인 페이지에서 직접 전화번호 찾기...")
            phone_elements = self.driver.find_elements(By.CSS_SELECTOR, "span.xlx7Q")
            print(f"메인 페이지에서 span.xlx7Q 찾은 개수: {len(phone_elements)}")
            
            if phone_elements:
                phone_number = phone_elements[0].text.strip()
                print(f"찾은 전화번호 텍스트: '{phone_number}'")
                if phone_number and len(phone_number) > 5:
                    print(f"✅ 메인 페이지에서 전화번호 발견: {phone_number}")
                    return phone_number
                else:
                    print(f"전화번호가 너무 짧거나 비어있음: '{phone_number}'")
            else:
                print("메인 페이지에서 span.xlx7Q를 찾을 수 없음")
            
            print("전화번호를 찾을 수 없음")
            return None
            
        except Exception as e:
            self.logger.error(f"상세 페이지에서 전화번호 추출 중 오류: {e}")
            print(f"상세 페이지에서 전화번호 추출 중 오류: {e}")
            return None
            
    def close(self):
        """브라우저 종료"""
        if self.driver:
            self.driver.quit()
            print("브라우저 종료")

# 메인 실행
if __name__ == "__main__":
    print("네이버 지도 크롤러 시작!")
    print(f"운영체제: {platform.system()}")
    if platform.system() == "Darwin":
        print("맥OS 환경에서 실행됩니다. 성능이 향상된 설정이 적용됩니다.")
    
    crawler = NaverMapCrawler()
    
    try:
        # stores.csv 파일이 존재하는지 확인
        input_file = "stores.csv"
        if not os.path.exists(input_file):
            print(f"오류: {input_file} 파일을 찾을 수 없습니다.")
            exit(1)
            
        print(f"입력 파일: {input_file}")
        # 전체 데이터 처리 (테스트하려면 test_count=10 등으로 설정)
        result_file = crawler.update_phone_numbers(input_file, test_count=None)
        if result_file:
            print(f"\n크롤링 완료! {result_file}")
        else:
            print("\n크롤링 실패")
    except KeyboardInterrupt:
        print("\n사용자에 의해 중단됨")
    except Exception as e:
        print(f"\n예상치 못한 오류: {e}")
    finally:
        crawler.close()