import pandas as pd
import time
import re
import urllib.parse
import platform
import os
import random
import csv
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
        
    def initialize_result_file(self):
        """결과 파일 초기화"""
        try:
            timestamp = datetime.now().strftime("%y%m%d%H%M%S")
            self.result_file = f'flexible_crawling_{timestamp}.csv'
            
            # 설정에서 출력 컬럼 가져오기
            headers = self.config['output_columns']
            
            with open(self.result_file, 'w', encoding='utf-8-sig', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(headers)
            
            print(f"📁 결과 파일 초기화: {self.result_file}")
            self.logger.info(f"결과 파일 초기화: {self.result_file}")
            
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
            
            wait_time = random.uniform(2.0, 5.0)  # 2초 단축
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
            
            wait_time = random.uniform(2.0, 5.0)  # 2초 단축
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
    
    def _check_and_extract_phone(self):
        """현재 페이지에서 전화번호 확인 및 추출"""
        try:
            # searchIframe에서 검색 결과 확인
            try:
                iframe = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.ID, "searchIframe"))
                )
                
                self.driver.switch_to.frame(iframe)
                results = self.driver.find_elements(By.CSS_SELECTOR, "li.VLTHu.OW9LQ")
                
                if results:
                    print(f"✅ {len(results)}개 검색 결과 발견!")
                    self.logger.info(f"✅ {len(results)}개 검색 결과 발견!")
                    
                    self.driver.switch_to.default_content()
                    
                    if len(results) >= 2:
                        phone_number = self.process_multiple_results(results)
                        if phone_number:
                            return phone_number
                        else:
                            return "MULTIPLE_RESULTS_NO_PHONE"
                    else:
                        phone_number = self.extract_phone_number(results[0])
                        if phone_number:
                            return phone_number
                        else:
                            return None
                
            except Exception as e:
                print(f"❌ iframe 처리 중 오류: {e}")
                self.driver.switch_to.default_content()
            
            # entryIframe에서 전화번호 확인
            phone_number = self.extract_phone_number_direct()
            if phone_number:
                return phone_number
            
            return None
            
        except Exception as e:
            print(f"전화번호 확인 중 오류: {e}")
            return None
    
    def process_multiple_results(self, results):
        """다중 검색 결과 처리"""
        try:
            print(f"다중 결과 처리 중... ({len(results)}개)")
            self.logger.info(f"=== 다중 검색 결과 처리 ({len(results)}개) ===")
            
            top_results = results[:3]
            best_result_index = None
            best_score = -1
            best_address = ""
            
            for i, result in enumerate(top_results):
                try:
                    self.logger.info(f"결과 {i+1} 확인 중...")
                    
                    self.driver.switch_to.frame("searchIframe")
                    
                    # a.PkgBl 클릭하여 상세 주소 정보 가져오기
                    try:
                        pkgbl_links = result.find_elements(By.CSS_SELECTOR, "a.PkgBl")
                        if pkgbl_links:
                            pkgbl_links[0].click()
                            time.sleep(1.0)  # 주소 정보 로딩 대기
                            
                            # 구주소(지번) 정보만 가져오기
                            jibun_elements = self.driver.find_elements(By.CSS_SELECTOR, "div.nQ7Lh")
                            jibun_address = ""
                            
                            for element in jibun_elements:
                                text = element.text
                                if "지번" in text:
                                    jibun_address = text.replace("지번", "").strip()
                                    break
                            
                            if jibun_address:
                                self.logger.info(f"결과 {i+1} 구주소: {jibun_address}")
                                self.driver.switch_to.default_content()
                                score = self.compare_address_similarity_with_jibun(jibun_address)
                                self.logger.info(f"구주소 유사도 점수: {score}")
                            else:
                                # 구주소를 못 가져온 경우 기존 방식 사용
                                address_elements = result.find_elements(By.CSS_SELECTOR, "span.Pb4bU")
                                if address_elements:
                                    search_address = address_elements[0].text.strip()
                                    self.logger.info(f"결과 {i+1} 주소: {search_address}")
                                    self.driver.switch_to.default_content()
                                    score = self.compare_address_similarity(search_address)
                                    self.logger.info(f"주소 {i+1} 유사도 점수: {score}")
                                else:
                                    self.driver.switch_to.default_content()
                                    score = 0
                                    self.logger.warning(f"결과 {i+1} 주소 정보 없음")
                        else:
                            # a.PkgBl이 없는 경우 기존 방식 사용
                            address_elements = result.find_elements(By.CSS_SELECTOR, "span.Pb4bU")
                            if address_elements:
                                search_address = address_elements[0].text.strip()
                                self.logger.info(f"결과 {i+1} 주소: {search_address}")
                                self.driver.switch_to.default_content()
                                score = self.compare_address_similarity(search_address)
                                self.logger.info(f"주소 {i+1} 유사도 점수: {score}")
                            else:
                                self.driver.switch_to.default_content()
                                score = 0
                                self.logger.warning(f"결과 {i+1} 주소 정보 없음")
                    except Exception as e:
                        print(f"결과 {i+1} 상세 주소 가져오기 중 오류: {e}")
                        self.driver.switch_to.default_content()
                        score = 0
                        continue
                    
                    if score > best_score:
                        best_score = score
                        best_result_index = i
                        best_jibun_address = jibun_address if 'jibun_address' in locals() and jibun_address else ""
                        best_address = search_address if 'search_address' in locals() else ""
                        
                except Exception as e:
                    print(f"결과 {i+1} 처리 중 오류: {e}")
                    self.driver.switch_to.default_content()
                    continue
            
            if best_result_index is not None:
                print(f"최적 결과 선택 (점수: {best_score})")
                self.logger.info(f"최적 결과 선택 (인덱스: {best_result_index}, 점수: {best_score})")
            else:
                print("첫 번째 결과 선택 (점수: 0)")
                self.logger.warning("모든 결과의 주소 유사도 점수가 0입니다. 첫 번째 결과를 선택합니다.")
                best_result_index = 0
                best_score = 0
                
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
                # 구주소 저장
                if 'best_jibun_address' in locals() and best_jibun_address:
                    self.current_collected_jibun_address = best_jibun_address
                    self.current_collected_address = best_jibun_address  # 수집된 주소도 구주소로 설정
                    self.logger.info(f"최적 결과 구주소 저장: {best_jibun_address}")
                elif 'best_address' in locals() and best_address:
                    self.current_collected_address = best_address
                    self.logger.info(f"최적 결과 주소 저장: {best_address}")
                
                try:
                    self.driver.switch_to.frame("searchIframe")
                    clickable_results = self.driver.find_elements(By.CSS_SELECTOR, "li.VLTHu.OW9LQ a.place_bluelink")
                    if best_result_index < len(clickable_results):
                        clickable_results[best_result_index].click()
                        wait_time = 2.0 if platform.system() == "Darwin" else 3.0
                        time.sleep(wait_time)
                        
                        self.driver.switch_to.default_content()
                        return self.extract_phone_number_from_detail()
                    else:
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
    
    def extract_phone_number_from_detail(self):
        """상세 페이지에서 전화번호 추출"""
        try:
            self.driver.switch_to.default_content()
            
            # entryIframe에서 a.BfF3H 찾기
            try:
                entry_iframes = self.driver.find_elements(By.ID, "entryIframe")
                if entry_iframes:
                    self.driver.switch_to.frame("entryIframe")
                    bf3h_elements = self.driver.find_elements(By.CSS_SELECTOR, "a.BfF3H")
                    if bf3h_elements:
                        bf3h_elements[0].click()
                        wait_time = 1.5 if platform.system() == "Darwin" else 2.0
                        time.sleep(wait_time)
                        
                        div_elements = self.driver.find_elements(By.CSS_SELECTOR, "div._YI7T.kH0zp")
                        
                        if div_elements:
                            for div in div_elements:
                                em_elements = div.find_elements(By.CSS_SELECTOR, "em")
                                
                                for em in em_elements:
                                    phone_text = em.text.strip()
                                    if phone_text and len(phone_text) > 8 and '-' in phone_text:
                                        print(f"✅ em 태그에서 전화번호 발견: {phone_text}")
                                        self.driver.switch_to.default_content()
                                        return phone_text
                        
                        self.driver.switch_to.default_content()
                        return None
                    
                    self.driver.switch_to.default_content()
                
            except Exception as e:
                print(f"entryIframe 처리 중 오류: {e}")
                self.driver.switch_to.default_content()
            
            # searchIframe에서 a.BfF3H 찾기
            try:
                self.driver.switch_to.frame("searchIframe")
                bf3h_elements = self.driver.find_elements(By.CSS_SELECTOR, "a.BfF3H")
                if bf3h_elements:
                    bf3h_elements[0].click()
                    wait_time = 1.5 if platform.system() == "Darwin" else 2.0
                    time.sleep(wait_time)
                    
                    div_elements = self.driver.find_elements(By.CSS_SELECTOR, "div._YI7T.kH0zp")
                    
                    if div_elements:
                        for div in div_elements:
                            em_elements = div.find_elements(By.CSS_SELECTOR, "em")
                            
                            for em in em_elements:
                                phone_text = em.text.strip()
                                if phone_text and len(phone_text) > 8 and '-' in phone_text:
                                    print(f"✅ em 태그에서 전화번호 발견: {phone_text}")
                                    self.driver.switch_to.default_content()
                                    return phone_text
                    
                    self.driver.switch_to.default_content()
                    return None
                
                self.driver.switch_to.default_content()
                
            except Exception as e:
                print(f"searchIframe 처리 중 오류: {e}")
                self.driver.switch_to.default_content()
            
            # 메인 페이지에서 a.BfF3H 찾기
            try:
                bf3h_elements = self.driver.find_elements(By.CSS_SELECTOR, "a.BfF3H")
                if bf3h_elements:
                    bf3h_elements[0].click()
                    wait_time = 1.5 if platform.system() == "Darwin" else 2.0
                    time.sleep(wait_time)
                    
                    div_elements = self.driver.find_elements(By.CSS_SELECTOR, "div._YI7T.kH0zp")
                    
                    if div_elements:
                        for div in div_elements:
                            em_elements = div.find_elements(By.CSS_SELECTOR, "em")
                            
                            for em in em_elements:
                                phone_text = em.text.strip()
                                if phone_text and len(phone_text) > 8 and '-' in phone_text:
                                    print(f"✅ em 태그에서 전화번호 발견: {phone_text}")
                                    return phone_text
                    
                    return None
                else:
                    print("메인 페이지에서 a.BfF3H를 찾을 수 없음")
                    
            except Exception as e:
                print(f"메인 페이지 처리 중 오류: {e}")
            
            # a.BfF3H를 찾지 못한 경우 span.xlx7Q에서 전화번호 찾기
            return self.extract_phone_number_direct()
            
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
            if not new_phone:
                return 0
            
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
            if not new_phone:
                return ""
            
            # 구주소가 있으면 구주소 반환
            if hasattr(self, 'current_collected_jibun_address') and self.current_collected_jibun_address:
                return self.current_collected_jibun_address
            
            # 구주소가 없으면 수집된 주소 반환
            if hasattr(self, 'current_collected_address') and self.current_collected_address:
                return self.current_collected_address
            
            return "전화번호 수집 성공 (주소 정보 없음)"
        except Exception as e:
            return f"주소 수집 중 오류: {str(e)}"
    
    def crawl_phone_numbers(self, test_count=None):
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
            
            # 결과 파일 초기화
            self.initialize_result_file()
            
            for index, row in test_df.iterrows():
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

# 메인 실행
if __name__ == "__main__":
    print("유연한 네이버 지도 크롤러 시작!")
    print(f"운영체제: {platform.system()}")
    if platform.system() == "Darwin":
        print("맥OS 환경에서 실행됩니다.")
    
    crawler = FlexibleCrawler()
    
    try:
        # 테스트용 5개 처리 (속도 개선 테스트)
        result = crawler.crawl_phone_numbers(test_count=5)
        
        if result:
            print(f"\n크롤링 완료! {result}")
        else:
            print("\n크롤링 실패")
            
    except KeyboardInterrupt:
        print("\n사용자에 의해 중단됨")
    except Exception as e:
        print(f"\n예상치 못한 오류: {e}")
    finally:
        crawler.close()
