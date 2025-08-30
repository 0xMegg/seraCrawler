#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
업소명을 비교해서 temp.csv의 새전화번호를 stores02.csv의 전화번호 칼럼에 업데이트하는 스크립트
"""

import pandas as pd
import numpy as np
from pathlib import Path
import logging

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('phone_update.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

def load_data():
    """CSV 파일들을 로드합니다."""
    try:
        # stores02.csv 로드 (업소명: 4번째 칼럼, 전화번호: 5번째 칼럼)
        stores_df = pd.read_csv('stores02.csv', encoding='utf-8')
        logging.info(f"stores02.csv 로드 완료: {len(stores_df)}개 행")
        
        # temp.csv 로드 (사업장명: 2번째 칼럼, 새전화번호: 5번째 칼럼)
        temp_df = pd.read_csv('temp.csv', encoding='utf-8')
        logging.info(f"temp.csv 로드 완료: {len(temp_df)}개 행")
        
        return stores_df, temp_df
        
    except Exception as e:
        logging.error(f"파일 로드 중 오류 발생: {e}")
        return None, None

def clean_business_name(name):
    """업소명을 정리합니다 (공백 제거, 소문자 변환 등)."""
    if pd.isna(name):
        return ""
    
    # 문자열로 변환
    name = str(name).strip()
    
    # 공백 정리 (여러 공백을 하나로)
    name = ' '.join(name.split())
    
    return name

def find_matching_businesses(stores_df, temp_df):
    """업소명이 일치하는 업체들을 찾습니다."""
    matches = []
    
    # stores02.csv의 업소명 칼럼 (4번째 칼럼, 인덱스 3)
    stores_business_col = stores_df.columns[3]
    
    # temp.csv의 사업장명 칼럼 (2번째 칼럼, 인덱스 1)
    temp_business_col = temp_df.columns[1]
    
    # temp.csv의 새전화번호 칼럼 (5번째 칼럼, 인덱스 4)
    temp_phone_col = temp_df.columns[4]
    
    logging.info(f"stores02.csv 업소명 칼럼: {stores_business_col}")
    logging.info(f"temp.csv 사업장명 칼럼: {temp_business_col}")
    logging.info(f"temp.csv 새전화번호 칼럼: {temp_phone_col}")
    
    for idx, temp_row in temp_df.iterrows():
        temp_business = clean_business_name(temp_row[temp_business_col])
        temp_phone = temp_row[temp_phone_col]
        
        # temp.csv에 있는 모든 업체를 기록 (전화번호 유무와 관계없이)
        for stores_idx, stores_row in stores_df.iterrows():
            stores_business = clean_business_name(stores_row[stores_business_col])
            
            if temp_business == stores_business:
                matches.append({
                    'temp_idx': idx,
                    'stores_idx': stores_idx,
                    'business_name': temp_business,
                    'new_phone': temp_phone,
                    'old_phone': stores_row.iloc[4],  # 전화번호 칼럼 (5번째, 인덱스 4)
                    'has_phone': not (pd.isna(temp_phone) or temp_phone == "")
                })
                break
    
    return matches

def update_phone_numbers(stores_df, matches):
    """전화번호를 업데이트하고 업데이트됨 칼럼을 추가합니다."""
    updated_count = 0
    phone_updated_count = 0
    
    # "업데이트됨" 칼럼 추가 (기본값 0)
    stores_df['업데이트됨'] = 0
    
    for match in matches:
        stores_idx = match['stores_idx']
        new_phone = match['new_phone']
        old_phone = match['old_phone']
        has_phone = match['has_phone']
        
        # 업데이트됨 칼럼에 1 설정 (temp.csv에 있는 모든 업체)
        stores_df.iloc[stores_idx, stores_df.columns.get_loc('업데이트됨')] = 1
        
        # 전화번호가 있는 경우에만 전화번호 업데이트
        if has_phone:
            stores_df.iloc[stores_idx, 4] = new_phone
            logging.info(f"전화번호 업데이트: {match['business_name']} - {old_phone} → {new_phone}")
            phone_updated_count += 1
        else:
            logging.info(f"업데이트됨 표시만: {match['business_name']} - 전화번호 없음")
        
        updated_count += 1
    
    return updated_count, phone_updated_count

def save_updated_file(stores_df, output_filename=None):
    """업데이트된 파일을 저장합니다."""
    if output_filename is None:
        # 원본 파일명에 _updated 접미사 추가
        output_filename = 'stores02_updated.csv'
    
    try:
        stores_df.to_csv(output_filename, index=False, encoding='utf-8-sig')
        logging.info(f"업데이트된 파일 저장 완료: {output_filename}")
        return True
    except Exception as e:
        logging.error(f"파일 저장 중 오류 발생: {e}")
        return False

def main():
    """메인 함수"""
    logging.info("전화번호 업데이트 작업 시작")
    
    # 데이터 로드
    stores_df, temp_df = load_data()
    if stores_df is None or temp_df is None:
        logging.error("데이터 로드 실패")
        return
    
    # 일치하는 업체 찾기
    logging.info("일치하는 업체 찾는 중...")
    matches = find_matching_businesses(stores_df, temp_df)
    logging.info(f"일치하는 업체 {len(matches)}개 발견")
    
    if len(matches) == 0:
        logging.warning("일치하는 업체가 없습니다.")
        return
    
    # 매칭 결과 출력
    for i, match in enumerate(matches[:10]):  # 처음 10개만 출력
        status = "전화번호 있음" if match['has_phone'] else "전화번호 없음"
        logging.info(f"{i+1}. {match['business_name']}: {status}")
    
    if len(matches) > 10:
        logging.info(f"... 외 {len(matches) - 10}개")
    
    # 전화번호 업데이트 및 업데이트됨 칼럼 설정
    logging.info("업데이트 작업 중...")
    updated_count, phone_updated_count = update_phone_numbers(stores_df, matches)
    logging.info(f"업데이트됨 표시 완료: {updated_count}개")
    logging.info(f"전화번호 업데이트 완료: {phone_updated_count}개")
    
    # 업데이트된 파일 저장
    if save_updated_file(stores_df):
        logging.info("작업 완료!")
    else:
        logging.error("파일 저장 실패")

if __name__ == "__main__":
    main()
