#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
울산 동구 데이터만 추출하는 스크립트
"""

import pandas as pd
import re

def extract_ulsan_donggu_data():
    """울산 동구 데이터만 추출"""
    
    # 원본 CSV 파일 읽기
    input_file = 'flexible_crawling_250829112327_수동확인.csv'
    output_file = 'ulsan_donggu_data.csv'
    
    print(f"📁 원본 파일 읽기: {input_file}")
    df = pd.read_csv(input_file)
    
    print(f"📊 전체 데이터: {len(df)}개")
    
    # 울산 동구 데이터만 필터링
    ulsan_donggu_mask = df['기존주소'].str.contains('울산광역시 동구', na=False)
    ulsan_donggu_df = df[ulsan_donggu_mask].copy()
    
    print(f"📊 울산 동구 데이터: {len(ulsan_donggu_df)}개")
    
    # 인덱스 재설정
    ulsan_donggu_df = ulsan_donggu_df.reset_index(drop=True)
    ulsan_donggu_df['인덱스'] = range(1, len(ulsan_donggu_df) + 1)
    
    # 결과 저장
    ulsan_donggu_df.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"💾 울산 동구 데이터 저장: {output_file}")
    
    # 통계 정보 출력
    print("\n📈 울산 동구 데이터 통계:")
    print(f"   - 총 업체 수: {len(ulsan_donggu_df)}개")
    
    # 업데이트 상태별 통계
    status_counts = ulsan_donggu_df['업데이트상태'].value_counts()
    print("\n📊 업데이트 상태별 통계:")
    for status, count in status_counts.items():
        print(f"   - {status}: {count}개")
    
    # 전화번호 수집 성공률
    success_count = len(ulsan_donggu_df[ulsan_donggu_df['새전화번호'].notna() & (ulsan_donggu_df['새전화번호'] != '')])
    success_rate = (success_count / len(ulsan_donggu_df)) * 100
    print(f"\n📞 전화번호 수집 성공률: {success_rate:.1f}% ({success_count}/{len(ulsan_donggu_df)})")
    
    # 동별 분포
    print("\n🏘️ 동별 분포:")
    dong_counts = ulsan_donggu_df['기존주소'].str.extract(r'동구 (\w+동)')[0].value_counts()
    for dong, count in dong_counts.items():
        print(f"   - {dong}: {count}개")
    
    return ulsan_donggu_df

if __name__ == "__main__":
    print("🚀 울산 동구 데이터 추출 시작")
    print("=" * 50)
    
    try:
        df = extract_ulsan_donggu_data()
        print("\n✅ 울산 동구 데이터 추출 완료!")
        
    except Exception as e:
        print(f"❌ 오류 발생: {e}")
        import traceback
        traceback.print_exc()
