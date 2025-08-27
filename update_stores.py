import pandas as pd
import os
import glob
from datetime import datetime

def find_crawling_files():
    """crawling 폴더에서 크롤링 결과 CSV 파일들을 찾습니다."""
    # 현재 디렉토리에서 크롤링 결과 파일들을 찾습니다
    crawling_files = glob.glob("stores_crawling_batch*.csv")
    return sorted(crawling_files)

def read_original_stores():
    """원본 stores.csv 파일을 읽습니다."""
    try:
        df = pd.read_csv('stores.csv')
        return df
    except FileNotFoundError:
        print("stores.csv 파일을 찾을 수 없습니다.")
        return None

def read_crawling_results(file_path):
    """크롤링 결과 파일을 읽습니다."""
    try:
        df = pd.read_csv(file_path)
        return df
    except FileNotFoundError:
        print(f"{file_path} 파일을 찾을 수 없습니다.")
        return None

def create_update_description(row):
    """업데이트 설명을 생성합니다."""
    original_phone = row['기존전화번호'] if pd.notna(row['기존전화번호']) else ''
    new_phone = row['새전화번호'] if pd.notna(row['새전화번호']) else ''
    
    if pd.isna(row['새전화번호']) or row['새전화번호'] == '':
        if row['업데이트'] == '결과없음':
            return "네이버 지도에서 해당 업체를 찾을 수 없었습니다"
        elif row['업데이트'] == 'MULTIPLE_RESULTS_NO_PHONE':
            return "네이버 지도에서 여러 결과가 나왔지만 전화번호 정보가 없었습니다"
        else:
            return "크롤링 결과가 없습니다"
    
    if original_phone == new_phone:
        return "기존 전화번호와 동일합니다"
    elif original_phone == '' and new_phone != '':
        return "기존에 전화번호가 없었는데 새로 찾았습니다"
    else:
        return f"전화번호가 변경되었습니다 (기존: {original_phone} → 새: {new_phone})"

def merge_data(original_df, crawling_df):
    """원본 데이터와 크롤링 결과를 병합합니다."""
    # 원본 데이터 복사
    result_df = original_df.copy()
    
    # 새전화 컬럼 추가 (소재지전화 오른쪽에)
    cols = list(result_df.columns)
    phone_index = cols.index('소재지전화')
    cols.insert(phone_index + 1, '새전화')
    result_df = result_df.reindex(columns=cols)
    result_df['새전화'] = ''
    
    # 크롤링 결과를 순번에 맞게 매핑
    for _, crawling_row in crawling_df.iterrows():
        store_index = crawling_row['순번'] - 1  # 0-based index
        
        if store_index < len(result_df):
            result_df.at[store_index, '새전화'] = crawling_row['새전화번호']
            
            # 업데이트 설명 생성
            update_desc = create_update_description(crawling_row)
            result_df.at[store_index, '업데이트'] = update_desc
    
    return result_df

def main():
    """메인 함수"""
    print("=== stores_updated.csv 생성 스크립트 ===")
    
    # 원본 데이터 읽기
    print("1. 원본 stores.csv 파일을 읽는 중...")
    original_df = read_original_stores()
    if original_df is None:
        return
    
    # 크롤링 결과 파일 찾기
    print("2. 크롤링 결과 파일을 찾는 중...")
    crawling_files = find_crawling_files()
    
    if not crawling_files:
        print("크롤링 결과 파일을 찾을 수 없습니다.")
        print("stores_crawling_batch*.csv 형식의 파일이 필요합니다.")
        return
    
    print(f"발견된 크롤링 파일: {crawling_files}")
    
    # 모든 크롤링 결과를 하나로 합치기
    all_crawling_data = []
    
    for file_path in crawling_files:
        print(f"3. {file_path} 파일을 처리하는 중...")
        crawling_df = read_crawling_results(file_path)
        if crawling_df is not None:
            all_crawling_data.append(crawling_df)
    
    if not all_crawling_data:
        print("처리할 수 있는 크롤링 데이터가 없습니다.")
        return
    
    # 모든 크롤링 데이터를 하나로 합치기
    combined_crawling_df = pd.concat(all_crawling_data, ignore_index=True)
    
    # 중복 제거 (같은 순번이 있다면 마지막 것을 사용)
    combined_crawling_df = combined_crawling_df.drop_duplicates(subset=['순번'], keep='last')
    combined_crawling_df = combined_crawling_df.sort_values('순번')
    
    print(f"4. 총 {len(combined_crawling_df)}개의 크롤링 결과를 처리합니다.")
    
    # 데이터 병합
    print("5. 원본 데이터와 크롤링 결과를 병합하는 중...")
    result_df = merge_data(original_df, combined_crawling_df)
    
    # 결과 저장
    output_file = 'stores_updated.csv'
    print(f"6. 결과를 {output_file}에 저장하는 중...")
    result_df.to_csv(output_file, index=False, encoding='utf-8-sig')
    
    # 통계 출력
    print("\n=== 처리 완료 ===")
    print(f"총 업체 수: {len(result_df)}")
    
    # 업데이트 통계
    update_stats = result_df['업데이트'].value_counts()
    print("\n업데이트 결과 통계:")
    for desc, count in update_stats.items():
        print(f"  {desc}: {count}개")
    
    # 전화번호 분리 통계
    existing_count = result_df['기존전화번호'].notna().sum()
    new_count = result_df['새전화번호'].notna().sum()
    
    # 빈 문자열이 아닌 경우를 카운트
    both_count = result_df[(result_df['기존전화번호'] != '') & (result_df['새전화번호'] != '')].shape[0]
    
    print(f"기존 전화번호만 있는 행: {existing_count - both_count}")
    print(f"새 전화번호만 있는 행: {new_count - both_count}")
    print(f"기존과 새 전화번호 모두 있는 행: {both_count}")

    print(f"\n결과 파일이 {output_file}에 저장되었습니다.")

if __name__ == "__main__":
    main()
