# ===== CSV 크롤링 설정 =====
# CSV 파일의 컬럼명을 여기서 지정하세요

CSV_CONFIG = {
    # 타겟 CSV 파일명
    'target_file': 'forTest.csv',
    
    # CSV 컬럼 매핑 (실제 CSV 파일의 컬럼명)
    'columns': {
        'business_name': '사업장명',      # 사업장명 컬럼
        'address': '기존주소',     # 주소 컬럼 (동이름 추출용)
        'phone': '기존전화번호'             # 기존 전화번호 컬럼 (선택사항)
    },
    
    # 출력 CSV 컬럼 순서
    'output_columns': [
        '인덱스', '사업장명', '기존주소', '기존전화번호', 
        '새전화번호', '업데이트상태', '주소유사도점수', '수집된주소'
    ]
}

# ===== 다른 CSV 파일 사용 예시 =====
# sera.csv 사용할 때:
# CSV_CONFIG = {
#     'target_file': 'sera.csv',
#     'columns': {
#         'business_name': '업소명',
#         'address': '소재지(지번)',
#         'phone': '전화번호'
#     },
#     'output_columns': [
#         '인덱스', '사업장명', '기존주소', '기존전화번호', 
#         '새전화번호', '업데이트상태', '주소유사도점수', '수집된주소'
#     ]
# }
