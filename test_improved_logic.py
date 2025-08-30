#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
개선된 검색 결과 분기 처리 로직 테스트 스크립트
"""

from flexible_crawler import FlexibleCrawler

def test_improved_logic():
    """개선된 분기 처리 로직 테스트"""
    print("🔍 개선된 검색 결과 분기 처리 로직 테스트 시작")
    print("=" * 60)
    
    # 테스트 케이스들
    test_cases = [
        {
            "name": "검색 결과 0개",
            "business_name": "존재하지않는업체",
            "dong_name": "일산동",
            "expected": "NO_RESULTS"
        },
        {
            "name": "검색 결과 1개 (데이터 표기됨)",
            "business_name": "테스트업체",
            "dong_name": "일산동",
            "expected": "전화번호 발견"
        },
        {
            "name": "검색 결과 1개 (데이터 미표기)",
            "business_name": "테스트업체2",
            "dong_name": "일산동",
            "expected": "SINGLE_RESULT_NO_PHONE_ULSAN_DONGGU"
        },
        {
            "name": "검색 결과 2개 이상 (울산 동구 있음)",
            "business_name": "테스트업체3",
            "dong_name": "일산동",
            "expected": "전화번호 발견"
        },
        {
            "name": "검색 결과 2개 이상 (울산 동구 없음)",
            "business_name": "테스트업체4",
            "dong_name": "일산동",
            "expected": "MULTIPLE_RESULTS_NO_ULSAN_DONGGU"
        }
    ]
    
    print("📋 테스트 케이스:")
    for i, case in enumerate(test_cases, 1):
        print(f"  {i}. {case['name']}")
        print(f"     업체명: {case['business_name']}")
        print(f"     동이름: {case['dong_name']}")
        print(f"     예상결과: {case['expected']}")
        print()
    
    print("=" * 60)
    print("✅ 테스트 케이스 정의 완료")
    print("실제 테스트는 크롤러 실행 시 확인할 수 있습니다.")
    
    # 분기 처리 로직 설명
    print("\n📚 개선된 분기 처리 로직:")
    print("1. 검색 결과 0개 → 'NO_RESULTS' 반환")
    print("2. 검색 결과 1개:")
    print("   - 데이터 표기됨 → 전화번호 반환")
    print("   - 데이터 미표기 → 클릭 후 전화번호 추출")
    print("   - 전화번호 없음 + 울산 동구 맞음 → 'SINGLE_RESULT_NO_PHONE_ULSAN_DONGGU'")
    print("   - 전화번호 없음 + 울산 동구 아님 → 'SINGLE_RESULT_WRONG_LOCATION'")
    print("   - 정보 확인 불가 → 'SINGLE_RESULT_UNKNOWN'")
    print("3. 검색 결과 2개 이상:")
    print("   - 울산 동구 결과 있음 → 최적 결과 선택 후 전화번호 추출")
    print("   - 울산 동구 결과 없음 → 'MULTIPLE_RESULTS_NO_ULSAN_DONGGU'")
    
    print("\n🔄 1차/2차 검색 전환 규칙:")
    print("- 1차 검색: 사업장명 + 동이름")
    print("- 2차 검색: 사업장명만")
    print("- 울산 동구가 아닌 결과는 2차 검색으로 전환")
    print("- 울산 동구 결과가 없으면 '식당을 찾지 못한 것'으로 판단")

if __name__ == "__main__":
    test_improved_logic()
