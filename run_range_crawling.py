#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
95번째부터 340번째 행까지 크롤링 실행 스크립트
"""

from flexible_crawler import FlexibleCrawler

def main():
    print("🚀 범위 크롤링 시작")
    print("📋 크롤링 범위: 95번째 ~ 340번째 행")
    print("=" * 50)
    
    try:
        # 크롤러 초기화
        crawler = FlexibleCrawler()
        
        # 범위 크롤링 실행
        crawler.crawl_range(start_row=1)
        
        print("\n✅ 크롤링 완료!")
        
    except Exception as e:
        print(f"❌ 크롤링 실행 중 오류: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
