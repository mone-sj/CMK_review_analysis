#!/bin/bash
#path setting
PATH=/usr/bin:/bin:/usr/sbin:/usr/local/bin

export LANG=ko_KR.utf8
# 글로우픽 리뷰 분석 시 실행
cd /home/CMK_review_analysis
python3 /home/CMK_review_analysis/exe_G.py
