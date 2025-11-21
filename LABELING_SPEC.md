# labeling_optimized.py 명세서

## 개요

OpenAI API를 활용하여 레스토랑 리뷰에 Aspect-Based Sentiment Analysis 라벨을 자동으로 부여하는 도구입니다.

## 요구사항

### 환경
- Python 3.9+
- OpenAI API 키

### 패키지 설치
```bash
pip install -r requirements.txt
```

### 환경변수 설정
```bash
# Windows PowerShell
$env:OPENAI_API_KEY="sk-your-key"

# Windows CMD
set OPENAI_API_KEY=sk-your-key
```

## 사용법

### 기본 사용
```bash
python labeling_optimized.py <입력파일>
```

### 예시
```bash
# 단일 파일
python labeling_optimized.py parquet_data/reviews_part1.parquet

# 여러 파일 순차 처리
python labeling_optimized.py parquet_data/reviews_part1.parquet parquet_data/reviews_part2.parquet
```

### CLI 옵션

| 옵션 | 단축 | 기본값 | 설명 |
|------|------|--------|------|
| `--batch-size` | `-b` | 50 | API 호출당 리뷰 개수 |
| `--concurrency` | `-c` | 10 | 동시 API 호출 수 |
| `--model` | `-m` | gpt-4o-mini | OpenAI 모델 |
| `--text-column` | `-t` | cleaned_text | 리뷰 텍스트 컬럼명 |

```bash
# 옵션 사용 예시
python labeling_optimized.py input.parquet --batch-size 30 --concurrency 5 --model gpt-4o
```

## 입력 형식

Parquet 파일에 최소 `cleaned_text` 컬럼 필요 (DATA_SPECIFICATION.md 참조)

## 출력 형식

### 추가되는 라벨 컬럼

| 컬럼 | 타입 | 범위 | 설명 |
|------|------|------|------|
| `food_score` | int | -2 ~ +2 | 음식/음료 품질 |
| `service_score` | int | -2 ~ +2 | 서비스/응대 |
| `ambience_score` | int | -2 ~ +2 | 분위기/인테리어 |
| `price_score` | int | -2 ~ +2 | 가격/가성비 |
| `hygiene_score` | int | -2 ~ +2 | 위생/청결 |
| `waiting_score` | int | -2 ~ +2 | 대기 시간 |
| `accessibility_score` | int | -2 ~ +2 | 접근성/주차 |
| `racism_flag` | int | 0, 1 | 차별 언급 여부 |
| `cash_only_flag` | int | 0, 1 | 현금결제만 가능 여부 |
| `comment` | string | - | 라벨링 근거 요약 (한국어) |

### 점수 기준
- **+2**: 매우 긍정 (best, amazing, perfect)
- **+1**: 긍정 (good, nice)
- **0**: 중립 또는 언급 없음
- **-1**: 부정 (disappointing)
- **-2**: 매우 부정 (worst, disgusting)

### 출력 파일

입력 파일명 기준으로 자동 생성:
```
입력: parquet_data/reviews_part1.parquet

출력:
├── parquet_data/reviews_part1_labeled.parquet
└── label_data/reviews_part1_labeled.csv
```

## 안전 기능

### 체크포인트
- 5배치마다 자동 저장
- 중단 후 재실행 시 이어서 처리
- 저장 위치: `label_data/checkpoint_<파일명>.json`

### 중간 저장
- 처리된 결과 주기적 저장
- 저장 위치: `label_data/intermediate_<파일명>.parquet`

### Rate Limit 대응
- API 한도 초과 시 자동 재시도 (최대 5회)
- 지수 백오프 (2초 ~ 60초)

### 오류 복구
- Ctrl+C 중단 시 현재까지 결과 저장
- 예외 발생 시 자동 저장 후 종료

## 디렉토리 구조

```
review-labeling/
├── labeling_optimized.py    # 메인 스크립트
├── labeling.py              # 원본 (참고용)
├── requirements.txt         # 의존성
├── DATA_SPECIFICATION.md    # 입력 데이터 명세
├── LABELING_SPEC.md         # 이 문서
├── parquet_data/
│   ├── reviews_part1.parquet
│   ├── reviews_part2.parquet
│   └── *_labeled.parquet    # 출력
└── label_data/
    ├── *_labeled.csv        # CSV 출력
    ├── checkpoint_*.json    # 체크포인트 (임시)
    └── intermediate_*.parquet  # 중간 저장 (임시)
```

## 예상 처리 시간

| 리뷰 수 | 예상 시간 | 비고 |
|---------|----------|------|
| 10,000 | ~10분 | batch=50, concurrency=10 |
| 76,623 | ~80분 | reviews_part1 기준 |
| 153,246 | ~160분 | 전체 (part1 + part2) |

※ 네트워크 상태 및 API 응답 속도에 따라 변동
