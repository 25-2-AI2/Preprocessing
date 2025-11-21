import os
import sys
import json
import math
import asyncio
import argparse
import pandas as pd
from pathlib import Path
from datetime import datetime
from openai import AsyncOpenAI
from tqdm.asyncio import tqdm_asyncio
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
from openai import RateLimitError, APITimeoutError, APIConnectionError

# ============================================================
# 설정
# ============================================================

# API 클라이언트
client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# 기본 경로
DEFAULT_INPUT_DIR = "parquet_data"
DEFAULT_OUTPUT_DIR = "parquet_data"
CHECKPOINT_DIR = "label_data"

# 텍스트 컬럼 (DATA_SPECIFICATION.md 기준)
TEXT_COL = "cleaned_text"
ID_COL = "review_id"

# 성능 설정
BATCH_SIZE = 50           # 한 번에 보낼 리뷰 개수
MAX_CONCURRENCY = 10      # 동시 실행 배치 수
CHECKPOINT_INTERVAL = 5   # N개 배치마다 체크포인트 저장
MODEL = "gpt-4o-mini"     # 속도 우선 모델

# ============================================================
# 최적화된 프롬프트 (압축 버전)
# ============================================================

SYSTEM_PROMPT = """Aspect-based sentiment annotator for restaurant reviews.

For each review, assign scores (-2 to +2) and flags (0/1):
- food_score: taste, quality, drinks
- service_score: staff attitude, response speed  
- ambience_score: atmosphere, noise, interior
- price_score: value for money
- hygiene_score: cleanliness
- waiting_score: wait time for seating
- accessibility_score: location, parking
- racism_flag: discrimination (1 if present)
- cash_only_flag: cash-only payment (1 if mentioned)
- comment: 1-2 sentence Korean summary

Scoring guide:
+2: very positive (best, amazing, perfect)
+1: positive (good, nice)
0: neutral or not mentioned
-1: negative (disappointing, problematic)
-2: very negative (worst, disgusting, dangerous)

Rules:
- Score 0 if aspect not mentioned
- If mixed sentiment, use stronger absolute value
- Output ONLY a JSON array, same order as input
- Each object: id, all scores, both flags, comment"""

USER_TEMPLATE = """Reviews JSON:
{reviews_json}

Return JSON array with keys: id, food_score, service_score, ambience_score, price_score, hygiene_score, waiting_score, accessibility_score, racism_flag, cash_only_flag, comment"""

# ============================================================
# 유틸리티 함수
# ============================================================

def get_paths(input_file: str):
    """입력 파일 기반으로 출력 경로 생성"""
    input_path = Path(input_file)
    stem = input_path.stem  # e.g., "reviews_part1"
    
    # 출력 파일명 생성
    output_parquet = input_path.parent / f"{stem}_labeled.parquet"
    output_csv = Path(CHECKPOINT_DIR) / f"{stem}_labeled.csv"
    checkpoint_file = Path(CHECKPOINT_DIR) / f"checkpoint_{stem}.json"
    intermediate_file = Path(CHECKPOINT_DIR) / f"intermediate_{stem}.parquet"
    
    return {
        'input': input_path,
        'output_parquet': output_parquet,
        'output_csv': output_csv,
        'checkpoint': checkpoint_file,
        'intermediate': intermediate_file,
    }


def load_checkpoint(checkpoint_path: Path):
    """체크포인트 로드"""
    if checkpoint_path.exists():
        try:
            with open(checkpoint_path, 'r', encoding='utf-8') as f:
                checkpoint = json.load(f)
                print(f"✓ 체크포인트 발견: 배치 {checkpoint['last_batch']+1}부터 재시작")
                return checkpoint
        except Exception as e:
            print(f"⚠ 체크포인트 로드 실패: {e}")
    return None


def save_checkpoint(checkpoint_path: Path, last_batch: int, all_labels: list, completed_batches: set):
    """체크포인트 저장"""
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    checkpoint = {
        'last_batch': last_batch,
        'labels': all_labels,
        'completed_batches': list(completed_batches),
        'timestamp': datetime.now().isoformat()
    }
    with open(checkpoint_path, 'w', encoding='utf-8') as f:
        json.dump(checkpoint, f, ensure_ascii=False)


def save_intermediate(intermediate_path: Path, df_original: pd.DataFrame, all_labels: list):
    """중간 결과 저장 (Parquet)"""
    intermediate_path.parent.mkdir(parents=True, exist_ok=True)
    
    # None이 아닌 라벨만 있는 행 확인
    valid_indices = [i for i, label in enumerate(all_labels) if label is not None]
    
    if not valid_indices:
        return
    
    # 유효한 라벨로 DataFrame 생성
    labels_df = pd.DataFrame([all_labels[i] if all_labels[i] else {} for i in range(len(all_labels))])
    df_partial = pd.concat([df_original.reset_index(drop=True), labels_df.reset_index(drop=True)], axis=1)
    
    df_partial.to_parquet(intermediate_path, index=False)
    print(f"  💾 중간 저장 완료: {len(valid_indices)}/{len(all_labels)} 리뷰 처리됨")


def cleanup_checkpoint(checkpoint_path: Path):
    """완료 후 체크포인트 삭제"""
    if checkpoint_path.exists():
        checkpoint_path.unlink()
        print("✓ 체크포인트 파일 정리 완료")


# ============================================================
# API 호출 함수 (재시도 로직 포함)
# ============================================================

@retry(
    retry=retry_if_exception_type((RateLimitError, APITimeoutError, APIConnectionError)),
    wait=wait_exponential(multiplier=1, min=2, max=60),
    stop=stop_after_attempt(5),
    before_sleep=lambda retry_state: print(f"  ⏳ 재시도 대기 중... (시도 {retry_state.attempt_number}/5)")
)
async def call_api(messages: list, sem: asyncio.Semaphore):
    """API 호출 (Rate Limit 대응)"""
    async with sem:
        resp = await client.chat.completions.create(
            model=MODEL,
            temperature=0,
            messages=messages,
            timeout=120
        )
        return resp


async def label_batch_async(
    texts: list,
    batch_idx: int,
    sem: asyncio.Semaphore,
) -> list:
    """배치 라벨링"""
    reviews_payload = [{"id": i, "text": str(t)} for i, t in enumerate(texts)]
    user_msg = USER_TEMPLATE.format(
        reviews_json=json.dumps(reviews_payload, ensure_ascii=False)
    )
    
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user_msg},
    ]
    
    try:
        resp = await call_api(messages, sem)
        content = resp.choices[0].message.content.strip()
        
        # JSON 파싱 (마크다운 코드블록 제거)
        if content.startswith("```"):
            content = content.split("```")[1]
            if content.startswith("json"):
                content = content[4:]
        content = content.strip()
        
        data_list = json.loads(content)
        
        if not isinstance(data_list, list) or len(data_list) != len(texts):
            raise ValueError(f"Output mismatch: expected {len(texts)}, got {len(data_list) if isinstance(data_list, list) else 'non-list'}")
        
        # id 기준 정렬 후 id 제거
        data_list = sorted(data_list, key=lambda d: d.get("id", 0))
        for d in data_list:
            d.pop("id", None)
        
        return data_list
    
    except json.JSONDecodeError as e:
        print(f"  ❌ [배치 {batch_idx}] JSON 파싱 오류: {e}")
        return [None] * len(texts)
    except Exception as e:
        print(f"  ❌ [배치 {batch_idx}] 오류: {e}")
        return [None] * len(texts)


# ============================================================
# 메인 파이프라인
# ============================================================

async def main_async(input_file: str):
    print("=" * 60)
    print("🚀 리뷰 라벨링 시작")
    print("=" * 60)
    
    # 경로 설정
    paths = get_paths(input_file)
    
    # 입력 파일 확인
    if not paths['input'].exists():
        print(f"❌ 입력 파일을 찾을 수 없습니다: {paths['input']}")
        sys.exit(1)
    
    # 데이터 로드
    df = pd.read_parquet(paths['input'])
    
    # 필수 컬럼 확인
    if TEXT_COL not in df.columns:
        print(f"❌ 필수 컬럼 '{TEXT_COL}'이 없습니다.")
        print(f"   사용 가능한 컬럼: {list(df.columns)}")
        sys.exit(1)
    
    texts = df[TEXT_COL].astype(str).tolist()
    n = len(texts)
    num_batches = math.ceil(n / BATCH_SIZE)
    
    print(f"📁 입력: {paths['input']}")
    print(f"📁 출력: {paths['output_parquet']}")
    print(f"📊 총 리뷰: {n:,}개")
    print(f"📦 배치 크기: {BATCH_SIZE}, 총 배치: {num_batches}")
    print(f"⚡ 동시 실행: {MAX_CONCURRENCY}")
    print(f"🤖 모델: {MODEL}")
    print("-" * 60)
    
    # 체크포인트 확인
    checkpoint = load_checkpoint(paths['checkpoint'])
    if checkpoint:
        all_labels = checkpoint['labels']
        completed_batches = set(checkpoint.get('completed_batches', []))
        pending_batches = [b for b in range(num_batches) if b not in completed_batches]
    else:
        all_labels = [None] * n
        completed_batches = set()
        pending_batches = list(range(num_batches))
    
    print(f"📋 처리 대기 배치: {len(pending_batches)}/{num_batches}")
    
    # 세마포어 (동시성 제한)
    sem = asyncio.Semaphore(MAX_CONCURRENCY)
    
    # 진행 상황 추적
    processed_count = 0
    lock = asyncio.Lock()
    
    async def handle_batch(batch_idx: int):
        nonlocal processed_count
        
        start = batch_idx * BATCH_SIZE
        end = min((batch_idx + 1) * BATCH_SIZE, n)
        batch_texts = texts[start:end]
        
        labels = await label_batch_async(batch_texts, batch_idx, sem)
        
        async with lock:
            all_labels[start:end] = labels
            completed_batches.add(batch_idx)
            processed_count += 1
            
            # 주기적 체크포인트 저장
            if processed_count % CHECKPOINT_INTERVAL == 0:
                save_checkpoint(paths['checkpoint'], batch_idx, all_labels, completed_batches)
                save_intermediate(paths['intermediate'], df, all_labels)
        
        return batch_idx
    
    # 배치 처리 (진행률 표시)
    tasks = [handle_batch(b) for b in pending_batches]
    
    try:
        await tqdm_asyncio.gather(
            *tasks,
            desc="라벨링 진행",
            total=len(pending_batches),
            unit="batch"
        )
    except KeyboardInterrupt:
        print("\n⚠ 중단됨! 현재까지 결과 저장 중...")
        save_checkpoint(paths['checkpoint'], max(completed_batches) if completed_batches else 0, all_labels, completed_batches)
        save_intermediate(paths['intermediate'], df, all_labels)
        print("✓ 저장 완료. 다시 실행하면 이어서 진행됩니다.")
        return
    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")
        print("현재까지 결과 저장 중...")
        save_checkpoint(paths['checkpoint'], max(completed_batches) if completed_batches else 0, all_labels, completed_batches)
        save_intermediate(paths['intermediate'], df, all_labels)
        print("✓ 저장 완료. 다시 실행하면 이어서 진행됩니다.")
        raise
    
    # 최종 결과 저장
    print("-" * 60)
    print("💾 최종 결과 저장 중...")
    
    labels_df = pd.DataFrame(all_labels)
    df_labeled = pd.concat([df.reset_index(drop=True), labels_df.reset_index(drop=True)], axis=1)
    
    # Parquet 저장
    paths['output_parquet'].parent.mkdir(parents=True, exist_ok=True)
    df_labeled.to_parquet(paths['output_parquet'], index=False)
    
    # CSV 저장
    paths['output_csv'].parent.mkdir(parents=True, exist_ok=True)
    df_labeled.to_csv(paths['output_csv'], index=False, encoding="utf-8-sig")
    
    # 실패한 항목 확인
    failed_count = sum(1 for label in all_labels if label is None)
    success_count = n - failed_count
    
    print("=" * 60)
    print("✅ 완료!")
    print(f"   - 성공: {success_count:,}/{n:,} ({success_count/n*100:.1f}%)")
    if failed_count > 0:
        print(f"   - 실패: {failed_count:,}개")
    print(f"   - Parquet: {paths['output_parquet']}")
    print(f"   - CSV: {paths['output_csv']}")
    print("=" * 60)
    
    # 체크포인트 정리
    cleanup_checkpoint(paths['checkpoint'])
    
    # 중간 파일 정리
    if paths['intermediate'].exists():
        paths['intermediate'].unlink()


# ============================================================
# 여러 파일 일괄 처리
# ============================================================

async def process_multiple_files(input_files: list):
    """여러 파일 순차 처리"""
    for i, input_file in enumerate(input_files, 1):
        print(f"\n{'#' * 60}")
        print(f"# 파일 {i}/{len(input_files)}: {input_file}")
        print(f"{'#' * 60}\n")
        await main_async(input_file)


# ============================================================
# CLI 인터페이스
# ============================================================

def parse_args():
    parser = argparse.ArgumentParser(
        description="Restaurant Review Labeling Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # 단일 파일 처리
  python labeling_optimized.py parquet_data/reviews_part1.parquet
  
  # 여러 파일 처리
  python labeling_optimized.py parquet_data/reviews_part1.parquet parquet_data/reviews_part2.parquet
  
  # 와일드카드 사용 (shell에서)
  python labeling_optimized.py parquet_data/reviews_part*.parquet
  
  # 설정 변경
  python labeling_optimized.py input.parquet --batch-size 30 --concurrency 5
        """
    )
    
    parser.add_argument(
        'input_files',
        nargs='+',
        help='입력 parquet 파일 경로 (여러 개 가능)'
    )
    
    parser.add_argument(
        '--batch-size', '-b',
        type=int,
        default=BATCH_SIZE,
        help=f'배치 크기 (기본값: {BATCH_SIZE})'
    )
    
    parser.add_argument(
        '--concurrency', '-c',
        type=int,
        default=MAX_CONCURRENCY,
        help=f'동시 실행 수 (기본값: {MAX_CONCURRENCY})'
    )
    
    parser.add_argument(
        '--model', '-m',
        type=str,
        default=MODEL,
        help=f'사용할 모델 (기본값: {MODEL})'
    )
    
    parser.add_argument(
        '--text-column', '-t',
        type=str,
        default=TEXT_COL,
        help=f'텍스트 컬럼명 (기본값: {TEXT_COL})'
    )
    
    return parser.parse_args()


# ============================================================
# 엔트리포인트
# ============================================================

if __name__ == "__main__":
    args = parse_args()
    
    # 전역 설정 업데이트
    BATCH_SIZE = args.batch_size
    MAX_CONCURRENCY = args.concurrency
    MODEL = args.model
    TEXT_COL = args.text_column
    
    # API 키 확인
    if not os.getenv("OPENAI_API_KEY"):
        print("❌ OPENAI_API_KEY 환경변수가 설정되지 않았습니다.")
        print("   설정 방법:")
        print("   - Windows CMD: set OPENAI_API_KEY=sk-your-key")
        print("   - PowerShell: $env:OPENAI_API_KEY=\"sk-your-key\"")
        sys.exit(1)
    
    # 실행
    if len(args.input_files) == 1:
        asyncio.run(main_async(args.input_files[0]))
    else:
        asyncio.run(process_multiple_files(args.input_files))
