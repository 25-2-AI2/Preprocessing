import os
import json
import math
import asyncio
import pandas as pd
from openai import AsyncOpenAI

# 1) API 키 준비 (환경변수 OPENAI_API_KEY에 설정해두기)
client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# 2) SYSTEM PROMPT: 여러 리뷰를 한 번에 처리하도록 수정 (기존 내용 재사용)
SYSTEM_PROMPT = """
You are an expert annotator for aspect-based sentiment analysis of restaurant reviews.

Your task:
- You will receive MULTIPLE restaurant reviews at once in a JSON array called "reviews".
- For EACH review, you must assign integer scores from -2 to +2 for:
  - food_score
  - service_score
  - ambience_score
  - price_score
  - hygiene_score
  - waiting_score
  - accessibility_score
- And binary flags:
  - racism_flag (0 or 1)
  - cash_only_flag (0 or 1)
- Add a short Korean comment summarizing the key reasoning.

Important:
- Use ONLY the information explicitly mentioned or strongly implied in each review.
- If an aspect is not mentioned, set it to 0.
- For EVERY input review, output ONE JSON object with these keys:
  - "id" (copied from input)
  - "food_score", "service_score", "ambience_score", "price_score",
    "hygiene_score", "waiting_score", "accessibility_score",
    "racism_flag", "cash_only_flag", "comment"

You MUST:
- Return a SINGLE valid JSON ARRAY.
- The k-th element in the output array MUST correspond to the k-th element in the input "reviews" array.
- Each element MUST include the same "id" as the corresponding input review.

========================
[SCORING GUIDELINES]
========================

[공통 스케일 (-2 ~ +2)]

각 Aspect는 다음 기준을 통해 -2에서 +2까지 점수를 가진다.

- +2: 매우 긍정 (Very Positive)
  - 강한 칭찬, 최고 수준, 반복적 찬사, 강한 만족
- +1: 긍정 (Positive)
  - 대체로 좋음, 전반적으로 만족
-  0: 중립 / 언급 없음 (Neutral)
  - 해당 Aspect 언급 없음 / 평가 모호 / 도입부용 언급
- -1: 부정 (Negative)
  - 실망, 다소 부정적, 문제 있으나 치명적 아님
- -2: 매우 부정 (Very Negative)
  - 강한 비난, 최악, 위험 요소, 심각한 불만

--------------------------------
1) FOOD (맛·음식·음료) → food_score
--------------------------------
음식뿐 아니라 음료(맥주, 칵테일, 커피 등) 전체 품질.

+2 (매우 긍정)
- "best I’ve ever had", "amazing", "incredible", "perfect"
- "인생 맛집", "완벽", "다시 먹으러 오고 싶다"

+1 (긍정)
- "good", "tasty", "맛있다", "괜찮다"

0 (중립/언급 없음)
- 음식 평가 없음
- "okay", "fine" 등 애매한 표현
- "음식은 괜찮았지만 ..."처럼 뒷부분 부정을 위한 도입부 역할만 하는 경우

-1 (부정)
- "not good", "a bit salty", "underwhelming"
- 맛/양/온도 문제 있지만 치명적이진 않음

-2 (매우 부정)
- "worst", "disgusting", "inedible", "토할 것 같았다"

--------------------------------
2) SERVICE (친절·응대·속도) → service_score
--------------------------------
고객 응대 태도, 친절도, 요청 처리, 서빙/응답 속도 등.

- 친절, 세심한 응대, 자주 챙겨줌 → +1 ~ +2
- 무관심, 불친절, 거친 말투 → -1
- 주문 누락, 완전 무시, 심한 무례함 → -2

단, "음식이 빨리 나왔다"는 경우는 service와 waiting 중 어디에 줄지 문맥을 보고 판단.
(웨이팅 줄/입장 대기 위주면 waiting_score, 테이블 안에서의 응대/서빙이면 service_score 쪽에 반영 가능)

--------------------------------
3) AMBIENCE (분위기·소음·인테리어) → ambience_score
--------------------------------
매장 느낌, 조명, 소음, 인테리어, 분위기, 안전감 등.

+2
- "cozy", "romantic", "beautiful space"
- "분위기가 너무 좋았다", "데이트하기 딱 좋다"

+1
- "nice vibe", "pleasant atmosphere"

0
- 분위기·소음·인테리어 언급 없음

-1
- "noisy", "too crowded", "어수선했다"

-2
- "시끄러워서 대화가 불가능했다", "blasting music all day"
- 싸움, 위협적 분위기, 매우 불쾌한 환경

--------------------------------
4) PRICE (가격·가성비) → price_score
--------------------------------
가격 수용성, 저렴함/비쌈, 가치 대비 만족도.

+2
- "great value", "가성비 최고", "가격이 정말 착하다"
- "full meal only $15–20, amazing value"

+1
- "가격도 괜찮았다", "비싸지 않은 편"

0
- 가격 언급 없음
- 가격 언급 있으나 명확히 긍/부정 아니면 0

-1
- "a bit expensive", "조금 비싸지만 이해 가능"

-2
- "overpriced", "rip-off", "돈 아까움"
- 가격에 대한 강한 분노/배신감

(결제 수단 제약은 cash_only_flag로, 필요시 price_score -1과 함께 쓸 수 있음.)

--------------------------------
5) HYGIENE (위생) → hygiene_score
--------------------------------
청결, 음식 위생, 바닥/테이블/화장실 상태 등.

+2
- "spotless", "VERY CLEAN", "너무 깨끗했다"

+1
- "clean", "깔끔했다"

0
- 위생 언급 없음

-1
- "dirty tables", "조금 지저분했다", 정리 안 된 상태
- 경미한 위생 문제, 오래된 느낌의 재료 등

-2
- 건강/안전 위협 수준:
  - 바퀴벌레, 쥐, 곰팡이, 심각한 이물질
  - 유통기한 지난 재료/재사용 주장
  - "여기 다녀온 뒤 식중독 걸렸다"

--------------------------------
6) WAITING (웨이팅/대기) → waiting_score
--------------------------------
식당 입장, 자리 배정까지의 대기 시간.

+2
- "we were immediately seated", "no wait at all"

+1
- "short wait", "a bit of a wait but fine"

0
- 웨이팅 언급 없음

-1
- "long wait", "had to wait too long"

-2
- "over an hour wait", "ridiculous line", "never again because of the wait"

--------------------------------
7) ACCESSIBILITY (위치·이동·주차) → accessibility_score
--------------------------------
위치 편리성, 대중교통·도보 접근성, 주차 가능 여부.

+2
- "easy parking", "plenty of parking", "perfect location"

+1
- "주차 자리 있긴 했다", "찾기 쉬운 편"

0
- 위치/주차 언급 없음

-1
- "parking was hard", "주차가 조금 불편"

-2
- "parking nightmare", "주차 지옥", "오기가 너무 힘들다"

--------------------------------
8) Binary Tags (0/1)
--------------------------------

8.1 RACISM FLAG → racism_flag
- 인종/민족/국적/외모/언어 기반 차별/혐오가 분명할 때만 1.

- 1 예시:
  - "racist staff", "treated differently because we are Asian"
- 그 외 일반 무례/폭력/싸움은 0.

8.2 CASH ONLY FLAG → cash_only_flag
- 결제 수단 제한 언급 시 1.

- 1 예시:
  - "cash only", "no cards", "현금만 받는다"
- 언급 없으면 0.

--------------------------------
9) 여러 표현이 섞일 때의 원칙
--------------------------------

같은 Aspect에 대해 긍정/부정이 함께 있으면, 평균내지 말고 **절대값이 더 큰 쪽**을 따른다.

- "맛은 좋았지만 너무 짜서 토할 것 같았다" → food_score = -2
- "서비스는 좋았지만 너무 느렸다" → service_score = -1
- "음식은 괜찮았으나, 가격이 너무 비싸다"
  - food_score = 0 (도입부용)
  - price_score = -1 또는 -2 (문맥 따라)

Aspect 언급이 전혀 없거나 애매하면 0.

========================
[YOUR TASK AT RUNTIME]
========================

At runtime, you will receive a JSON array under the key "reviews", where each element has:
- "id": an integer index
- "text": the review text

You MUST respond with ONLY ONE JSON array of the same length, where each element is a JSON object:

{
  "id": same integer as input,
  "food_score": int,
  "service_score": int,
  "ambience_score": int,
  "price_score": int,
  "hygiene_score": int,
  "waiting_score": int,
  "accessibility_score": int,
  "racism_flag": 0 or 1,
  "cash_only_flag": 0 or 1,
  "comment": "short Korean summary of your reasoning"
}

No extra text, no markdown, only the JSON array.
"""

# 3) USER 템플릿: 여러 리뷰를 한 번에 넘김
USER_TEMPLATE_BATCH = """
You are given multiple restaurant reviews in JSON format under the key "reviews".

Each element has:
- "id" (integer)
- "text" (string)

Here is the JSON:

{reviews_json}

For EACH review, produce ONE JSON object with keys:
["id",
 "food_score","service_score","ambience_score","price_score",
 "hygiene_score","waiting_score","accessibility_score",
 "racism_flag","cash_only_flag","comment"]

Rules:
- Scores: integers in [-2,-1,0,1,2]
- Flags: 0 or 1
- comment: short Korean explanation (1~2 sentences) summarizing why you gave these scores.

Return:
- A SINGLE valid JSON ARRAY (no markdown, no extra text).
- The k-th element of the output array must correspond to the k-th element of the "reviews" array.
"""

# 비동기 배치 라벨링 함수
async def label_batch_async(
    texts,
    model: str = "gpt-4.1-mini",
    sem: asyncio.Semaphore | None = None,
):
    """
    texts: 문자열 리스트 (한 배치의 리뷰들)
    return: 각 리뷰에 대한 라벨 dict 리스트 (입력 순서와 동일)
    """
    reviews_payload = [
        {"id": i, "text": str(t)} for i, t in enumerate(texts)
    ]

    user_msg = USER_TEMPLATE_BATCH.format(
        reviews_json=json.dumps(reviews_payload, ensure_ascii=False, indent=2)
    )

    try:
        if sem is not None:
            async with sem:
                resp = await client.chat.completions.create(
                    model=model,
                    temperature=0,
                    messages=[
                        {"role": "system", "content": SYSTEM_PROMPT},
                        {"role": "user", "content": user_msg},
                    ],
                )
        else:
            resp = await client.chat.completions.create(
                model=model,
                temperature=0,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": user_msg},
                ],
            )

        content = resp.choices[0].message.content.strip()
        data_list = json.loads(content)

        # safety: 리스트 아닌 경우 예외 처리
        if not isinstance(data_list, list) or len(data_list) != len(texts):
            raise ValueError("Output length mismatch or not a list")

        # id 기준 정렬
        data_list = sorted(data_list, key=lambda d: d.get("id", 0))

        # id 제거
        for d in data_list:
            d.pop("id", None)

        return data_list

    except Exception as e:
        print("[ERROR] label_batch_async failed:", e)
        # 문제 생긴 배치는 None으로 채워서 반환 (나중에 필터링 가능)
        return [None] * len(texts)


# 5) 데이터 불러오기 및 전체 파이프라인
INPUT_PATH = "parquet_data/reviews.parquet"
OUTPUT_PATH = "parquet_data/reviews_labeled.parquet"
TEXT_COL = "text"

BATCH_SIZE = 20        # 한 번에 보낼 리뷰 개수 (텍스트 길이 보고 10~30 사이에서 조절)
MAX_CONCURRENCY = 5    # 동시에 돌릴 배치 개수 (3~5 정도 추천)


async def main_async():
    df = pd.read_parquet(INPUT_PATH)
    texts = df[TEXT_COL].astype(str).tolist()
    n = len(texts)

    num_batches = math.ceil(n / BATCH_SIZE)
    print(f"Total reviews: {n}, batch_size: {BATCH_SIZE}, num_batches: {num_batches}")

    # 최종 결과를 담을 리스트 (인덱스 맞게 채워넣기)
    all_labels = [None] * n

    # 동시성 제한용 세마포어
    sem = asyncio.Semaphore(MAX_CONCURRENCY)

    async def handle_batch(batch_idx, start, end):
        batch_texts = texts[start:end]
        print(f"[Batch {batch_idx+1}/{num_batches}] labeling reviews {start+1} ~ {end} ...")

        labels = await label_batch_async(
            batch_texts,
            model="gpt-4.1-mini",  # 필요하면 여기서 모델 바꿀 수 있음
            sem=sem,
        )

        # 결과를 전체 리스트에 채워넣기
        all_labels[start:end] = labels

    tasks = []
    for b in range(num_batches):
        start = b * BATCH_SIZE
        end = min((b + 1) * BATCH_SIZE, n)
        tasks.append(asyncio.create_task(handle_batch(b, start, end)))

    # 모든 배치 동시 실행
    await asyncio.gather(*tasks)

    # DataFrame으로 합치기
    labels_df = pd.DataFrame(all_labels)
    df_labeled = pd.concat(
        [df.reset_index(drop=True), labels_df.reset_index(drop=True)],
        axis=1,
    )

    # 저장
    os.makedirs("label_data", exist_ok=True)
    df_labeled.to_parquet(OUTPUT_PATH, index=False)
    csv_path = "label_data/reviews_labeled.csv"
    df_labeled.to_csv(csv_path, index=False, encoding="utf-8-sig")

    print("Done! Saved to:", OUTPUT_PATH, "and", csv_path)