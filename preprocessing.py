"""
Google Maps 리뷰 전처리 스크립트
2025.11.16 기준으로 날짜 표준화

주요 기능:
1. 중복 리뷰 제거 (review_id 기준)
2. 빈 텍스트 또는 너무 짧은 리뷰 필터링 (20자 미만)
3. NULL 값 처리
4. 날짜 형식 표준화 (2025.11.16 기준)
5. 특수문자 정리 (이모지는 [EMOJI_name] 형식으로 변환)
6. 클리닝 (URL, HTML, 제어문자 제거. 전화번호·이메일 마스킹. 다중 공백 정리)
"""

import json
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set
from collections import defaultdict
import unicodedata


class EmojiConverter:
    """이모지를 텍스트 태그로 변환"""
    
    # 주요 이모지 매핑
    EMOJI_MAP = {
        '😀': 'grinning', '😃': 'smiley', '😄': 'smile', '😁': 'grin',
        '😆': 'laughing', '😅': 'sweat_smile', '🤣': 'rofl', '😂': 'joy',
        '🙂': 'slightly_smiling', '🙃': 'upside_down', '😉': 'wink', '😊': 'blush',
        '😇': 'innocent', '🥰': 'smiling_face_with_hearts', '😍': 'heart_eyes', '🤩': 'star_struck',
        '😘': 'kissing_heart', '😗': 'kissing', '☺️': 'relaxed', '😚': 'kissing_closed_eyes',
        '😙': 'kissing_smiling_eyes', '🥲': 'smiling_face_with_tear', '😋': 'yum', '😛': 'stuck_out_tongue',
        '😜': 'stuck_out_tongue_winking_eye', '🤪': 'zany_face', '😝': 'stuck_out_tongue_closed_eyes',
        '🤑': 'money_mouth', '🤗': 'hugs', '🤭': 'hand_over_mouth', '🤫': 'shushing',
        '🤔': 'thinking', '🤐': 'zipper_mouth', '🤨': 'raised_eyebrow', '😐': 'neutral_face',
        '😑': 'expressionless', '😶': 'no_mouth', '😏': 'smirk', '😒': 'unamused',
        '🙄': 'rolling_eyes', '😬': 'grimacing', '🤥': 'lying_face', '😌': 'relieved',
        '😔': 'pensive', '😪': 'sleepy', '🤤': 'drooling', '😴': 'sleeping',
        '😷': 'mask', '🤒': 'face_with_thermometer', '🤕': 'face_with_head_bandage',
        '🤢': 'nauseated', '🤮': 'vomiting', '🤧': 'sneezing', '🥵': 'hot',
        '🥶': 'cold', '🥴': 'woozy', '😵': 'dizzy', '🤯': 'exploding_head',
        '😕': 'confused', '😟': 'worried', '🙁': 'slightly_frowning', '☹️': 'frowning',
        '😮': 'open_mouth', '😯': 'hushed', '😲': 'astonished', '😳': 'flushed',
        '🥺': 'pleading', '😦': 'frowning_open_mouth', '😧': 'anguished', '😨': 'fearful',
        '😰': 'cold_sweat', '😥': 'disappointed_relieved', '😢': 'cry', '😭': 'sob',
        '😱': 'scream', '😖': 'confounded', '😣': 'persevere', '😞': 'disappointed',
        '😓': 'sweat', '😩': 'weary', '😫': 'tired', '🥱': 'yawning',
        '😤': 'triumph', '😡': 'rage', '😠': 'angry', '🤬': 'cursing',
        '👍': 'thumbs_up', '👎': 'thumbs_down', '👏': 'clap', '🙌': 'raised_hands',
        '👐': 'open_hands', '🤲': 'palms_up', '🤝': 'handshake', '🙏': 'pray',
        '✨': 'sparkles', '⭐': 'star', '🌟': 'glowing_star', '💫': 'dizzy_star',
        '✅': 'check', '❌': 'x', '⭕': 'o', '❗': 'exclamation',
        '❓': 'question', '💯': 'hundred', '🔥': 'fire', '💥': 'boom',
        '❤️': 'heart', '🧡': 'orange_heart', '💛': 'yellow_heart', '💚': 'green_heart',
        '💙': 'blue_heart', '💜': 'purple_heart', '🖤': 'black_heart', '🤍': 'white_heart',
        '🤎': 'brown_heart', '💔': 'broken_heart', '❣️': 'heart_exclamation', '💕': 'two_hearts',
        '💞': 'revolving_hearts', '💓': 'heartbeat', '💗': 'heartpulse', '💖': 'sparkling_heart',
        '💘': 'cupid', '💝': 'gift_heart', '💟': 'heart_decoration',
        '🍕': 'pizza', '🍔': 'hamburger', '🍟': 'fries', '🌭': 'hotdog',
        '🥪': 'sandwich', '🌮': 'taco', '🌯': 'burrito', '🥙': 'stuffed_flatbread',
        '🥗': 'salad', '🍝': 'spaghetti', '🍜': 'ramen', '🍲': 'stew',
        '🍛': 'curry', '🍣': 'sushi', '🍱': 'bento', '🥟': 'dumpling',
        '🍤': 'fried_shrimp', '🍙': 'rice_ball', '🍚': 'rice', '🍘': 'rice_cracker',
        '🍥': 'fish_cake', '🥮': 'moon_cake', '🍢': 'oden', '🍡': 'dango',
        '🍧': 'shaved_ice', '🍨': 'ice_cream', '🍦': 'soft_ice_cream', '🥧': 'pie',
        '🧁': 'cupcake', '🍰': 'cake', '🎂': 'birthday_cake', '🍮': 'custard',
        '🍭': 'lollipop', '🍬': 'candy', '🍫': 'chocolate', '🍿': 'popcorn',
        '🍩': 'donut', '🍪': 'cookie', '🌰': 'chestnut', '🥜': 'peanuts',
        '☕': 'coffee', '🍵': 'tea', '🧃': 'juice_box', '🥤': 'cup_with_straw',
        '🧋': 'bubble_tea', '🍶': 'sake', '🍺': 'beer', '🍻': 'beers',
        '🥂': 'champagne_glass', '🍷': 'wine', '🥃': 'whisky', '🍸': 'cocktail',
        '🍹': 'tropical_drink', '🧉': 'mate', '🍾': 'champagne',
    }
    
    @classmethod
    def convert_emoji_to_tag(cls, text: str) -> str:
        """이모지를 [EMOJI_name] 형식으로 변환 (한글/영어/숫자는 변환하지 않음)"""
        result = text
        
        # 1단계: 매핑된 이모지 변환
        for emoji, name in cls.EMOJI_MAP.items():
            if emoji in result:
                result = result.replace(emoji, f'[EMOJI_{name}]')
        
        # 2단계: 남은 이모지만 [EMOJI_unknown]으로 변환
        # 각 문자를 검사하여 실제 이모지인지 확인
        def is_emoji(char):
            """문자가 이모지인지 확인 (영어, 스페인어, 한국어, 이탈리아어, 프랑스어 등 제외)"""
            # 이미 변환된 태그는 건드리지 않음
            if char == '[' or char == ']':
                return False
            
            cp = ord(char)
            
            # 기본 ASCII 범위 제외 (영어, 숫자, 기본 기호)
            if cp < 0x80:
                return False
            
            # 라틴 확장 문자 제외 (스페인어, 이탈리아어, 프랑스어 등)
            # Latin-1 Supplement: 0080-00FF (á, é, í, ó, ú, ñ, à, è, ç 등)
            if 0x0080 <= cp <= 0x00FF:
                return False
            
            # Latin Extended-A: 0100-017F (추가 라틴 문자)
            if 0x0100 <= cp <= 0x017F:
                return False
            
            # Latin Extended-B: 0180-024F (추가 라틴 문자)
            if 0x0180 <= cp <= 0x024F:
                return False
            
            # 한글 범위 제외 (AC00-D7AF: 한글 음절, 1100-11FF: 한글 자모)
            if 0xAC00 <= cp <= 0xD7AF or 0x1100 <= cp <= 0x11FF:
                return False
            
            # 실제 이모지 범위만 True
            # 감정 이모티콘
            if 0x1F600 <= cp <= 0x1F64F:
                return True
            # 기호 & 픽토그램
            if 0x1F300 <= cp <= 0x1F5FF:
                return True
            # 교통 & 지도
            if 0x1F680 <= cp <= 0x1F6FF:
                return True
            # 국기
            if 0x1F1E0 <= cp <= 0x1F1FF:
                return True
            # 추가 이모지
            if 0x1F900 <= cp <= 0x1F9FF:
                return True
            # 최신 이모지
            if 0x1FA70 <= cp <= 0x1FAFF:
                return True
            # 기타 특수 이모지
            if 0x2600 <= cp <= 0x26FF:
                return True
            if 0x2700 <= cp <= 0x27BF:
                return True
            
            # 유니코드 카테고리로 추가 확인 (Symbol, other)
            category = unicodedata.category(char)
            if category == 'So':
                # 단, CJK 통합 한자 등은 제외
                if 0x4E00 <= cp <= 0x9FFF:  # CJK 통합 한자
                    return False
                if 0x3400 <= cp <= 0x4DBF:  # CJK 통합 한자 확장 A
                    return False
                return True
            
            return False
        
        # 문자별로 처리
        converted = []
        i = 0
        in_tag = False
        
        while i < len(result):
            char = result[i]
            
            # [EMOJI_xxx] 태그 내부는 건드리지 않음
            if char == '[':
                # 태그 시작인지 확인
                if result[i:i+7] == '[EMOJI_':
                    in_tag = True
                    # 태그 끝까지 찾기
                    end = result.find(']', i)
                    if end != -1:
                        converted.append(result[i:end+1])
                        i = end + 1
                        in_tag = False
                        continue
            
            if not in_tag and is_emoji(char):
                converted.append('[EMOJI_unknown]')
            else:
                converted.append(char)
            
            i += 1
        
        return ''.join(converted)


class DateParser:
    """날짜 파싱 및 표준화 (2025.11.16 기준)"""
    
    BASE_DATE = datetime(2025, 11, 16)
    
    @classmethod
    def parse_relative_date(cls, date_str: str) -> str:
        """
        상대적 날짜를 절대 날짜로 변환
        예: "16시간 전" -> "2025.11.15"
        """
        if not date_str or date_str.strip() == '':
            return ''
        
        # '수정일:' 접두사 제거
        date_str = date_str.replace('수정일:', '').strip()
        
        # 한국어 패턴
        patterns = {
            r'(\d+)시간\s*전': lambda x: cls.BASE_DATE - timedelta(hours=int(x)),
            r'(\d+)일\s*전': lambda x: cls.BASE_DATE - timedelta(days=int(x)),
            r'(\d+)주\s*전': lambda x: cls.BASE_DATE - timedelta(weeks=int(x)),
            r'(\d+)달\s*전': lambda x: cls.BASE_DATE - timedelta(days=int(x)*30),
            r'(\d+)개월\s*전': lambda x: cls.BASE_DATE - timedelta(days=int(x)*30),
            r'(\d+)년\s*전': lambda x: cls.BASE_DATE - timedelta(days=int(x)*365),
        }
        
        for pattern, delta_func in patterns.items():
            match = re.search(pattern, date_str)
            if match:
                date = delta_func(match.group(1))
                return date.strftime('%Y.%m.%d')
        
        # 이미 표준 형식인 경우 반환
        # YYYY.MM.DD, YYYY-MM-DD, YYYY/MM/DD 등
        date_formats = [
            r'(\d{4})\.(\d{1,2})\.(\d{1,2})',
            r'(\d{4})-(\d{1,2})-(\d{1,2})',
            r'(\d{4})/(\d{1,2})/(\d{1,2})',
        ]
        
        for fmt in date_formats:
            match = re.search(fmt, date_str)
            if match:
                year, month, day = match.groups()
                return f"{year}.{month.zfill(2)}.{day.zfill(2)}"
        
        # 파싱 실패 시 원본 반환
        return date_str
    
    @classmethod
    def is_valid_date(cls, date_str: str) -> bool:
        """날짜 형식이 유효한지 확인"""
        if not date_str:
            return False
        
        pattern = r'\d{4}\.\d{2}\.\d{2}'
        return bool(re.match(pattern, date_str))


class TextCleaner:
    """텍스트 정제 클래스"""
    
    @staticmethod
    def remove_urls(text: str) -> str:
        """URL 제거"""
        url_pattern = r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
        return re.sub(url_pattern, '', text)
    
    @staticmethod
    def remove_html_tags(text: str) -> str:
        """HTML 태그 제거"""
        return re.sub(r'<[^>]+>', '', text)
    
    @staticmethod
    def remove_control_characters(text: str) -> str:
        """제어 문자 제거 (탭, 줄바꿈 제외)"""
        # 제어 문자 중 \t, \n, \r만 유지
        return ''.join(char for char in text if unicodedata.category(char)[0] != 'C' or char in '\t\n\r')
    
    @staticmethod
    def mask_phone_numbers(text: str) -> str:
        """전화번호 마스킹"""
        # 한국 전화번호 패턴
        phone_patterns = [
            r'\b0\d{1,2}-\d{3,4}-\d{4}\b',  # 02-1234-5678, 010-1234-5678
            r'\b0\d{9,10}\b',  # 0212345678, 01012345678
        ]
        
        # 미국 전화번호 패턴
        us_phone_patterns = [
            r'\b\(\d{3}\)\s?\d{3}-\d{4}\b',  # (123) 456-7890
            r'\b\d{3}-\d{3}-\d{4}\b',  # 123-456-7890
        ]
        
        result = text
        for pattern in phone_patterns + us_phone_patterns:
            result = re.sub(pattern, '[PHONE]', result)
        
        return result
    
    @staticmethod
    def mask_emails(text: str) -> str:
        """이메일 마스킹"""
        email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        return re.sub(email_pattern, '[EMAIL]', text)
    
    @staticmethod
    def normalize_whitespace(text: str) -> str:
        """다중 공백 정리"""
        # 탭을 공백으로 변환
        text = text.replace('\t', ' ')
        
        # 연속된 공백을 하나로
        text = re.sub(r' +', ' ', text)
        
        # 연속된 줄바꿈을 최대 2개로
        text = re.sub(r'\n{3,}', '\n\n', text)
        
        # 각 줄의 앞뒤 공백 제거
        lines = text.split('\n')
        lines = [line.strip() for line in lines]
        text = '\n'.join(lines)
        
        # 전체 텍스트의 앞뒤 공백 제거
        return text.strip()
    
    @classmethod
    def clean_text(cls, text: str) -> str:
        """전체 클리닝 프로세스"""
        if not text:
            return ''
        
        # 1. URL 제거
        text = cls.remove_urls(text)
        
        # 2. HTML 태그 제거
        text = cls.remove_html_tags(text)
        
        # 3. 제어 문자 제거
        text = cls.remove_control_characters(text)
        
        # 4. 전화번호 마스킹
        text = cls.mask_phone_numbers(text)
        
        # 5. 이메일 마스킹
        text = cls.mask_emails(text)
        
        # 6. 공백 정리
        text = cls.normalize_whitespace(text)
        
        return text


class ReviewPreprocessor:
    """리뷰 전처리 메인 클래스"""
    
    def __init__(self, min_text_length: int = 20):
        self.min_text_length = min_text_length
        self.seen_review_ids: Set[str] = set()
        self.stats = defaultdict(int)
    
    def is_valid_review(self, review: Dict) -> tuple[bool, str]:
        """
        리뷰 유효성 검증
        Returns: (is_valid, reason)
        """
        # 1. review_id 중복 체크
        review_id = review.get('review_id', '')
        if not review_id:
            return False, 'no_review_id'
        
        if review_id in self.seen_review_ids:
            return False, 'duplicate'
        
        # 2. 텍스트 존재 및 길이 체크
        text = review.get('text', '')
        if text is None or text.strip() == '':
            return False, 'empty_text'
        
        if len(text.strip()) < self.min_text_length:
            return False, 'too_short'
        
        return True, 'valid'
    
    def handle_null_values(self, review: Dict) -> Dict:
        """NULL 값 처리"""
        # 기본값 설정
        defaults = {
            'date': '',
            'language': 'unknown',
            'rating': 0,
            'review_id': '',
            'text': ''
        }
        
        processed = {}
        for key, default_value in defaults.items():
            value = review.get(key)
            if value is None or (isinstance(value, str) and value.strip() == ''):
                processed[key] = default_value
            else:
                processed[key] = value
        
        # 추가 필드도 포함
        for key, value in review.items():
            if key not in processed:
                processed[key] = value if value is not None else ''
        
        return processed
    
    def preprocess_review(self, review: Dict, restaurant_info: Dict) -> Optional[Dict]:
        """개별 리뷰 전처리"""
        # 1. NULL 값 처리
        review = self.handle_null_values(review)
        
        # 2. 유효성 검증
        is_valid, reason = self.is_valid_review(review)
        if not is_valid:
            self.stats[f'filtered_{reason}'] += 1
            return None
        
        # review_id 추가
        self.seen_review_ids.add(review['review_id'])
        
        # 3. 텍스트 전처리
        original_text = review['text']
        
        # 3-1. 이모지 변환
        text_with_emoji_tags = EmojiConverter.convert_emoji_to_tag(original_text)
        
        # 3-2. 텍스트 클리닝
        cleaned_text = TextCleaner.clean_text(text_with_emoji_tags)
        
        # 클리닝 후 길이 재확인
        if len(cleaned_text.strip()) < self.min_text_length:
            self.stats['filtered_too_short_after_cleaning'] += 1
            return None
        
        # 4. 날짜 표준화
        parsed_date = DateParser.parse_relative_date(review.get('date', ''))
        
        # 5. 전처리된 리뷰 생성
        processed_review = {
            'review_id': review['review_id'],
            'original_text': original_text,
            'cleaned_text': cleaned_text,
            'date': parsed_date,
            'date_valid': DateParser.is_valid_date(parsed_date),
            'language': review.get('language', 'unknown'),
            'rating': review.get('rating', 0),
            
            # 레스토랑 정보
            'restaurant_name': restaurant_info.get('name', ''),
            'restaurant_place_id': restaurant_info.get('place_id', ''),
            'restaurant_grid': restaurant_info.get('grid', ''),
            'restaurant_address': restaurant_info.get('address', ''),
            'restaurant_rating': restaurant_info.get('rating', 0),
            'restaurant_phone': restaurant_info.get('phone_number', ''),
            
            # 메타 정보
            'char_count': len(cleaned_text),
            'word_count': len(cleaned_text.split()),
        }
        
        self.stats['processed'] += 1
        return processed_review
    
    def process_restaurant_file(self, file_path: Path) -> List[Dict]:
        """레스토랑 파일 처리"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            restaurant_info = {
                'name': data.get('name'),
                'place_id': data.get('place_id'),
                'grid': data.get('grid'),
                'address': data.get('address'),
                'rating': data.get('rating'),
                'user_ratings_total': data.get('user_ratings_total'),
                'phone_number': data.get('phone_number'),
            }
            
            processed_reviews = []
            
            for review in data.get('reviews', []):
                processed = self.preprocess_review(review, restaurant_info)
                if processed:
                    processed_reviews.append(processed)
            
            return processed_reviews
            
        except Exception as e:
            print(f"Error processing {file_path}: {e}")
            self.stats['errors'] += 1
            return []
    
    def process_all_files(self, input_dir: Path, output_dir: Path):
        """모든 파일 처리"""
        input_dir = Path(input_dir)
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        all_processed_reviews = []
        
        # 모든 JSON 파일 찾기
        json_files = list(input_dir.rglob('*.json'))
        total_files = len(json_files)
        
        print(f"총 {total_files}개 파일 처리 시작...")
        print(f"기준 날짜: {DateParser.BASE_DATE.strftime('%Y.%m.%d')}")
        print(f"최소 텍스트 길이: {self.min_text_length}자\n")
        
        for idx, file_path in enumerate(json_files, 1):
            if idx % 100 == 0:
                print(f"진행 중: {idx}/{total_files} ({idx/total_files*100:.1f}%)")
            
            processed_reviews = self.process_restaurant_file(file_path)
            all_processed_reviews.extend(processed_reviews)
        
        # 결과 저장
        output_file = output_dir / 'preprocessed_reviews.json'
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(all_processed_reviews, f, ensure_ascii=False, indent=2)
        
        # 통계 저장
        stats_output = {
            'total_files_processed': total_files,
            'total_reviews_processed': self.stats['processed'],
            'filtered_no_review_id': self.stats['filtered_no_review_id'],
            'filtered_duplicate': self.stats['filtered_duplicate'],
            'filtered_empty_text': self.stats['filtered_empty_text'],
            'filtered_too_short': self.stats['filtered_too_short'],
            'filtered_too_short_after_cleaning': self.stats['filtered_too_short_after_cleaning'],
            'errors': self.stats['errors'],
            'base_date': DateParser.BASE_DATE.strftime('%Y.%m.%d'),
            'min_text_length': self.min_text_length,
        }
        
        stats_file = output_dir / 'preprocessing_stats.json'
        with open(stats_file, 'w', encoding='utf-8') as f:
            json.dump(stats_output, f, ensure_ascii=False, indent=2)
        
        # 요약 출력
        print("\n" + "="*60)
        print("전처리 완료!")
        print("="*60)
        print(f"✅ 처리된 리뷰: {self.stats['processed']:,}개")
        print(f"❌ 필터링된 리뷰:")
        print(f"   - review_id 없음: {self.stats['filtered_no_review_id']:,}개")
        print(f"   - 중복: {self.stats['filtered_duplicate']:,}개")
        print(f"   - 빈 텍스트: {self.stats['filtered_empty_text']:,}개")
        print(f"   - 너무 짧음 (클리닝 전): {self.stats['filtered_too_short']:,}개")
        print(f"   - 너무 짧음 (클리닝 후): {self.stats['filtered_too_short_after_cleaning']:,}개")
        print(f"⚠️  에러: {self.stats['errors']:,}개")
        print(f"\n📁 결과 파일:")
        print(f"   - {output_file}")
        print(f"   - {stats_file}")
        print("="*60)
        
        return all_processed_reviews


def main():
    """메인 실행 함수"""
    # 경로 설정
    input_dir = Path(r"E:\gitrepo\reivew-embedding\reviews")
    output_dir = Path(r"E:\gitrepo\reivew-embedding\preprocessed")
    
    # 전처리기 초기화
    preprocessor = ReviewPreprocessor(
        min_text_length=20  # 20자 미만 필터링
    )
    
    # 전처리 실행
    reviews = preprocessor.process_all_files(
        input_dir=input_dir,
        output_dir=output_dir
    )
    
    print(f"\n총 {len(reviews):,}개의 리뷰가 전처리되어 저장되었습니다.")


if __name__ == "__main__":
    main()
