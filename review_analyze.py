import pandas as pd
import requests
import json

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, Column, DateTime, String, Integer, ForeignKey
from sqlalchemy.orm import sessionmaker

from datetime import datetime
import pytz

# privateKey.json 파일에서 데이터베이스 설정 불러오기
with open('privateKey.json', 'r') as file:
    config = json.load(file)

# SQLAlchemy 설정
DATABASE_URL = config['DATABASE_URL']
Base = declarative_base()


class Feedback(Base):
    __tablename__ = 'feedback'

    feedback_id = Column(Integer, primary_key=True, autoincrement=True)
    place_id = Column(Integer, ForeignKey('place.place_id'))
    p_summary = Column(String)
    p_body = Column(String)
    n_summary = Column(String)
    n_body = Column(String)
    keyword = Column(String)
    updated_at = Column(DateTime)

# 'places' 테이블을 정의한 예시


class Place(Base):
    __tablename__ = 'place'

    place_id = Column(Integer, primary_key=True, autoincrement=True)


engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
session = SessionLocal()


class CompletionExecutor:

    def review_execute(review_request_data):
        headers = {
            'X-NCP-CLOVASTUDIO-API-KEY': config['X-NCP-CLOVASTUDIO-API-KEY'],
            'X-NCP-APIGW-API-KEY': config['X-NCP-APIGW-API-KEY'],
            'X-NCP-CLOVASTUDIO-REQUEST-ID': config['X-NCP-CLOVASTUDIO-REQUEST-ID-1'],
            'Content-Type': 'application/json; charset=utf-8',
            'Accept': 'text/event-stream'
        }

        # POST 요청을 보냅니다.
        with requests.post(config['CLOVA-HOST'] + '/testapp/v1/chat-completions/HCX-003',
                           headers=headers, json=review_request_data, stream=True) as r:

            flag = True
            for line in r.iter_lines():
                if line and flag:
                    decoded_line = line.decode("utf-8")
                    if decoded_line.startswith('event:result'):
                        flag = False
                        continue

                elif line and not flag:
                    decoded_line = line.decode("utf-8")

                    if decoded_line.startswith('data:'):
                        response_text = decoded_line[5:]
                        print(f"*** review response: {response_text}")
                        flag = True
                        break

        try:
            response_json = json.loads(response_text)

            # 2단계: 내부 content의 문자열을 다시 JSON으로 파싱
            content_str = response_json['message']['content']
            # 이스케이프된 문자 처리
            response = content_str.replace('\\"', '"').replace('\\n', '')
            content_json = json.loads(response)

            return content_json
        except json.JSONDecodeError as e:
            print(f"JSON 변환 오류: {e}")

    def feedback_execute(feedback_request_data):
        headers = {
            'X-NCP-CLOVASTUDIO-API-KEY': config['X-NCP-CLOVASTUDIO-API-KEY'],
            'X-NCP-APIGW-API-KEY': config['X-NCP-APIGW-API-KEY'],
            'X-NCP-CLOVASTUDIO-REQUEST-ID': config['X-NCP-CLOVASTUDIO-REQUEST-ID-2'],
            'Content-Type': 'application/json; charset=utf-8',
            'Accept': 'text/event-stream'
        }
        print("----feedback start------")

        # POST 요청을 보냅니다.
        with requests.post(config['CLOVA-HOST'] + '/testapp/v1/chat-completions/HCX-003',
                           headers=headers, json=feedback_request_data, stream=True) as r:

            flag = True
            for line in r.iter_lines():
                if line and flag:
                    decoded_line = line.decode("utf-8")
                    if decoded_line.startswith('event:result'):
                        flag = False
                        continue

                elif line and not flag:
                    decoded_line = line.decode("utf-8")

                    if decoded_line.startswith('data:'):
                        response_text = decoded_line[5:]
                        print(f"*** feedback response: {response_text}")
                        flag = True
                        break

        try:
            response_json = json.loads(response_text)

            # 2단계: 내부 content의 문자열을 다시 JSON으로 파싱
            content_str = response_json['message']['content']
            # 이스케이프된 문자 처리
            response = content_str.replace('\\"', '"').replace('\\n', '')
            content_json = json.loads(response)

            return content_json
        except json.JSONDecodeError as e:
            print(f"JSON 변환 오류: {e}")


# place_id로 Feedback 데이터를 업데이트하는 함수
def update_feedback(place_id, analysis_result, feedback_result):
    # 1. place_id에 해당하는 기존 데이터 조회
    feedback = session.query(Feedback).filter_by(place_id=place_id).first()

    if feedback:
        # 2. positive, negative, keyword 값 추출
        feedback.p_summary = analysis_result['positive']
        feedback.n_summary = analysis_result['negative']
        feedback.keyword = ", ".join(analysis_result['keyword'])
        feedback.p_body = feedback_result['positive_feedback']
        feedback.n_body = feedback_result['negative_feedback']

        # 한국 시간으로 updated_at 컬럼 업데이트
        korea_timezone = pytz.timezone('Asia/Seoul')
        feedback.updated_at = datetime.now(korea_timezone)

        # 3. 변경 사항 커밋 (데이터베이스에 저장)
        session.commit()
        print(f"place_id {place_id}에 해당하는 데이터가 성공적으로 업데이트되었습니다.")
    else:
        print(f"place_id {place_id}에 해당하는 데이터를 찾을 수 없습니다.")


def run_analyze(place_id):
    print(f"place ID: {place_id}에 대한 리뷰 분석 실행중")

    # CSV 파일 읽기
    file_name = f"files/review_{place_id}.csv"
    try:
        df = pd.read_csv(file_name)

        # 모든 리뷰를 한 문자열로 합치기
        all_reviews = " ".join(df.astype(str).agg(' '.join, axis=1))

        # 프롬프트 텍스트 준비
        review_preset_text = [
            {"role": "system",
                "content": "- 주어진 리뷰를 '긍정'과 '부정' 두 가지로 감정 분류하세요. 긍정 리뷰와 부정 리뷰 각각을 3문장으로 요약하세요. 마지막으로 전체 리뷰를 분석하여 키워드 5가지를 추출합니다.\r\n- 감정은 리뷰의 분위기/톤을 분석하여 긍정과 부정을 적절하게 구분하고, 감정을 기준으로 그룹화하세요.\r\n- 그룹화된 리뷰를 각각 3문장으로 요약하세요.\r\n- 전체 리뷰를 분석하여 빈출된 순으로 유의미한 키워드 5가지를 추출합니다. \r\n- 분석 결과는 {'긍정': '긍정 리뷰 3문장 요약', '부정': '부정 리뷰 3문장 요약', '키워드': '키워드 5가지'} 형식으로 제시하세요.\r\n\r\n### \r\n예시:\r\n\r\n미국 가보지는 안았지만 미국버거 맛프랜차이즈와 다르게 맛있어요맛집없는 안양에서 몇 안되는 맛집인듯\r\n\r\n줄서서 먹을정도 까지는 아닌듯\r\n\r\n분위기는 예쁜데 맛은 보통 가격 저렴하지 않음분위기도 중요하지만 일단 식당은 먹으러 가는 곳이니맛도 중요하니까 다시 갈것 같진 않아요\r\n\r\n처음 생겼을때 부터 쭉 이용했는데 이 곳은 정말 지켜주고 싶은 가게에요 성수동에 제가 좋아했던 로컬 식당들 죄다 요상꾸리한거 들어오면서 사라져서 슬펐는데 이 곳은 혼자만 알고 싶은 맛집이었지만 부디 오래오래 이자리에 남아주십사하는 마음에 후기 남겨요 항상 기분 좋게 맞이해주는 사장님 진짜 처음 부터 지금까지 한결 같아요 한번쯤은 지친 모습 보일법도 한데 여길 수차례 왔지만 한번도 못봤어요맛성비 최고인 은준쌀국수 영원하라 포에버\r\n\r\n\r\n{\r\n  \"positive\": \"이 식당은 맛이 좋고, 기분 좋게 맞아주는 사장님이 인상적입니다. 처음부터 지금까지 지속적으로 이용하며, 맛과 가격 모두 만족스럽습니다. 혼자 알고 싶은 맛집으로 오랫동안 기억되기를 바랍니다.\",\r\n  \"negative\": \"미국 버거 프랜차이즈와 비교할 때 맛은 보통이고, 가격이 저렴하지 않습니다. 줄 서서 먹을 정도는 아니며, 다시 갈 것 같지 않습니다. 분위기는 예쁘지만 식당에서 가장 중요한 맛이 부족하다는 느낌입니다.\",\r\n  \"keyword\": [\"맛집\", \"가격\", \"사장님\", \"분위기\", \"맛\"]\r\n}\r"},
            {"role": "user", "content": all_reviews}
        ]

        review_request_data = {
            'messages': review_preset_text,
            'topP': 0.8,
            'topK': 0,
            'maxTokens': 256,
            'temperature': 0.1,
            'repeatPenalty': 1.2,
            'stopBefore': [],
            'includeAiFilters': True,
            'seed': 0
        }

        # 리뷰 분석 AI 모델에 요청 실행
        analysis_result = CompletionExecutor.review_execute(
            review_request_data)

        # 분석 결과 출력
        if analysis_result:
            print(f"\n---{place_id} review 분석 결과:")
            print(json.dumps(analysis_result, indent=4, ensure_ascii=False))

            # 여기에 feedback AI
            feedback_preset_text = [{"role": "system", "content": "- 주어진 데이터를 기반으로 매장에 도움이 될만한 피드백을 제공하세요.\n- positive는 긍정 리뷰이며, negative는 부정 리뷰 요약입니다.\n- 각각의 리뷰에 대해 각각 피드백을 제공합니다.\n- 출력은 꼭 아래의 예시와 같은 형식으로 중괄호 안에 \"positive_feedback\"과 \"negative_feedback\"이 있어야 합니다.\r\n\r\n### \r\n예시:\r\n\r\n\"positive\": \"맛있는 닭한마리 요리를 즐길 수 있으며, 소스와 야채의 조합이 좋다. 맑은 국물과 칼국수, 죽 등의 추가 메뉴도 만족스럽다. 주차장은 복잡하지만 대중교통을 이용하면 편리하다.\",\n\"negative\": \"일부 직원의 불친절한 서비스와 개념 없는 행동, 그리고 음식의 양이나 리필 등에 대한 야박한 인심이 아쉽다. 또한, 주차 문제와 대기 시간이 길다는 점도 불편하다.\"\n\n\n{\"positive_feedback\": \"만족도가 높은 메뉴 조합과 다양한 추가 메뉴를 강조하여 홍보하세요. 특히 소스와 야채 조합의 특성을 살린 특별한 메뉴 소개를 통해 고객의 관심을 끌 수 있습니다. 복잡한 주차 문제를 해결하기 위해, 대중교통을 이용한 방문 방법을 SNS와 매장 내 안내문으로 쉽게 전달하세요. 예를 들어, 가장 가까운 버스나 지하철 역 정보를 제공하면 고객의 방문이 더 수월해질 것입니다.\",\n\"negative_feedback\":\"직원들의 서비스 태도 개선과 일관된 야채 리필 정책을 마련하여 고객의 불편을 최소화하세요. 주차장 관리를 강화하고, 주차 공간 부족 문제를 해결하기 위해 주차 안내판을 설치하거나, 주차 가능 시간을 제한하는 등의 조치를 취할 수 있습니다. 이러한 문제를 해결하면 고객 만족도를 높일 수 있으며, 매장의 이미지 개선에도 도움이 됩니다.\"}"},
                                    {"role": "user", "content": f"\"positive\": \"{analysis_result['positive']}\", \"negative\": \"{analysis_result['negative']}\""}]

            feedback_request_data = {
                'messages': feedback_preset_text,
                'topP': 0.8,
                'topK': 0,
                'maxTokens': 256,
                'temperature': 0.1,
                'repeatPenalty': 1.2,
                'stopBefore': [],
                'includeAiFilters': True,
                'seed': 0
            }

            # 리뷰 분석 AI 모델에 요청 실행
            feedback_result = CompletionExecutor.feedback_execute(
                feedback_request_data)
            print(f"\n---{place_id} feedback 분석 결과:")
            print(json.dumps(feedback_result, indent=4, ensure_ascii=False))

            if feedback_result:
                update_feedback(place_id, analysis_result, feedback_result)

        else:
            print("분석 결과를 받아오지 못했습니다.")

    except FileNotFoundError:
        print(f"{file_name} 파일을 찾을 수 없습니다.")
        return None
