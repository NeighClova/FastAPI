import pandas as pd
import json
import os
from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String, select
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from fastapi.responses import JSONResponse
from tempfile import NamedTemporaryFile

# instagram upload
from instagrapi import Client

import concurrent.futures
import importlib.util
import subprocess
import getPlaceUrl


# 모듈 로드
spec_review = importlib.util.spec_from_file_location(
    "naver_review", "naver_review.py")
naver_review = importlib.util.module_from_spec(spec_review)
spec_review.loader.exec_module(naver_review)

# privateKey.json 파일에서 데이터베이스 설정 불러오기
with open('privateKey.json', 'r') as file:
    config = json.load(file)

# SQLAlchemy 설정
DATABASE_URL = config['DATABASE_URL']
Base = declarative_base()


class Place(Base):
    __tablename__ = 'place'
    place_id = Column(Integer, primary_key=True, index=True)
    place_num = Column(String)


engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# FastAPI 및 스케줄러 설정
app = FastAPI()
scheduler = AsyncIOScheduler()


def do_thread_crawl_and_analyze(place_pairs: list):
    thread_list = []
    with ThreadPoolExecutor(max_workers=8) as executor:
        for place_id, place_num in place_pairs:
            thread_list.append(executor.submit(
                naver_review.run_crawler, place_id, place_num))
        for execution in concurrent.futures.as_completed(thread_list):
            execution.result()


def do_process_crawl(place_pairs: list):
    chunk_size = 4
    chunks = [place_pairs[i:i + chunk_size]
              for i in range(0, len(place_pairs), chunk_size)]

    process_list = []
    with ProcessPoolExecutor(max_workers=chunk_size) as executor:
        for chunk in chunks:
            process_list.append(executor.submit(
                do_thread_crawl_and_analyze, chunk))
        for execution in concurrent.futures.as_completed(process_list):
            execution.result()


def schedule_tasks():
    session = SessionLocal()
    # Place 모델에서 place_id와 place_num을 함께 가져옵니다.
    places = session.execute(select(Place.place_id, Place.place_num)).all()
    session.close()

    # 요일별로 회원 ID 그룹핑
    day_places = {i: [] for i in range(7)}
    for place_id, place_num in places:
        day_of_week = place_id % 7
        day_places[day_of_week].append((place_id, place_num))

    day_map = ['sun', 'mon', 'tue', 'wed', 'thu', 'fri', 'sat']
    for day, place_pairs in day_places.items():
        cron_expr = f"00 03 * * {day_map[day]}"
        scheduler.add_job(
            do_process_crawl,
            CronTrigger.from_crontab(cron_expr),
            id=f"day_{day}",
            args=[place_pairs]
        )


@app.on_event("startup")
async def startup_event():
    schedule_tasks()
    scheduler.start()


@app.on_event("shutdown")
async def shutdown_event():
    scheduler.shutdown()


@app.get("/")
async def read_root():
    return {"message": "스케줄러가 실행 중입니다"}

# place ID 추출


@app.post("/")
async def get_placeNum(request: Request):
    try:
        body = await request.json()
        place_url = body.get("place_url")
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    place_num = getPlaceUrl.getUrl(place_url)

    return JSONResponse(content={"code": "SU", "message": "Success", "place_num": place_num}, status_code=200)

# 인스타그램 자동 업로더


@app.post("/instagram/upload")
async def upload_instagram(
    instagramId: str = Form(...),
    instagramPw: str = Form(...),
    content: str = Form(...),
    file: UploadFile = File(...)
):
    try:
        # Save the uploaded file to a temporary file
        with NamedTemporaryFile(delete=False, suffix=".jpg") as temp_file:
            temp_file.write(await file.read())
            temp_file_path = temp_file.name

        # Instagram login
        cl = Client()
        cl.login(instagramId, instagramPw)

        # Upload the photo using the file path
        cl.photo_upload(temp_file_path, content)

        # Clean up: Delete the temporary file
        os.remove(temp_file_path)

        return JSONResponse(content={"code": "SU", "message": "Success"}, status_code=200)

    except Exception as e:
        return JSONResponse(content={"code": "ER", "message": str(e)}, status_code=400)

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host='0.0.0.0', port=8000)
