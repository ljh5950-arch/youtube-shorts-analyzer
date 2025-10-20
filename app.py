from fastapi import FastAPI, Query
from googleapiclient.discovery import build
import isodate
from typing import List
from datetime import datetime, timedelta

app = FastAPI(title="YouTube Shorts Analyzer")

# =====================
#  환경변수 세팅 (Render에서 추가해둔 값 사용)
# =====================
import os
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")

# =====================
#  유튜브 API 기본 설정
# =====================
YOUTUBE = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)


@app.get("/api/search_shorts")
def search_shorts(
    q: str = Query(..., description="검색할 키워드"),
    max_results: int = Query(100, ge=1, le=200),
    days: int = Query(90, ge=1, le=180),
    order: str = Query("views", description="정렬 기준: views, date 등"),
    shorts_only: bool = Query(True, description="쇼츠만 보기"),
):
    """키워드로 유튜브 쇼츠 영상 리스트 검색"""
    published_after = (datetime.utcnow() - timedelta(days=days)).isoformat("T") + "Z"
    video_ids = []
    next_page = None

    while len(video_ids) < max_results:
        req = YOUTUBE.search().list(
            q=q,
            part="id",
            type="video",
            order="date",
            publishedAfter=published_after,
            maxResults=min(50, max_results - len(video_ids)),
            pageToken=next_page,
        )
        resp = req.execute()
        ids = [i["id"]["videoId"] for i in resp.get("items", [])]
        video_ids += ids
        next_page = resp.get("nextPageToken")
        if not next_page:
            break

    if not video_ids:
        return {"message": "결과가 없습니다."}

    video_data = []
    for i in range(0, len(video_ids), 50):
        ids = ",".join(video_ids[i:i+50])
        details = YOUTUBE.videos().list(
            part="snippet,contentDetails,statistics",
            id=ids
        ).execute()
        for v in details.get("items", []):
            duration = isodate.parse_duration(v["contentDetails"]["duration"]).total_seconds()
            if shorts_only and duration > 60:
                continue
            vid = {
                "videoId": v["id"],
                "videoTitle": v["snippet"]["title"],
                "channelId": v["snippet"]["channelId"],
                "publishedAt": v["snippet"]["publishedAt"],
                "viewCount": int(v["statistics"].get("viewCount", 0)),
                "likeCount": int(v["statistics"].get("likeCount", 0)),
                "commentCount": int(v["statistics"].get("commentCount", 0)),
                "durationSec": int(duration),
                "watchUrl": f"https://www.youtube.com/watch?v={v['id']}"
            }
            video_data.append(vid)

    return {"keyword": q, "count": len(video_data), "videos": video_data}


@app.get("/health")
def health():
    return {"ok": True, "time": datetime.utcnow().isoformat()}
