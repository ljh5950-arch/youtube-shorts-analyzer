from fastapi import FastAPI, Query
from googleapiclient.discovery import build
import isodate
from datetime import datetime, timedelta
import os

app = FastAPI(title="YouTube Shorts Analyzer (MVP)")

# ====== 환경변수 불러오기 ======
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")

# ====== 유튜브 API 설정 ======
youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)


@app.get("/api/search_shorts")
def search_shorts(
    q: str = Query(..., description="검색 키워드"),
    max_results: int = Query(50, ge=1, le=200),
    days: int = Query(90, ge=1, le=180),
    order: str = Query("views"),
    shorts_only: bool = Query(True, description="쇼츠만 보기 (60초 이하)")
):
    """유튜브 쇼츠 검색 API"""
    published_after = (datetime.utcnow() - timedelta(days=days)).isoformat("T") + "Z"
    all_videos = []
    next_page_token = None

    while len(all_videos) < max_results:
        search_response = youtube.search().list(
            q=q,
            part="id",
            type="video",
            order=order,
            publishedAfter=published_after,
            maxResults=min(50, max_results - len(all_videos)),
            pageToken=next_page_token
        ).execute()

        video_ids = [item["id"]["videoId"] for item in search_response["items"]]
        next_page_token = search_response.get("nextPageToken")

        if not video_ids:
            break

        video_response = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=",".join(video_ids)
        ).execute()

        for video in video_response["items"]:
            duration = isodate.parse_duration(video["contentDetails"]["duration"]).total_seconds()
            if shorts_only and duration > 60:
                continue
            info = {
                "videoId": video["id"],
                "videoTitle": video["snippet"]["title"],
                "channelTitle": video["snippet"]["channelTitle"],
                "publishedAt": video["snippet"]["publishedAt"],
                "viewCount": int(video["statistics"].get("viewCount", 0)),
                "likeCount": int(video["statistics"].get("likeCount", 0)),
                "commentCount": int(video["statistics"].get("commentCount", 0)),
                "durationSec": int(duration),
                "watchUrl": f"https://www.youtube.com/watch?v={video['id']}"
            }
            all_videos.append(info)

        if not next_page_token:
            break

    return {"keyword": q, "count": len(all_videos), "videos": all_videos}


@app.get("/health")
def health():
    return {"ok": True, "time": datetime.utcnow().isoformat()}
