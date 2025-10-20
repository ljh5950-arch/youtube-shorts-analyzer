# app.py — YouTube Shorts Analyzer (v1.3, 쇼츠 길이 가변)
from fastapi import FastAPI, Query, Body, HTTPException
from typing import List, Dict, Any, Union
from datetime import datetime, timedelta
import os, json, isodate

from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials

app = FastAPI(title="YouTube Shorts Analyzer (MVP)")

# =========================
# 환경변수
# =========================
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
SHEETS_PARENT_SPREADSHEET_ID = os.getenv("SHEETS_PARENT_SPREADSHEET_ID")
GOOGLE_SA_JSON = os.getenv("GOOGLE_SA_JSON")

# =========================
# 외부 서비스 연결 함수
# =========================
def get_youtube():
    if not YOUTUBE_API_KEY:
        raise HTTPException(status_code=500, detail="YOUTUBE_API_KEY 환경변수가 없습니다.")
    return build("youtube", "v3", developerKey=YOUTUBE_API_KEY)

def get_sheets_service():
    if not (GOOGLE_SA_JSON and SHEETS_PARENT_SPREADSHEET_ID):
        raise HTTPException(status_code=500, detail="시트 업로드용 환경변수(GOOGLE_SA_JSON, SHEETS_PARENT_SPREADSHEET_ID)가 설정되지 않았습니다.")
    sa_info = json.loads(GOOGLE_SA_JSON)
    creds = Credentials.from_service_account_info(
        sa_info, scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    return build("sheets", "v4", credentials=creds)

# =========================
# 유튜브 검색 정렬값 매핑
# =========================
ORDER_MAP = {
    "views": "viewCount",
    "viewCount": "viewCount",
    "date": "date",
    "relevance": "relevance",
    "rating": "rating",
    "title": "title",
    "videoCount": "videoCount",
}

# =========================
# 기본 경로 / 헬스체크
# =========================
@app.get("/")
def root():
    return {
        "service": "YouTube Shorts Analyzer",
        "endpoints": ["/api/search_shorts", "/api/export/sheets", "/health"]
    }

@app.get("/health")
def health():
    return {"ok": True, "time": datetime.utcnow().isoformat()}

# =========================
# 1️⃣ 유튜브 쇼츠 검색 API
# =========================
@app.get("/api/search_shorts")
def search_shorts(
    q: str = Query(..., description="검색 키워드"),
    max_results: int = Query(100, ge=1, le=200),
    days: int = Query(90, ge=1, le=180),
    order: str = Query("views"),
    shorts_only: bool = Query(True, description="쇼츠만 보기(길이 제한 적용)"),
    max_duration_sec: int = Query(180, ge=1, le=600, description="쇼츠로 인정할 최대 길이(초), 기본 180")
):
    """
    쇼츠 판별 로직:
    - shorts_only = True 일 때, durationSec <= max_duration_sec 인 영상만 반환
    - 기본값 180초(3분)
    """
    yt = get_youtube()
    published_after = (datetime.utcnow() - timedelta(days=days)).isoformat("T") + "Z"
    order_api = ORDER_MAP.get(order, "viewCount")

    # 1) 검색으로 videoId 모으기
    video_ids: List[str] = []
    next_page_token = None
    while len(video_ids) < max_results:
        resp = yt.search().list(
            q=q, part="id", type="video",
            order=order_api,
            publishedAfter=published_after,
            maxResults=min(50, max_results - len(video_ids)),
            pageToken=next_page_token
        ).execute()

        ids = [it["id"]["videoId"] for it in resp.get("items", [])]
        video_ids += ids
        next_page_token = resp.get("nextPageToken")
        if not next_page_token:
            break

    if not video_ids:
        return {"keyword": q, "count": 0, "videos": []}

    # 2) 세부 정보 조회 + 길이 필터
    videos: List[Dict[str, Any]] = []
    for i in range(0, len(video_ids), 50):
        chunk = ",".join(video_ids[i:i+50])
        vresp = yt.videos().list(id=chunk, part="snippet,contentDetails,statistics").execute()

        for v in vresp.get("items", []):
            dur = isodate.parse_duration(v["contentDetails"]["duration"]).total_seconds()
            if shorts_only and dur > max_duration_sec:
                continue
            videos.append({
                "videoId": v["id"],
                "videoTitle": v["snippet"]["title"],
                "channelId": v["snippet"]["channelId"],
                "channelTitle": v["snippet"]["channelTitle"],
                "publishedAt": v["snippet"]["publishedAt"],
                "viewCount": int(v["statistics"].get("viewCount", 0)),
                "likeCount": int(v["statistics"].get("likeCount", 0)),
                "commentCount": int(v["statistics"].get("commentCount", 0)),
                "durationSec": int(dur),
                "watchUrl": f"https://www.youtube.com/watch?v={v['id']}"
            })

    if not videos:
        return {"keyword": q, "count": 0, "videos": []}

    # 3) 채널 구독자 수 조회
    ch_ids = sorted({v["channelId"] for v in videos})
    subs_map: Dict[str, Any] = {}
    for i in range(0, len(ch_ids), 50):
        chunk = ",".join(ch_ids[i:i+50])
        cresp = yt.channels().list(id=chunk, part="statistics").execute()
        for c in cresp.get("items", []):
            s = c.get("statistics", {})
            subs_map[c["id"]] = None if s.get("hiddenSubscriberCount") else int(s.get("subscriberCount", 0))

    # 4) 계산 필드 추가
    for v in videos:
        sub = subs_map.get(v["channelId"])
        v["subscriberCount"] = sub
        if sub and sub > 0:
            v["viewsPerSub"] = round(v["viewCount"] / sub, 4)
            v["likesPerSub"] = round(v["likeCount"] / sub, 4)
        else:
            v["viewsPerSub"] = None
            v["likesPerSub"] = None

    # 5) 기본 정렬(조회수 내림차순)
    videos.sort(key=lambda x: x["viewCount"], reverse=True)
    return {
        "keyword": q,
        "count": len(videos),
        "shorts_only": shorts_only,
        "max_duration_sec": max_duration_sec,
        "videos": videos
    }

# =========================
# 2️⃣ Google Sheets 업로드 (rows / videos / 배열 모두 지원)
# =========================
@app.post("/api/export/sheets")
def export_to_sheets(payload: Union[Dict[str, Any], List[Dict[str, Any]]] = Body(...)):
    # 1) 배열로만 온 경우
    if isinstance(payload, list):
        rows = payload
        keyword = "검색결과"
        sheet_name = f"{keyword}_{datetime.utcnow().strftime('%Y%m%d')}"
    else:
        # 2) rows 또는 videos 둘 다 허용
        rows = payload.get("rows") or payload.get("videos") or []
        keyword = payload.get("keyword") or "검색결과"
        sheet_name = payload.get("sheetName") or f"{keyword}_{datetime.utcnow().strftime('%Y%m%d')}"

    if not rows:
        raise HTTPException(status_code=400, detail="rows 가 비어있습니다.")

    if not (GOOGLE_SA_JSON and SHEETS_PARENT_SPREADSHEET_ID):
        raise HTTPException(status_code=500, detail="시트 업로드용 환경변수(GOOGLE_SA_JSON, SHEETS_PARENT_SPREADSHEET_ID)가 필요합니다.")

    sheets = get_sheets_service()

    headers = [
        "channelTitle", "videoTitle", "publishedAt",
        "subscriberCount", "viewCount", "viewsPerSub",
        "likeCount", "likesPerSub", "commentCount",
        "watchUrl", "videoId", "durationSec", "channelId"
    ]

    values = [headers]
    for r in rows:
        row = []
        for h in headers:
            val = r.get(h, "")
            if isinstance(val, float):
                val = round(val, 6)
            row.append(str(val))
        values.append(row)

    # 시트 탭 만들기 (중복 시 무시)
    try:
        sheets.spreadsheets().batchUpdate(
            spreadsheetId=SHEETS_PARENT_SPREADSHEET_ID,
            body={"requests": [{"addSheet": {"properties": {"title": sheet_name}}}]}
        ).execute()
    except Exception:
        pass

    sheets.spreadsheets().values().update(
        spreadsheetId=SHEETS_PARENT_SPREADSHEET_ID,
        range=f"{sheet_name}!A1",
        valueInputOption="RAW",
        body={"values": values}
    ).execute()

    return {
        "message": "업로드 완료",
        "sheet_url": f"https://docs.google.com/spreadsheets/d/{SHEETS_PARENT_SPREADSHEET_ID}",
        "sheet_name": sheet_name,
        "rows": len(rows)
    }
