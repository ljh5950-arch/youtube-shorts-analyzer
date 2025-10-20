# app.py  — YouTube Shorts Analyzer (완성본 v1)
from fastapi import FastAPI, Query, Body, HTTPException
from typing import List, Dict, Any
from datetime import datetime, timedelta
import os, json
import isodate

from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials

app = FastAPI(title="YouTube Shorts Analyzer (MVP)")

# =========================
# 환경변수
# =========================
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")  # 필수
SHEETS_PARENT_SPREADSHEET_ID = os.getenv("SHEETS_PARENT_SPREADSHEET_ID")  # 선택(시트 업로드용)
GOOGLE_SA_JSON = os.getenv("GOOGLE_SA_JSON")  # 선택(시트 업로드용)

# =========================
# 외부 서비스 핸들러
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

# 유튜브 search.list 허용 order 값 매핑(사람이 쓰기 쉬운 별칭 → API 값)
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
# 기본 화면/헬스체크
# =========================
@app.get("/")
def root():
    return {
        "service": "YouTube Shorts Analyzer",
        "docs": "/docs",
        "endpoints": ["/api/search_shorts", "/api/export/sheets", "/health"],
    }

@app.get("/health")
def health():
    return {"ok": True, "time": datetime.utcnow().isoformat()}

# =========================
# 1) 검색 API (쇼츠 중심)
# =========================
@app.get("/api/search_shorts")
def search_shorts(
    q: str = Query(..., description="검색 키워드"),
    max_results: int = Query(100, ge=1, le=200),
    days: int = Query(90, ge=1, le=180),
    order: str = Query("views", description="views(viewCount), date, relevance, rating, title, videoCount"),
    shorts_only: bool = Query(True, description="쇼츠만 보기(길이 ≤ 60초)")
):
    yt = get_youtube()
    published_after = (datetime.utcnow() - timedelta(days=days)).isoformat("T") + "Z"
    order_api = ORDER_MAP.get(order, "viewCount")

    # 1) 검색으로 videoId 모으기 (페이지네이션)
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

    # 2) 영상 상세 조회
    videos: List[Dict[str, Any]] = []
    for i in range(0, len(video_ids), 50):
        chunk = ",".join(video_ids[i:i+50])
        vresp = yt.videos().list(
            id=chunk, part="snippet,contentDetails,statistics"
        ).execute()

        for v in vresp.get("items", []):
            duration = isodate.parse_duration(v["contentDetails"]["duration"]).total_seconds()
            if shorts_only and duration > 60:
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
                "durationSec": int(duration),
                "watchUrl": f"https://www.youtube.com/watch?v={v['id']}",
            })

    if not videos:
        return {"keyword": q, "count": 0, "videos": []}

    # 3) 채널 구독자 수 조회 후 병합
    channel_ids = sorted({v["channelId"] for v in videos})
    subs_map: Dict[str, Any] = {}
    for i in range(0, len(channel_ids), 50):
        chunk = ",".join(channel_ids[i:i+50])
        cresp = yt.channels().list(id=chunk, part="statistics").execute()
        for c in cresp.get("items", []):
            stats = c.get("statistics", {})
            if stats.get("hiddenSubscriberCount"):
                subs_map[c["id"]] = None
            else:
                subs_map[c["id"]] = int(stats.get("subscriberCount", 0))

    # 4) 계산 필드 추가(구독자 대비 조회/좋아요)
    for v in videos:
        subc = subs_map.get(v["channelId"])
        v["subscriberCount"] = subc
        if subc and subc > 0:
            v["viewsPerSub"] = round(v["viewCount"] / subc, 4)
            v["likesPerSub"] = round(v["likeCount"] / subc, 4)
        else:
            v["viewsPerSub"] = None
            v["likesPerSub"] = None

    # 5) 기본 정렬(조회수 내림차순)
    videos.sort(key=lambda x: x["viewCount"], reverse=True)

    return {"keyword": q, "count": len(videos), "videos": videos}

# =========================
# 2) Google Sheets 업로드
# =========================
@app.post("/api/export/sheets")
def export_to_sheets(payload: Dict[str, Any] = Body(...)):
    """
    payload 예시:
    {
      "keyword": "반려동물",               # 기본 시트명 생성에 사용(옵션)
      "sheetName": null,                   # 직접 지정도 가능(옵션)
      "rows": [ {...}, {...} ]             # /api/search_shorts 의 videos 배열을 넣으면 됨
    }
    """
    if not (GOOGLE_SA_JSON and SHEETS_PARENT_SPREADSHEET_ID):
        raise HTTPException(status_code=500, detail="시트 업로드용 환경변수(GOOGLE_SA_JSON, SHEETS_PARENT_SPREADSHEET_ID)가 필요합니다.")

    rows: List[Dict[str, Any]] = payload.get("rows") or []
    if not rows:
        raise HTTPException(status_code=400, detail="rows 가 비어있습니다.")

    keyword: str = payload.get("keyword") or "검색결과"
    sheet_name: str = payload.get("sheetName") or f"{keyword}_{datetime.utcnow().strftime('%Y%m%d')}"

    # 표 헤더(고정 순서)
    headers = [
        "channelTitle", "videoTitle", "publishedAt",
        "subscriberCount", "viewCount", "viewsPerSub",
        "likeCount", "likesPerSub", "commentCount",
        "watchUrl", "videoId", "durationSec", "channelId"
    ]

    # rows 를 표로 변환
    values = [headers]
    for r in rows:
        row = []
        for h in headers:
            val = r.get(h, "")
            if isinstance(val, float):
                val = round(val, 6)
            row.append(str(val))
        values.append(row)

    # 시트 API 호출
    sheets = get_sheets_service()

    # 시트 탭 생성(이미 있으면 무시)
    try:
        sheets.spreadsheets().batchUpdate(
            spreadsheetId=SHEETS_PARENT_SPREADSHEET_ID,
            body={"requests": [{"addSheet": {"properties": {"title": sheet_name}}}]}
        ).execute()
    except Exception:
        pass

    # 값 기록
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
