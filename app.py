import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, Query, HTTPException, Body, Header
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials

# =========================
# 환경 변수
# =========================

YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
GOOGLE_SHEETS_ID = os.getenv("GOOGLE_SHEETS_ID")
QUICK_WEBHOOK_TOKEN = os.getenv("QUICK_WEBHOOK_TOKEN", "")

if not YOUTUBE_API_KEY:
    raise RuntimeError("YOUTUBE_API_KEY 환경변수가 설정되어 있지 않습니다.")
if not GOOGLE_SERVICE_ACCOUNT_JSON:
    raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON 환경변수가 설정되어 있지 않습니다.")
if not GOOGLE_SHEETS_ID:
    raise RuntimeError("GOOGLE_SHEETS_ID 환경변수가 설정되어 있지 않습니다.")

# =========================
# FastAPI 초기화
# =========================

app = FastAPI(
    title="YouTube Shorts Analyzer",
    description="YouTube 쇼츠 데이터 수집 및 Sheets 내보내기 도구",
    version="2.1-region"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_credentials=True,
    allow_headers=["*"],
)

# =========================
# 공용 유틸 함수
# =========================

def get_youtube():
    return build("youtube", "v3", developerKey=YOUTUBE_API_KEY)


def get_sheets():
    creds = Credentials.from_service_account_info(
        eval(GOOGLE_SERVICE_ACCOUNT_JSON)  # 문자열로 들어온 JSON을 dict로 변환 사용 (주의: eval은 내부용 전제)
    )
    return build("sheets", "v4", credentials=creds)


def to_yyyy_mm_dd(ts: str) -> str:
    """RFC3339 / ISO8601 문자열을 YYYY-MM-DD 로 축약."""
    try:
        return ts[:10]
    except Exception:
        return ts


def parse_iso_duration(duration: str) -> int:
    """
    ISO8601 Duration(예: PT1M23S, PT2M, PT45S)을 초 단위(int)로 변환.
    """
    if not duration.startswith("PT"):
        return 0
    duration = duration[2:]

    total_seconds = 0
    num = ""
    for ch in duration:
        if ch.isdigit():
            num += ch
        else:
            if not num:
                continue
            value = int(num)
            num = ""
            if ch == "H":
                total_seconds += value * 3600
            elif ch == "M":
                total_seconds += value * 60
            elif ch == "S":
                total_seconds += value
    return total_seconds


def viral_score(row: Dict[str, Any]) -> float:
    """
    바이럴 점수 = viewsPerSub * 0.6 + likesPerSub * 400
    viewsPerSub = views / subscribers
    likesPerSub = likes / subscribers
    """
    views = row.get("views", 0) or 0
    likes = row.get("likes", 0) or 0
    subs = row.get("subscribers", 0) or 0
    if subs <= 0:
        subs = 1
    views_per_sub = views / subs
    likes_per_sub = likes / subs
    score = views_per_sub * 0.6 + likes_per_sub * 400
    return float(score)


def normalize_region(region: Optional[str]) -> Optional[str]:
    """
    지역 문자열을 정규화.
    - GLOBAL, ALL, WORLD -> None (전세계)
    - KR, TW, JP 등 2글자 -> 그대로
    - 그 외 값 -> None
    """
    if not region:
        return None
    r = region.upper().strip()
    if r in {"GLOBAL", "ALL", "WORLD"}:
        return None
    if len(r) == 2:
        return r
    return None


def ensure_sheet_and_get_range_title(svc, spreadsheet_id: str, title: str) -> str:
    """
    주어진 title의 시트가 없으면 생성하고, 있으면 그대로 사용.
    반환값 예: '키워드_YYYYMMDD'
    """
    sheets_api = svc.spreadsheets()

    meta = sheets_api.get(spreadsheetId=spreadsheet_id).execute()
    existing_sheets = meta.get("sheets", [])
    existing_titles = [s["properties"]["title"] for s in existing_sheets]

    if title not in existing_titles:
        add_req = {
            "requests": [
                {
                    "addSheet": {
                        "properties": {
                            "title": title
                        }
                    }
                }
            ]
        }
        sheets_api.batchUpdate(
            spreadsheetId=spreadsheet_id,
            body=add_req
        ).execute()

    return title


def write_sheet(
    rows: List[List[Any]],
    keyword: str,
    region: Optional[str]
) -> str:
    """
    rows 데이터를 Google Sheets에 기록.
    시트 이름: f"{키워드}_{YYYYMMDD}" (region이 있으면 접두사 추가).
    """
    svc = get_sheets()
    today = datetime.utcnow().strftime("%Y%m%d")

    title = f"{keyword}_{today}"
    if region:
        # 예: KR_프로미스나인_20251025
        title = f"{region}_{title}"

    sheet_title = ensure_sheet_and_get_range_title(svc, GOOGLE_SHEETS_ID, title)
    range_name = f"{sheet_title}!A1"

    body = {
        "range": range_name,
        "majorDimension": "ROWS",
        "values": rows
    }

    svc.spreadsheets().values().clear(
        spreadsheetId=GOOGLE_SHEETS_ID,
        range=range_name
    ).execute()

    svc.spreadsheets().values().update(
        spreadsheetId=GOOGLE_SHEETS_ID,
        range=range_name,
        valueInputOption="RAW",
        body=body
    ).execute()

    # 시트 URL
    sheet_url = f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEETS_ID}/edit#gid=0"
    return sheet_url


# =========================
# 모델 정의
# =========================

class QuickWebhookPayload(BaseModel):
    cmd: Optional[str] = None
    q: Optional[str] = None
    n: Optional[int] = None
    days: Optional[int] = None
    duration: Optional[int] = None
    region: Optional[str] = None


# =========================
# 루트
# =========================

@app.get("/")
def root():
    return {
        "status": "ok",
        "version": "2.1-region",
        "message": "YouTube Shorts Analyzer"
    }


# =========================
# 메인 검색 + 시트 내보내기
# =========================

ORDER_MAP = {
    "views": "viewCount",
    "new": "date",
    "relevance": "relevance"
}


@app.get("/api/search_shorts")
def search_and_export(
    q: str = Query(..., description="검색 키워드"),
    max_results: int = Query(100, ge=1, le=200),
    days: int = Query(90, ge=1, le=180),
    order: str = Query("views"),
    shorts_only: bool = Query(True, description="쇼츠만 보기(길이 제한 적용)"),
    max_duration_sec: int = Query(180, ge=1, le=600, description="쇼츠 최대 길이(초)"),
    auto_sheet: bool = Query(True, description="True면 결과를 바로 Google Sheets로 내보냄"),
    region: str = Query("GLOBAL", description="지역코드: GLOBAL, KR, TW, JP 등")
):
    """
    유튜브 영상 검색 후, 쇼츠 데이터 수집 + (옵션) 시트로 내보내기.
    """
    yt = get_youtube()
    published_after = (datetime.utcnow() - timedelta(days=days)).isoformat("T") + "Z"
    order_api = ORDER_MAP.get(order, "viewCount")
    region_code = normalize_region(region)

    # 1) 검색으로 videoId 모으기
    video_ids: List[str] = []
    next_page_token = None

    while len(video_ids) < max_results:
        search_params: Dict[str, Any] = {
            "q": q,
            "part": "id",
            "type": "video",
            "order": order_api,
            "publishedAfter": published_after,
            "maxResults": min(50, max_results - len(video_ids)),
            "pageToken": next_page_token
        }
        if region_code:
            search_params["regionCode"] = region_code

        resp = yt.search().list(**search_params).execute()
        ids = [item["id"]["videoId"] for item in resp.get("items", [])]
        video_ids.extend(ids)
        next_page_token = resp.get("nextPageToken")
        if not next_page_token:
            break

    video_ids = list(dict.fromkeys(video_ids))  # 중복 제거

    if not video_ids:
        return {
            "keyword": q,
            "region": region_code or "GLOBAL",
            "count": 0,
            "videos": [],
            "sheet_url": None
        }

    # 2) video 상세 정보 조회
    videos: List[Dict[str, Any]] = []
    for i in range(0, len(video_ids), 50):
        batch_ids = video_ids[i:i+50]
        v_resp = yt.videos().list(
            part="snippet,statistics,contentDetails",
            id=",".join(batch_ids)
        ).execute()

        for item in v_resp.get("items", []):
            vid = item["id"]
            snippet = item.get("snippet", {})
            stats = item.get("statistics", {})
            content = item.get("contentDetails", {})

            duration_sec = parse_iso_duration(content.get("duration", "PT0S"))

            if shorts_only and duration_sec > max_duration_sec:
                continue

            channel_title = snippet.get("channelTitle", "")
            title = snippet.get("title", "")
            published_at = snippet.get("publishedAt", "")
            published_date = to_yyyy_mm_dd(published_at)

            view_count = int(stats.get("viewCount", 0) or 0)
            like_count = int(stats.get("likeCount", 0) or 0)
            comment_count = int(stats.get("commentCount", 0) or 0)

            # 구독자 수는 videos().list로 직접 나오지 않음 → channel API를 또 호출해야 하지만
            # 여기서는 간단히 0 또는 별도 로직 없이 placeholder로 둔다.
            # 필요하면 추후 channelId 기반으로 채널 통계를 추가.
            subscribers = 0

            row = {
                "video_id": vid,
                "url": f"https://www.youtube.com/watch?v={vid}",
                "title": title,
                "channel_title": channel_title,
                "published_at": published_at,
                "published_date": published_date,
                "duration_sec": duration_sec,
                "views": view_count,
                "likes": like_count,
                "comments": comment_count,
                "subscribers": subscribers
            }
            row["viral_score"] = viral_score(row)
            videos.append(row)

    # 정렬 (viewCount 기준)
    videos.sort(key=lambda x: x.get("views", 0), reverse=True)

    # 3) 시트로 내보내기
    sheet_url = None
    if auto_sheet and videos:
        header = [
            "채널명",
            "영상제목",
            "업로드날짜",
            "조회수",
            "좋아요수",
            "댓글수",
            "구독자수",
            "영상길이(초)",
            "바이럴점수",
            "영상링크"
        ]
        data_rows: List[List[Any]] = [header]
        for v in videos:
            data_rows.append([
                v.get("channel_title", ""),
                v.get("title", ""),
                v.get("published_date", ""),
                v.get("views", 0),
                v.get("likes", 0),
                v.get("comments", 0),
                v.get("subscribers", 0),
                v.get("duration_sec", 0),
                v.get("viral_score", 0.0),
                v.get("url", "")
            ])

        sheet_url = write_sheet(data_rows, q, region_code)

    result = {
        "keyword": q,
        "region": region_code or "GLOBAL",
        "count": len(videos),
        "videos": videos,
        "sheet_url": sheet_url
    }
    return result


# =========================
# /api/quick
# =========================

@app.get("/api/quick")
def quick(
    cmd: Optional[str] = Query(
        None,
        description='형식: "키워드 / 결과수 / days / 길이" 예) 이재명 / 30 / 30 / 180'
    ),
    q: Optional[str] = Query(None, description="키워드"),
    n: Optional[int] = Query(None, ge=1, le=200, description="결과수"),
    days: Optional[int] = Query(None, ge=1, le=180, description="기간(일)"),
    duration: Optional[int] = Query(None, ge=1, le=600, description="최대 길이(초)"),
    region: Optional[str] = Query("GLOBAL", description="지역코드: GLOBAL, KR, TW, JP 등")
):
    """
    한 줄 명령으로 빠르게 검색:
    - cmd 예: '이재명 / 30 / 30 / 180'
    """
    if cmd:
        parts = [p.strip() for p in cmd.split("/") if p.strip()]
        if len(parts) >= 1:
            q = q or parts[0]
        if len(parts) >= 2:
            try:
                n = int(parts[1])
            except ValueError:
                pass
        if len(parts) >= 3:
            try:
                days = int(parts[2])
            except ValueError:
                pass
        if len(parts) >= 4:
            try:
                duration = int(parts[3])
            except ValueError:
                pass

    q = q or "검색결과"
    n = n or 50
    days = days or 14
    duration = duration or 180
    region = region or "GLOBAL"

    return search_and_export(
        q=q,
        max_results=n,
        days=days,
        order="views",
        shorts_only=True,
        max_duration_sec=duration,
        auto_sheet=True,
        region=region
    )


# =========================
# /api/quick_webhook
# =========================

@app.post("/api/quick_webhook")
def quick_webhook(
    payload: QuickWebhookPayload = Body(...),
    x_token: Optional[str] = Header(None, convert_underscores=False)
):
    """
    봇/슬랙/텔레그램용 웹훅 엔드포인트.
    Header: X-Token: QUICK_WEBHOOK_TOKEN
    Body: { "cmd": "...", "q": "...", "n": 30, "days": 7, "duration": 180, "region": "TW" }
    """
    if QUICK_WEBHOOK_TOKEN and x_token != QUICK_WEBHOOK_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid token")

    cmd = payload.cmd
    q = payload.q
    n = payload.n
    days = payload.days
    dur = payload.duration
    region = payload.region

    if cmd:
        parts = [p.strip() for p in cmd.split("/") if p.strip()]
        if len(parts) >= 1:
            q = q or parts[0]
        if len(parts) >= 2:
            try:
                n = int(parts[1])
            except ValueError:
                pass
        if len(parts) >= 3:
            try:
                days = int(parts[2])
            except ValueError:
                pass
        if len(parts) >= 4:
            try:
                dur = int(parts[3])
            except ValueError:
                pass

    q = q or "검색결과"
    n = int(n) if n else 50
    days = int(days) if days else 14
    dur = int(dur) if dur else 180
    region = region or "GLOBAL"

    result = search_and_export(
        q=q,
        max_results=n,
        days=days,
        order="views",
        shorts_only=True,
        max_duration_sec=dur,
        auto_sheet=True,
        region=region
    )

    return {
        "status": "ok",
        "message": f"'{q}' 검색 완료 (region={region})",
        "result": result
    }


# =========================
# 엔트리 포인트 (로컬 실행용)
# =========================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=int(os.getenv("PORT", 8000)), reload=True)
