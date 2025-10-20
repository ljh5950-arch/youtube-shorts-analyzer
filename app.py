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

from fastapi import Body
from googleapiclient.discovery import build_from_document
from google.oauth2.service_account import Credentials
import json

# ====== Google Sheets API 설정 ======
def get_sheets_service():
    # Render 환경변수에 저장된 서비스 계정 JSON 읽기
    sa_json = os.getenv("GOOGLE_SA_JSON")
    if not sa_json:
        raise Exception("서비스 계정 JSON이 설정되지 않았습니다.")
    sa_info = json.loads(sa_json)
    creds = Credentials.from_service_account_info(sa_info, scopes=["https://www.googleapis.com/auth/spreadsheets"])
    service = build("sheets", "v4", credentials=creds)
    return service


@app.post("/api/export/sheets")
def export_to_sheets(
    rows: list = Body(..., description="유튜브 쇼츠 검색 결과 배열"),
    sheet_name: str = Body(None, description="시트 이름 (비워두면 자동 생성)")
):
    """Google Sheets로 결과 업로드"""
    parent_sheet_id = os.getenv("SHEETS_PARENT_SPREADSHEET_ID")
    if not parent_sheet_id:
        return {"error": "SHEETS_PARENT_SPREADSHEET_ID 환경변수가 없습니다."}

    if not sheet_name:
        # 키워드_날짜 형식으로 자동 이름 생성
        today = datetime.utcnow().strftime("%Y%m%d")
        sheet_name = f"검색결과_{today}"

    sheets = get_sheets_service()

    # 새 탭 만들기
    try:
        sheets.spreadsheets().batchUpdate(
            spreadsheetId=parent_sheet_id,
            body={"requests": [{"addSheet": {"properties": {"title": sheet_name}}}]}
        ).execute()
    except Exception:
        pass  # 이미 시트가 존재하면 무시

    # 데이터 행 변환
    headers = list(rows[0].keys())
    values = [headers] + [[str(r.get(h, "")) for h in headers] for r in rows]

    # 시트에 데이터 기록
    sheets.spreadsheets().values().update(
        spreadsheetId=parent_sheet_id,
        range=f"{sheet_name}!A1",
        valueInputOption="RAW",
        body={"values": values}
    ).execute()

    return {"message": "업로드 완료", "sheet_url": f"https://docs.google.com/spreadsheets/d/{parent_sheet_id}"}

@app.get("/health")
def health():
    return {"ok": True, "time": datetime.utcnow().isoformat()}
