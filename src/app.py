"""
app.py — 배송 경로 자동 정리 프로그램
비개발자용 Windows GUI  |  GitHub Actions → PyInstaller → .exe 배포

주소 검색: 카카오 우편번호 서비스 (postcode.map.kakao.com)
중단 기능: threading.Event 로 파이프라인 전 단계 즉시 중단 + 체크포인트 초기화
"""

import multiprocessing
import os
import sys
import threading
import time

import customtkinter as ctk
from tkinter import filedialog, messagebox
import pandas as pd
from openpyxl import load_workbook

BASE = getattr(sys, '_MEIPASS', os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE)

from core.geocoder  import geocode, reverse_geocode, verify_address
from core.optimizer import (build_time_matrix, optimize_route,
                             clear_checkpoint, CHECKPOINT_FILE)

# ─── 디자인 토큰 ──────────────────────────────────────────────────────────────
ctk.set_appearance_mode("dark")
ctk.set_default_color_theme("blue")

_BG       = "#0F1117"   # 최상위 배경
_CARD     = "#1A1D27"   # 카드 배경
_BORDER   = "#2A2D3E"   # 카드 테두리
_ACCENT   = "#4F8EF7"   # 포인트 블루
_ACCENT2  = "#7C3AED"   # 보조 퍼플
_SUCCESS  = "#22C55E"   # 초록
_WARN     = "#F59E0B"   # 노랑
_DANGER   = "#EF4444"   # 빨강
_TEXT     = "#E2E8F0"   # 기본 텍스트
_SUBTEXT  = "#94A3B8"   # 보조 텍스트
_FONT_H   = ("Pretendard", 15, "bold")   # 없으면 맑은 고딕 fallback
_FONT_B   = ("맑은 고딕", 11)


# ─────────────────────────────────────────────────────────────────────────────
# 카카오 우편번호 서비스 HTML
# ─────────────────────────────────────────────────────────────────────────────
_POSTCODE_HTML = """<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<style>
  * { margin:0; padding:0; box-sizing:border-box; }
  body { width:100%; height:100vh; overflow:hidden; background:#fff; }
  #wrap { width:100%; height:100%; }
</style>
<script src="//t1.kakaocdn.net/mapjsapi/bundle/postcode/prod/postcode.v2.js"></script>
</head>
<body>
<div id="wrap"></div>
<script>
new kakao.Postcode({
  oncomplete: function(data) {
    var addr = (data.userSelectedType === 'R') ? data.roadAddress : data.jibunAddress;
    var extra = '';
    if (data.userSelectedType === 'R') {
      if (data.bname !== '' && /[동로가]$/.test(data.bname)) extra += data.bname;
      if (data.buildingName !== '' && data.apartment === 'Y')
        extra += (extra !== '' ? ', ' : '') + data.buildingName;
      if (extra !== '') extra = ' (' + extra + ')';
    }
    window.pywebview.api.on_select(addr + extra, data.zonecode);
  },
  width:'100%', height:'100%'
}).embed(document.getElementById('wrap'));
</script>
</body>
</html>"""


def _postcode_worker(queue, title):
    import pywebview
    _result = {}

    class _Api:
        def on_select(self, address, zonecode):
            _result['address'] = address
            _result['zonecode'] = zonecode
            _win.destroy()

    _api = _Api()
    _win = pywebview.create_window(
        title, html=_POSTCODE_HTML, js_api=_api,
        width=520, height=620, on_top=True)
    pywebview.start()
    queue.put(_result)


def _open_postcode(title="주소 검색"):
    q = multiprocessing.Queue()
    p = multiprocessing.Process(target=_postcode_worker, args=(q, title))
    p.start()
    p.join()
    return q.get() if not q.empty() else {}


# ─────────────────────────────────────────────────────────────────────────────
# 공용 헬퍼: 카드 프레임
# ─────────────────────────────────────────────────────────────────────────────
def _card(parent, **kw):
    return ctk.CTkFrame(parent, fg_color=_CARD,
                        corner_radius=12, border_width=1,
                        border_color=_BORDER, **kw)


def _section_label(parent, text):
    ctk.CTkLabel(parent, text=text,
                 font=ctk.CTkFont(size=11, weight="bold"),
                 text_color=_SUBTEXT).pack(anchor="w", pady=(0, 6))


# ─────────────────────────────────────────────────────────────────────────────
# 출발지 주소 찾기 팝업
# ─────────────────────────────────────────────────────────────────────────────
class AddressSearchDialog(ctk.CTkToplevel):
    def __init__(self, parent, headers: dict):
        super().__init__(parent)
        self.headers = headers
        self.result  = None
        self.title("출발지 주소 찾기")
        self.geometry("460x260")
        self.resizable(False, False)
        self.configure(fg_color=_BG)
        self.grab_set()
        self._build()

    def _build(self):
        card = _card(self)
        card.pack(fill="both", expand=True, padx=16, pady=16)

        ctk.CTkLabel(card, text="🏢  출발 창고 / 사무실 주소",
                     font=ctk.CTkFont(size=14, weight="bold"),
                     text_color=_TEXT).pack(pady=(20, 4), padx=20, anchor="w")
        ctk.CTkLabel(card, text="카카오 우편번호 검색으로 정확한 주소를 찾습니다",
                     font=ctk.CTkFont(size=11),
                     text_color=_SUBTEXT).pack(padx=20, anchor="w")

        self.search_btn = ctk.CTkButton(
            card, text="🔍  주소 검색 창 열기",
            height=42, fg_color=_ACCENT, hover_color="#3B6FD4",
            font=ctk.CTkFont(size=13, weight="bold"),
            command=self._launch)
        self.search_btn.pack(fill="x", padx=20, pady=16)

        self.status = ctk.CTkLabel(card, text="",
                                   font=ctk.CTkFont(size=11),
                                   text_color=_SUBTEXT, wraplength=400)
        self.status.pack(padx=20, pady=(0, 8))

        btn_row = ctk.CTkFrame(card, fg_color="transparent")
        btn_row.pack(fill="x", padx=20, pady=(0, 16))
        self.ok_btn = ctk.CTkButton(
            btn_row, text="확인", height=36,
            fg_color=_SUCCESS, hover_color="#16A34A",
            state="disabled", command=self._confirm)
        self.ok_btn.pack(side="left", expand=True, padx=(0, 6))
        ctk.CTkButton(btn_row, text="닫기", height=36,
                      fg_color="#374151", hover_color="#4B5563",
                      command=self.destroy).pack(side="left", expand=True, padx=(6, 0))

    def _launch(self):
        self.search_btn.configure(state="disabled", text="⏳  열리는 중...")
        self.status.configure(text="카카오 우편번호 검색 창이 열립니다...", text_color=_SUBTEXT)
        threading.Thread(target=self._worker, daemon=True).start()

    def _worker(self):
        raw = _open_postcode("출발지 주소 찾기")
        if raw.get('address'):
            geo = geocode(raw['address'], self.headers)
            if geo:
                self.result = {'address': raw['address'], 'lat': geo['lat'], 'lon': geo['lon']}
                self.after(0, self._on_success)
            else:
                self.after(0, self._on_fail, raw['address'])
        else:
            self.after(0, self._on_cancel)

    def _on_success(self):
        self.status.configure(text=f"✅  {self.result['address']}", text_color=_SUCCESS)
        self.ok_btn.configure(state="normal")
        self.search_btn.configure(state="normal", text="🔍  주소 검색 창 열기")

    def _on_fail(self, addr):
        self.status.configure(text=f"⚠️  좌표 변환 실패: {addr}", text_color=_WARN)
        self.search_btn.configure(state="normal", text="🔍  주소 검색 창 열기")

    def _on_cancel(self):
        self.status.configure(text="검색을 취소했습니다", text_color=_SUBTEXT)
        self.search_btn.configure(state="normal", text="🔍  주소 검색 창 열기")

    def _confirm(self):
        self.destroy()


# ─────────────────────────────────────────────────────────────────────────────
# 주소 불일치 수정 팝업
# ─────────────────────────────────────────────────────────────────────────────
class AddressFixDialog(ctk.CTkToplevel):
    def __init__(self, parent, headers: dict,
                 name: str, orig: str, rev: str,
                 event: threading.Event, result_holder: dict):
        super().__init__(parent)
        self.headers       = headers
        self.event         = event
        self.result_holder = result_holder
        self._pending      = None
        self.title("주소 확인 필요")
        self.geometry("540x460")
        self.resizable(False, False)
        self.configure(fg_color=_BG)
        self.grab_set()
        self.protocol("WM_DELETE_WINDOW", self._skip)
        self._build(name, orig, rev)

    def _build(self, name, orig, rev):
        # 상단 경고 바
        top = ctk.CTkFrame(self, fg_color=_DANGER, corner_radius=0, height=48)
        top.pack(fill="x")
        top.pack_propagate(False)
        ctk.CTkLabel(top, text="⚠️   주소 불일치 감지",
                     font=ctk.CTkFont(size=13, weight="bold"),
                     text_color="white").pack(expand=True)

        card = _card(self)
        card.pack(fill="both", expand=True, padx=16, pady=12)

        inner = ctk.CTkFrame(card, fg_color="transparent")
        inner.pack(fill="both", expand=True, padx=18, pady=14)

        ctk.CTkLabel(inner, text=f"수령인:  {name}",
                     font=ctk.CTkFont(size=13, weight="bold"),
                     text_color=_TEXT).pack(anchor="w", pady=(0, 10))

        for icon, label, text, color in [
            ("📋", "엑셀 입력 주소",    orig,                             "#1E3A5F"),
            ("🗺️", "지도 확인 주소",  rev or "(위치 못 찾음)",           "#3B1212"),
        ]:
            ctk.CTkLabel(inner, text=f"{icon}  {label}",
                         font=ctk.CTkFont(size=10), text_color=_SUBTEXT).pack(anchor="w")
            ctk.CTkLabel(inner, text=text, wraplength=480,
                         font=ctk.CTkFont(size=11),
                         fg_color=color, corner_radius=6,
                         padx=10, pady=5, text_color=_TEXT).pack(fill="x", pady=(2, 8))

        ctk.CTkLabel(inner,
                     text="두 주소가 다릅니다. 아래에서 올바른 주소를 검색해 선택하세요.",
                     font=ctk.CTkFont(size=11), text_color=_WARN).pack(anchor="w", pady=(0, 10))

        self.search_btn = ctk.CTkButton(
            inner, text="🔍  올바른 주소 검색",
            height=38, fg_color=_ACCENT, hover_color="#3B6FD4",
            font=ctk.CTkFont(size=12, weight="bold"),
            command=self._launch)
        self.search_btn.pack(fill="x", pady=(0, 6))

        self.verify_lbl = ctk.CTkLabel(inner, text="", font=ctk.CTkFont(size=11),
                                        text_color=_SUBTEXT, wraplength=480)
        self.verify_lbl.pack(anchor="w", pady=(0, 8))

        btn_row = ctk.CTkFrame(inner, fg_color="transparent")
        btn_row.pack(fill="x")
        self.ok_btn = ctk.CTkButton(btn_row, text="✅  이 주소로 수정",
                                     height=36, fg_color=_SUCCESS,
                                     hover_color="#16A34A",
                                     state="disabled", command=self._confirm)
        self.ok_btn.pack(side="left", expand=True, padx=(0, 6))
        ctk.CTkButton(btn_row, text="원본 그대로 사용",
                       height=36, fg_color="#374151", hover_color="#4B5563",
                       command=self._skip).pack(side="left", expand=True, padx=(6, 0))

    def _launch(self):
        self.search_btn.configure(state="disabled", text="⏳  열리는 중...")
        threading.Thread(target=self._worker, daemon=True).start()

    def _worker(self):
        raw = _open_postcode("올바른 주소 찾기")
        if raw.get('address'):
            geo = geocode(raw['address'], self.headers)
            if geo:
                rev     = reverse_geocode(geo['lat'], geo['lon'], self.headers)
                verdict = verify_address(raw['address'], rev)
                self._pending = {'address': raw['address'], 'lat': geo['lat'],
                                 'lon': geo['lon'], 'reverse': rev, 'verdict': verdict}
                self.after(0, self._on_success, verdict, rev, raw['address'])
            else:
                self.after(0, self._on_fail)
        else:
            self.after(0, self._on_cancel)

    def _on_success(self, verdict, rev, addr):
        color = _SUCCESS if verdict == '일치' else _WARN
        icon  = "✅" if verdict == '일치' else "⚠️"
        self.verify_lbl.configure(text=f"{icon}  {addr}", text_color=color)
        self.ok_btn.configure(state="normal")
        self.search_btn.configure(state="normal", text="🔍  올바른 주소 검색")

    def _on_fail(self):
        self.verify_lbl.configure(text="⚠️  좌표 변환 실패", text_color=_WARN)
        self.search_btn.configure(state="normal", text="🔍  올바른 주소 검색")

    def _on_cancel(self):
        self.verify_lbl.configure(text="검색 취소됨", text_color=_SUBTEXT)
        self.search_btn.configure(state="normal", text="🔍  올바른 주소 검색")

    def _confirm(self):
        if self._pending:
            self.result_holder.update(self._pending)
        self.event.set()
        self.destroy()

    def _skip(self):
        self.result_holder.clear()
        self.event.set()
        self.destroy()


# ─────────────────────────────────────────────────────────────────────────────
# 완료 팝업
# ─────────────────────────────────────────────────────────────────────────────
class DoneDialog(ctk.CTkToplevel):
    def __init__(self, parent, output_path: str, warn_cnt: int):
        super().__init__(parent)
        self.title("작업 완료")
        self.geometry("480x260")
        self.resizable(False, False)
        self.configure(fg_color=_BG)
        self.grab_set()

        card = _card(self)
        card.pack(fill="both", expand=True, padx=16, pady=16)

        ctk.CTkLabel(card, text="🎉  배송 순서 정리 완료",
                     font=ctk.CTkFont(size=15, weight="bold"),
                     text_color=_SUCCESS).pack(pady=(20, 4))

        folder = os.path.dirname(output_path)
        fname  = os.path.basename(output_path)

        ctk.CTkLabel(card, text=fname,
                     font=ctk.CTkFont(size=12, weight="bold"),
                     text_color=_TEXT).pack(pady=(0, 2))
        ctk.CTkLabel(card, text=folder, wraplength=440,
                     font=ctk.CTkFont(size=10), text_color=_SUBTEXT).pack(pady=(0, 8))

        if warn_cnt:
            ctk.CTkLabel(card,
                         text=f"⚠️  주소 미수정 {warn_cnt}건 — '주소검증결과' 열 확인 권장",
                         font=ctk.CTkFont(size=11), text_color=_WARN).pack(pady=(0, 8))

        btn_row = ctk.CTkFrame(card, fg_color="transparent")
        btn_row.pack(fill="x", padx=20, pady=(0, 16))
        ctk.CTkButton(btn_row, text="📂  저장 폴더 열기", height=36,
                      fg_color=_ACCENT, hover_color="#3B6FD4",
                      command=lambda: self._open(folder)).pack(
            side="left", expand=True, padx=(0, 6))
        ctk.CTkButton(btn_row, text="닫기", height=36,
                      fg_color="#374151", hover_color="#4B5563",
                      command=self.destroy).pack(side="left", expand=True, padx=(6, 0))

    @staticmethod
    def _open(path):
        if sys.platform == "win32":
            os.startfile(path)
        elif sys.platform == "darwin":
            os.system(f'open "{path}"')
        else:
            os.system(f'xdg-open "{path}"')


# ─────────────────────────────────────────────────────────────────────────────
# 메인 앱
# ─────────────────────────────────────────────────────────────────────────────
class App(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title("배송 경로 자동 정리")
        self.geometry("700x860")
        self.resizable(False, False)
        self.configure(fg_color=_BG)

        self.file_path  = ""
        self.start_lat  = None
        self.start_lon  = None
        self._stop_evt  = threading.Event()   # 중단 신호

        self._build_ui()

    # ── UI ───────────────────────────────────────────────────────────────────
    def _build_ui(self):
        # ── 헤더 ─────────────────────────────────────────────────────────────
        hdr = ctk.CTkFrame(self, fg_color=_CARD,
                           corner_radius=0, height=72, border_width=0)
        hdr.pack(fill="x")
        hdr.pack_propagate(False)

        hdr_inner = ctk.CTkFrame(hdr, fg_color="transparent")
        hdr_inner.pack(expand=True)

        ctk.CTkLabel(hdr_inner, text="🚚",
                     font=ctk.CTkFont(size=26)).pack(side="left", padx=(0, 10))
        title_col = ctk.CTkFrame(hdr_inner, fg_color="transparent")
        title_col.pack(side="left")
        ctk.CTkLabel(title_col, text="배송 경로 자동 정리",
                     font=ctk.CTkFont(size=18, weight="bold"),
                     text_color=_TEXT).pack(anchor="w")
        ctk.CTkLabel(title_col, text="카카오 API 기반 최적 경로 계산",
                     font=ctk.CTkFont(size=11),
                     text_color=_SUBTEXT).pack(anchor="w")

        # ── 구분선 ────────────────────────────────────────────────────────────
        ctk.CTkFrame(self, fg_color=_BORDER, height=1,
                     corner_radius=0).pack(fill="x")

        # ── 스크롤 가능한 본문 ─────────────────────────────────────────────────
        body = ctk.CTkScrollableFrame(self, fg_color=_BG, scrollbar_fg_color=_CARD)
        body.pack(fill="both", expand=True, padx=0, pady=0)

        pad = {"padx": 20, "pady": 6}

        # ① API 키
        c1 = _card(body)
        c1.pack(fill="x", **pad)
        inner1 = ctk.CTkFrame(c1, fg_color="transparent")
        inner1.pack(fill="x", padx=16, pady=14)
        _section_label(inner1, "① 카카오 REST API 키")
        self.api_entry = ctk.CTkEntry(
            inner1, placeholder_text="카카오 REST API 키를 입력하세요",
            height=40, show="*",
            fg_color="#0D1117", border_color=_BORDER,
            text_color=_TEXT, placeholder_text_color=_SUBTEXT)
        self.api_entry.pack(fill="x")

        # ② 출발지
        c2 = _card(body)
        c2.pack(fill="x", **pad)
        inner2 = ctk.CTkFrame(c2, fg_color="transparent")
        inner2.pack(fill="x", padx=16, pady=14)
        _section_label(inner2, "② 출발 창고 / 사무실 주소")

        row2 = ctk.CTkFrame(inner2, fg_color="transparent")
        row2.pack(fill="x")
        self.addr_entry = ctk.CTkEntry(
            row2, placeholder_text="[주소 찾기]를 눌러 검색하세요",
            height=40, state="disabled",
            fg_color="#0D1117", border_color=_BORDER,
            text_color=_TEXT, placeholder_text_color=_SUBTEXT)
        self.addr_entry.pack(side="left", fill="x", expand=True, padx=(0, 8))
        ctk.CTkButton(row2, text="🔍 주소 찾기", width=110, height=40,
                      fg_color=_ACCENT, hover_color="#3B6FD4",
                      font=ctk.CTkFont(size=12, weight="bold"),
                      command=self._find_origin).pack(side="left")

        self.addr_ok = ctk.CTkLabel(inner2, text="",
                                     font=ctk.CTkFont(size=11),
                                     text_color=_SUCCESS)
        self.addr_ok.pack(anchor="w", pady=(6, 0))

        # ③ 파일
        c3 = _card(body)
        c3.pack(fill="x", **pad)
        inner3 = ctk.CTkFrame(c3, fg_color="transparent")
        inner3.pack(fill="x", padx=16, pady=14)
        _section_label(inner3, "③ 배송 목록 엑셀 파일 (.xlsx)")

        row3 = ctk.CTkFrame(inner3, fg_color="transparent")
        row3.pack(fill="x")
        ctk.CTkButton(row3, text="📁 파일 선택", width=110, height=40,
                      fg_color="#374151", hover_color="#4B5563",
                      font=ctk.CTkFont(size=12),
                      command=self._pick_file).pack(side="left", padx=(0, 8))
        self.file_lbl = ctk.CTkLabel(row3, text="선택된 파일 없음",
                                      text_color=_SUBTEXT,
                                      font=ctk.CTkFont(size=11),
                                      wraplength=460, anchor="w")
        self.file_lbl.pack(side="left", fill="x", expand=True)

        # ── 실행 / 중단 버튼 ──────────────────────────────────────────────────
        btn_frame = ctk.CTkFrame(body, fg_color="transparent")
        btn_frame.pack(fill="x", padx=20, pady=10)

        self.run_btn = ctk.CTkButton(
            btn_frame, text="▶   배송 순서 자동 정리 시작",
            height=52, font=ctk.CTkFont(size=15, weight="bold"),
            fg_color=_ACCENT, hover_color="#3B6FD4",
            command=self._start)
        self.run_btn.pack(side="left", fill="x", expand=True, padx=(0, 8))

        self.stop_btn = ctk.CTkButton(
            btn_frame, text="⏹  중단",
            height=52, width=100,
            font=ctk.CTkFont(size=13, weight="bold"),
            fg_color=_DANGER, hover_color="#B91C1C",
            state="disabled",
            command=self._request_stop)
        self.stop_btn.pack(side="left")

        # ── 진행 상황 카드 ─────────────────────────────────────────────────────
        c4 = _card(body)
        c4.pack(fill="x", **pad)
        inner4 = ctk.CTkFrame(c4, fg_color="transparent")
        inner4.pack(fill="x", padx=16, pady=14)

        step_row = ctk.CTkFrame(inner4, fg_color="transparent")
        step_row.pack(fill="x", pady=(0, 6))
        self.step_lbl = ctk.CTkLabel(step_row, text="대기 중",
                                      font=ctk.CTkFont(size=12, weight="bold"),
                                      text_color=_SUBTEXT)
        self.step_lbl.pack(side="left")
        self.pct_lbl = ctk.CTkLabel(step_row, text="",
                                     font=ctk.CTkFont(size=12, weight="bold"),
                                     text_color=_ACCENT)
        self.pct_lbl.pack(side="right")

        self.bar = ctk.CTkProgressBar(inner4, height=8,
                                       fg_color=_BORDER,
                                       progress_color=_ACCENT)
        self.bar.pack(fill="x", pady=(0, 12))
        self.bar.set(0)

        _section_label(inner4, "작업 현황")
        self.log = ctk.CTkTextbox(
            inner4, height=260,
            fg_color="#0D1117", border_color=_BORDER, border_width=1,
            corner_radius=8,
            font=ctk.CTkFont(family="맑은 고딕", size=11),
            text_color=_TEXT,
            state="disabled", wrap="word")
        self.log.pack(fill="both", expand=True)

        # 하단 여백
        ctk.CTkFrame(body, fg_color="transparent", height=20).pack()

    # ── 이벤트 ───────────────────────────────────────────────────────────────
    def _find_origin(self):
        k = self.api_entry.get().strip()
        if not k:
            messagebox.showwarning("API 키 필요", "먼저 카카오 API 키를 입력해주세요.")
            return
        dlg = AddressSearchDialog(self, {"Authorization": f"KakaoAK {k}"})
        self.wait_window(dlg)
        if dlg.result:
            r = dlg.result
            self.start_lat, self.start_lon = r['lat'], r['lon']
            self.addr_entry.configure(state="normal")
            self.addr_entry.delete(0, "end")
            self.addr_entry.insert(0, r['address'])
            self.addr_entry.configure(state="disabled")
            self.addr_ok.configure(text=f"✅  확인된 주소: {r['address']}")

    def _pick_file(self):
        p = filedialog.askopenfilename(filetypes=[("Excel 파일", "*.xlsx *.xls")])
        if p:
            self.file_path = p
            self.file_lbl.configure(text=os.path.basename(p), text_color=_TEXT)

    def _start(self):
        if not self.api_entry.get().strip():
            messagebox.showwarning("입력 필요", "API 키를 입력해주세요.")
            return
        if self.start_lat is None:
            messagebox.showwarning("입력 필요", "출발지 주소를 선택해주세요.")
            return
        if not self.file_path:
            messagebox.showwarning("입력 필요", "엑셀 파일을 선택해주세요.")
            return

        self._stop_evt.clear()
        self.run_btn.configure(state="disabled", text="⏳  작업 진행 중...")
        self.stop_btn.configure(state="normal")
        self._clear_log()
        self.bar.set(0)
        threading.Thread(target=self._pipeline, daemon=True).start()

    def _request_stop(self):
        """중단 버튼 — 파이프라인에 중단 신호 전송 + 체크포인트 초기화."""
        if not messagebox.askyesno("중단 확인",
                                   "작업을 중단하시겠습니까?\n\n"
                                   "중단하면 저장된 도로 계산 데이터도 초기화됩니다."):
            return
        self._stop_evt.set()
        self.stop_btn.configure(state="disabled", text="중단 중...")
        self._log("⏹  사용자 중단 요청 — 현재 단계 완료 후 종료됩니다...")

    # ── 로그 / 진행 헬퍼 ─────────────────────────────────────────────────────
    def _log(self, m):
        self.after(0, self._write_log, m)

    def _write_log(self, m):
        self.log.configure(state="normal")
        self.log.insert("end", m + "\n")
        self.log.see("end")
        self.log.configure(state="disabled")

    def _clear_log(self):
        self.log.configure(state="normal")
        self.log.delete("1.0", "end")
        self.log.configure(state="disabled")

    def _step(self, txt, pct, color=None):
        self.after(0, self.step_lbl.configure,
                   {"text": txt, "text_color": color or _TEXT})
        self.after(0, self.bar.set, pct)
        self.after(0, self.pct_lbl.configure,
                   {"text": f"{int(pct * 100)}%" if pct > 0 else ""})

    def _reset_btn(self):
        self.after(0, self.run_btn.configure,
                   {"state": "normal", "text": "▶   배송 순서 자동 정리 시작"})
        self.after(0, self.stop_btn.configure,
                   {"state": "disabled", "text": "⏹  중단"})

    def _open_fix(self, headers, name, orig, rev, event, holder):
        AddressFixDialog(self, headers, name, orig, rev, event, holder)

    # ── 중단 체크 헬퍼 ────────────────────────────────────────────────────────
    def _stopped(self) -> bool:
        return self._stop_evt.is_set()

    def _abort(self):
        """체크포인트 초기화 후 UI 리셋."""
        clear_checkpoint()
        self._log("🗑️  저장된 도로 계산 데이터 초기화 완료")
        self._step("중단됨", 0, _DANGER)
        self._reset_btn()

    # ── 파이프라인 ───────────────────────────────────────────────────────────
    def _pipeline(self):
        key     = self.api_entry.get().strip()
        headers = {"Authorization": f"KakaoAK {key}"}

        try:
            # ━━━━ 1단계: 엑셀 읽기 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            self._step("1단계 — 엑셀 파일 읽는 중", 0.05)
            self._log("━" * 38)
            self._log("  1단계  엑셀 파일 읽기")
            self._log("━" * 38)

            try:
                df = pd.read_excel(self.file_path, header=0)
            except Exception as e:
                self._log(f"❌  파일 읽기 실패: {e}")
                self._reset_btn()
                return

            if '택배받을 주소' not in df.columns:
                self._log("❌  '택배받을 주소' 열이 없습니다.")
                self._reset_btn()
                return

            df    = df.dropna(subset=['택배받을 주소'])
            df['택배받을 주소'] = df['택배받을 주소'].astype(str).str.strip()
            total = len(df)
            self._log(f"✅  {total}건 확인 완료")

            if self._stopped(): self._abort(); return

            # ━━━━ 2단계: 위치 확인 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            self._step("2단계 — 각 배송지 위치 확인 중", 0.15)
            self._log("\n" + "━" * 38)
            self._log("  2단계  각 배송지 위치 확인")
            self._log("━" * 38)

            lats, lons, k_addrs = [], [], []
            for i, (_, row) in enumerate(df.iterrows()):
                if self._stopped(): self._abort(); return
                r = geocode(row['택배받을 주소'], headers)
                time.sleep(0.15)
                name = row.get('이름', '')
                if r:
                    lats.append(r['lat']); lons.append(r['lon'])
                    k_addrs.append(r['kakao_road_addr'])
                    self._log(f"  ✅  ({i+1}/{total})  {name}")
                else:
                    lats.append(None); lons.append(None); k_addrs.append('')
                    self._log(f"  ⚠️   ({i+1}/{total})  {name}  — 위치 못 찾음")
                self._step("2단계 — 각 배송지 위치 확인 중",
                           0.15 + (i + 1) / total * 0.15)

            df['Latitude']      = lats
            df['Longitude']     = lons
            df['카카오_확인주소'] = k_addrs
            self._log(f"\n✅  2단계 완료 — {sum(1 for v in lats if v)}/{total}건")

            if self._stopped(): self._abort(); return

            # ━━━━ 3단계: 주소 정확도 검사 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            self._step("3단계 — 주소 정확도 검사 중", 0.32)
            self._log("\n" + "━" * 38)
            self._log("  3단계  주소 정확도 검사")
            self._log("━" * 38)

            revs, verdicts, fix_cnt = [], [], 0
            for i, (di, row) in enumerate(df.iterrows()):
                if self._stopped(): self._abort(); return

                lat, lon = row['Latitude'], row['Longitude']
                name     = row.get('이름', f'항목{i+1}')
                if pd.notna(lat) and pd.notna(lon):
                    rev     = reverse_geocode(lat, lon, headers)
                    time.sleep(0.15)
                    verdict = verify_address(row['택배받을 주소'], rev)
                else:
                    rev, verdict = '', '위치없음'

                if verdict not in ('일치', '위치없음', '확인불가'):
                    self._log(f"  ⚠️   ({i+1}/{total})  {name}  — 불일치 → 팝업 확인")
                    self._step("⚠️  불일치 — 팝업에서 올바른 주소를 검색해주세요",
                               0.32 + (i + 1) / total * 0.13, _WARN)
                    ev, holder = threading.Event(), {}
                    self.after(0, self._open_fix,
                               headers, name, row['택배받을 주소'], rev, ev, holder)
                    ev.wait()
                    if self._stopped(): self._abort(); return
                    if holder:
                        df.at[di, 'Latitude']      = holder['lat']
                        df.at[di, 'Longitude']     = holder['lon']
                        df.at[di, '카카오_확인주소'] = holder['address']
                        rev     = holder.get('reverse', holder['address'])
                        verdict = holder.get('verdict', '수정됨')
                        fix_cnt += 1
                        self._log(f"     →  수정: {holder['address']}")
                    else:
                        self._log("     →  원본 그대로 사용")
                else:
                    icon = "✅" if verdict == '일치' else "➖"
                    self._log(f"  {icon}  ({i+1}/{total})  {name}  — {verdict}")

                revs.append(rev); verdicts.append(verdict)
                self._step("3단계 — 주소 정확도 검사 중",
                           0.32 + (i + 1) / total * 0.13)

            df['역지오코딩_주소'] = revs
            df['주소검증결과']   = verdicts
            warn_cnt = sum(1 for v in verdicts
                           if v not in ('일치', '위치없음', '확인불가', '수정됨'))
            self._log(f"\n✅  3단계 완료"
                      + (f"  — {fix_cnt}건 수정, ⚠️ {warn_cnt}건 미수정"
                         if fix_cnt or warn_cnt else "  — 모두 정상"))

            if self._stopped(): self._abort(); return

            # ━━━━ 4단계: 도로 이동 시간 계산 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            self._step("4단계 — 도로 이동 시간 계산 중 (시간 소요)", 0.46)
            self._log("\n" + "━" * 38)
            self._log("  4단계  도로 이동 시간 계산")
            self._log("  (배송지 수에 따라 10~15분 소요)")
            self._log("━" * 38)

            vdf   = df.dropna(subset=['Latitude', 'Longitude']).copy()
            nodes = [{'id': -1, 'name': '출발지',
                      'lat': self.start_lat, 'lon': self.start_lon}]
            for idx, row in vdf.iterrows():
                nodes.append({'id':   idx,
                              'name': row.get('이름', f'배송지{idx}'),
                              'lat':  row['Latitude'],
                              'lon':  row['Longitude']})

            def _prog(done, tot):
                if self._stopped():
                    return
                self._step("4단계 — 도로 이동 시간 계산 중",
                           0.46 + done / tot * 0.24)
                self._log(f"  →  {done} / {tot} 경로 계산 완료")

            clear_checkpoint()
            matrix = build_time_matrix(nodes, headers,
                                       progress_cb=_prog,
                                       stop_event=self._stop_evt)

            if self._stopped(): self._abort(); return
            self._log(f"✅  4단계 완료 — {len(nodes)}개 지점")

            # ━━━━ 5단계: 최적 배송 순서 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            self._step("5단계 — 최적 배송 순서 계산 중 (최대 3분)", 0.72)
            self._log("\n" + "━" * 38)
            self._log("  5단계  최적 배송 순서 계산  (최대 3분)")
            self._log("━" * 38)

            ordered = optimize_route(nodes, matrix)
            if self._stopped(): self._abort(); return
            if ordered is None:
                self._log("❌  순서 계산 실패")
                self._reset_btn()
                return
            mapping = {nodes[ni]['id']: step for step, ni in enumerate(ordered, 1)}
            self._log(f"✅  5단계 완료 — {len(ordered)}건 순서 결정")

            # ━━━━ 6단계: 결과 저장 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            self._step("6단계 — 결과 파일 저장 중", 0.90)
            self._log("\n" + "━" * 38)
            self._log("  6단계  결과 파일 저장")
            self._log("━" * 38)

            out = self._save_xlsx(mapping)
            if out is None:
                self._reset_btn()
                return

            self._step("✅  모든 작업 완료!", 1.0, _SUCCESS)
            self._log(f"\n🎉  완료!")
            self._log(f"    파일명: {os.path.basename(out)}")
            self._log(f"    위치:   {os.path.dirname(out)}")
            self.after(0, lambda: DoneDialog(self, out, warn_cnt))

        except Exception as e:
            self._log(f"\n❌  오류: {e}")
            self.after(0, lambda: messagebox.showerror("오류", str(e)))
        finally:
            self._reset_btn()

    # ── xlsx 저장 ─────────────────────────────────────────────────────────────
    def _save_xlsx(self, mapping: dict):
        try:
            wb   = load_workbook(self.file_path)
            ws   = wb.active
            hrow = [c.value for c in ws[1]]

            if '배송순서' in hrow:
                col = hrow.index('배송순서') + 1
                self._log("  '배송순서' 열 존재 → 덮어씁니다")
            else:
                ws.insert_cols(1)
                ws.cell(row=1, column=1, value='배송순서')
                col = 1
                self._log("  '배송순서' 열 없음 → 첫 번째 열에 추가")

            for df_idx, order_val in mapping.items():
                ws.cell(row=df_idx + 2, column=col, value=order_val)

            base = os.path.splitext(self.file_path)[0]
            out  = f"{base}_배송순서완성.xlsx"
            wb.save(out)
            self._log(f"✅  저장 완료: {os.path.basename(out)}")
            return out
        except Exception as e:
            self._log(f"❌  저장 실패: {e}")
            return None


if __name__ == "__main__":
    multiprocessing.freeze_support()
    App().mainloop()
