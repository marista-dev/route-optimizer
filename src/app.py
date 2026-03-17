"""
app.py — 배송 경로 자동 정리 프로그램
비개발자용 Windows GUI  |  GitHub Actions → PyInstaller → .exe 배포

주소 검색: 카카오 우편번호 서비스 (postcode.map.kakao.com)
          pywebview 별도 프로세스로 실행 → tkinter 이벤트루프와 충돌 없음
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
from core.optimizer import build_time_matrix, optimize_route, clear_checkpoint

ctk.set_appearance_mode("light")
ctk.set_default_color_theme("blue")


# ─────────────────────────────────────────────────────────────────────────────
# 카카오 우편번호 서비스 HTML
# embed() 방식 — pywebview WebView2 내에 iframe으로 렌더링
# ─────────────────────────────────────────────────────────────────────────────
_POSTCODE_HTML = """<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { width: 100%; height: 100vh; overflow: hidden; background: #fff; }
  #wrap { width: 100%; height: 100%; }
</style>
<script src="//t1.kakaocdn.net/mapjsapi/bundle/postcode/prod/postcode.v2.js"></script>
</head>
<body>
<div id="wrap"></div>
<script>
new kakao.Postcode({
  oncomplete: function(data) {
    // 사용자가 선택한 타입(도로명/지번)에 따라 주소 결정
    var addr = (data.userSelectedType === 'R') ? data.roadAddress : data.jibunAddress;
    // 법정동명 + 건물명(공동주택) 참고 항목 조합
    var extra = '';
    if (data.userSelectedType === 'R') {
      if (data.bname !== '' && /[동로가]$/.test(data.bname)) extra += data.bname;
      if (data.buildingName !== '' && data.apartment === 'Y')
        extra += (extra !== '' ? ', ' : '') + data.buildingName;
      if (extra !== '') extra = ' (' + extra + ')';
    }
    window.pywebview.api.on_select(addr + extra, data.zonecode);
  },
  width  : '100%',
  height : '100%'
}).embed(document.getElementById('wrap'));
</script>
</body>
</html>"""


# ─────────────────────────────────────────────────────────────────────────────
# multiprocessing 워커 — 반드시 모듈 최상위에 위치 (pickle 직렬화 필요)
# tkinter 이벤트루프와 분리된 별도 프로세스에서 pywebview 실행
# ─────────────────────────────────────────────────────────────────────────────
def _postcode_worker(queue, title):
    """카카오 우편번호 서비스 웹뷰를 별도 프로세스에서 실행."""
    import pywebview
    _result = {}

    class _Api:
        def on_select(self, address, zonecode):
            _result['address'] = address
            _result['zonecode'] = zonecode
            _win.destroy()

    _api = _Api()
    _win  = pywebview.create_window(
        title, html=_POSTCODE_HTML, js_api=_api,
        width=520, height=620, on_top=True)
    pywebview.start()
    queue.put(_result)


def _open_postcode(title="주소 검색"):
    """
    백그라운드에서 _postcode_worker 프로세스를 실행하고 결과를 반환.
    호출 스레드를 블록하므로 반드시 백그라운드 스레드에서 호출해야 함.
    반환: {'address': str, 'zonecode': str} or {}
    """
    q = multiprocessing.Queue()
    p = multiprocessing.Process(target=_postcode_worker, args=(q, title))
    p.start()
    p.join()
    return q.get() if not q.empty() else {}


# ─────────────────────────────────────────────────────────────────────────────
# 출발지 주소 찾기 팝업
# ─────────────────────────────────────────────────────────────────────────────
class AddressSearchDialog(ctk.CTkToplevel):
    """
    카카오 우편번호 서비스로 출발지 주소 검색.
    선택 후 Kakao Local API로 위도/경도 추출.
    """
    def __init__(self, parent, headers: dict):
        super().__init__(parent)
        self.headers = headers
        self.result  = None   # {'address', 'lat', 'lon'}

        self.title("출발 창고/사무실 주소 찾기")
        self.geometry("480x300")
        self.resizable(False, False)
        self.grab_set()
        self._build()

    def _build(self):
        ctk.CTkLabel(self,
                     text="카카오 우편번호 검색으로 출발지 주소를 찾습니다",
                     font=ctk.CTkFont(size=13, weight="bold"),
                     wraplength=440).pack(pady=(24, 6), padx=20)

        ctk.CTkLabel(self,
                     text="아파트·건물명으로도 검색 가능합니다",
                     font=ctk.CTkFont(size=11), text_color="gray").pack()

        self.search_btn = ctk.CTkButton(
            self, text="🔍  주소 검색 창 열기",
            height=44, width=260,
            font=ctk.CTkFont(size=14, weight="bold"),
            command=self._launch)
        self.search_btn.pack(pady=20)

        self.status = ctk.CTkLabel(
            self, text="", font=ctk.CTkFont(size=12),
            wraplength=440, text_color="#1A5276")
        self.status.pack(pady=(0, 12))

        btn_row = ctk.CTkFrame(self, fg_color="transparent")
        btn_row.pack(fill="x", padx=20, pady=(0, 16))
        self.ok_btn = ctk.CTkButton(
            btn_row, text="✅ 이 주소로 확인",
            height=38, state="disabled",
            command=self._confirm)
        self.ok_btn.pack(side="left", expand=True, padx=(0, 6))
        ctk.CTkButton(
            btn_row, text="닫기", height=38,
            fg_color="gray", hover_color="#555",
            command=self.destroy).pack(side="left", expand=True, padx=(6, 0))

    def _launch(self):
        """백그라운드 스레드에서 우편번호 웹뷰 프로세스 실행."""
        self.search_btn.configure(state="disabled", text="⏳ 검색 창 열리는 중...")
        self.status.configure(text="카카오 우편번호 검색 창이 열립니다...", text_color="gray")
        threading.Thread(target=self._worker, daemon=True).start()

    def _worker(self):
        raw = _open_postcode("출발지 주소 찾기")
        if raw.get('address'):
            geo = geocode(raw['address'], self.headers)
            if geo:
                self.result = {'address': raw['address'],
                               'lat': geo['lat'], 'lon': geo['lon']}
                self.after(0, self._on_success)
            else:
                self.after(0, self._on_fail,
                           f"좌표 변환 실패: {raw['address']}")
        else:
            self.after(0, self._on_cancel)

    def _on_success(self):
        self.status.configure(
            text=f"✅ 선택된 주소: {self.result['address']}",
            text_color="#1A5276")
        self.ok_btn.configure(state="normal")
        self.search_btn.configure(state="normal", text="🔍  주소 검색 창 열기")

    def _on_fail(self, msg):
        self.status.configure(text=f"⚠️ {msg}", text_color="#C0392B")
        self.search_btn.configure(state="normal", text="🔍  주소 검색 창 열기")

    def _on_cancel(self):
        self.status.configure(text="검색을 취소했습니다. 다시 시도해보세요.",
                               text_color="gray")
        self.search_btn.configure(state="normal", text="🔍  주소 검색 창 열기")

    def _confirm(self):
        self.destroy()


# ─────────────────────────────────────────────────────────────────────────────
# 주소 불일치 수정 팝업
# ─────────────────────────────────────────────────────────────────────────────
class AddressFixDialog(ctk.CTkToplevel):
    """
    주소 불일치 감지 시 카카오 우편번호 서비스로 올바른 주소 재검색 + 재검증.
    threading.Event 로 백그라운드 파이프라인과 동기화.
    """
    def __init__(self, parent, headers: dict,
                 name: str, orig: str, rev: str,
                 event: threading.Event, result_holder: dict):
        super().__init__(parent)
        self.headers       = headers
        self.event         = event
        self.result_holder = result_holder

        self.title("⚠️ 주소 확인 필요")
        self.geometry("560x480")
        self.resizable(False, False)
        self.grab_set()
        self.protocol("WM_DELETE_WINDOW", self._skip)
        self._build(name, orig, rev)

    def _build(self, name, orig, rev):
        # 헤더 바
        hdr = ctk.CTkFrame(self, fg_color="#C0392B", corner_radius=0, height=52)
        hdr.pack(fill="x")
        hdr.pack_propagate(False)
        ctk.CTkLabel(hdr, text="⚠️  주소를 확인해주세요",
                     font=ctk.CTkFont(size=15, weight="bold"),
                     text_color="white").pack(expand=True)

        body = ctk.CTkFrame(self, fg_color="transparent")
        body.pack(fill="both", expand=True, padx=22, pady=10)

        ctk.CTkLabel(body, text=f"수령인:  {name}",
                     font=ctk.CTkFont(size=13, weight="bold")).pack(anchor="w", pady=(0, 8))

        for label, text, bg in [
            ("📋 엑셀에 입력된 주소",         orig,                             "#EBF5FB"),
            ("🗺️ 지도에서 확인된 실제 주소",  rev or "(위치를 찾지 못했습니다)", "#FDEDEC"),
        ]:
            ctk.CTkLabel(body, text=label,
                         font=ctk.CTkFont(size=11), text_color="gray").pack(anchor="w")
            ctk.CTkLabel(body, text=text, wraplength=510,
                         font=ctk.CTkFont(size=12),
                         fg_color=bg, corner_radius=6,
                         padx=10, pady=6).pack(fill="x", pady=(2, 10))

        ctk.CTkLabel(body,
                     text="두 주소가 다릅니다. 카카오 우편번호 검색으로 올바른 주소를 찾아주세요.",
                     font=ctk.CTkFont(size=12), text_color="#E74C3C",
                     wraplength=510).pack(anchor="w", pady=(0, 14))

        self.search_btn = ctk.CTkButton(
            body, text="🔍  올바른 주소 검색 창 열기",
            height=42, font=ctk.CTkFont(size=13, weight="bold"),
            command=self._launch)
        self.search_btn.pack(fill="x", pady=(0, 8))

        self.verify_lbl = ctk.CTkLabel(
            body, text="", font=ctk.CTkFont(size=12),
            wraplength=510)
        self.verify_lbl.pack(anchor="w", pady=(0, 10))

        btn_row = ctk.CTkFrame(body, fg_color="transparent")
        btn_row.pack(fill="x")
        self.ok_btn = ctk.CTkButton(
            btn_row, text="✅ 이 주소로 수정",
            height=40, state="disabled", command=self._confirm)
        self.ok_btn.pack(side="left", expand=True, padx=(0, 6))
        ctk.CTkButton(btn_row, text="원본 그대로 사용",
                       height=40, fg_color="gray", hover_color="#555",
                       command=self._skip).pack(side="left", expand=True, padx=(6, 0))

        self._pending = None

    def _launch(self):
        self.search_btn.configure(state="disabled", text="⏳ 검색 창 열리는 중...")
        self.verify_lbl.configure(text="카카오 우편번호 검색 창이 열립니다...", text_color="gray")
        threading.Thread(target=self._worker, daemon=True).start()

    def _worker(self):
        raw = _open_postcode("올바른 주소 찾기")
        if raw.get('address'):
            geo = geocode(raw['address'], self.headers)
            if geo:
                rev     = reverse_geocode(geo['lat'], geo['lon'], self.headers)
                verdict = verify_address(raw['address'], rev)
                self._pending = {
                    'address': raw['address'],
                    'lat':     geo['lat'],
                    'lon':     geo['lon'],
                    'reverse': rev,
                    'verdict': verdict
                }
                self.after(0, self._on_success, verdict, rev)
            else:
                self.after(0, self._on_fail, raw['address'])
        else:
            self.after(0, self._on_cancel)

    def _on_success(self, verdict, rev):
        addr = self._pending['address']
        if verdict == '일치':
            self.verify_lbl.configure(
                text=f"✅ 재검증 완료: {addr}", text_color="#1A8A1A")
        else:
            self.verify_lbl.configure(
                text=f"⚠️ 선택된 주소: {addr}\n   (지도 주소: {rev} — 확인 권장)",
                text_color="#E67E22")
        self.ok_btn.configure(state="normal")
        self.search_btn.configure(state="normal", text="🔍  올바른 주소 검색 창 열기")

    def _on_fail(self, address):
        self.verify_lbl.configure(
            text=f"⚠️ 좌표 변환 실패: {address}", text_color="#C0392B")
        self.search_btn.configure(state="normal", text="🔍  올바른 주소 검색 창 열기")

    def _on_cancel(self):
        self.verify_lbl.configure(
            text="검색을 취소했습니다. 다시 시도해보세요.", text_color="gray")
        self.search_btn.configure(state="normal", text="🔍  올바른 주소 검색 창 열기")

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
# 완료 팝업 — 저장 위치 + 폴더 열기 버튼
# ─────────────────────────────────────────────────────────────────────────────
class DoneDialog(ctk.CTkToplevel):
    def __init__(self, parent, output_path: str, warn_cnt: int):
        super().__init__(parent)
        self.title("✅ 작업 완료")
        self.geometry("500x280")
        self.resizable(False, False)
        self.grab_set()

        folder = os.path.dirname(output_path)
        fname  = os.path.basename(output_path)

        ctk.CTkLabel(self, text="🎉  배송 순서 정리 완료!",
                     font=ctk.CTkFont(size=16, weight="bold")).pack(pady=(24, 6))

        ctk.CTkLabel(self, text="저장된 파일",
                     font=ctk.CTkFont(size=11), text_color="gray").pack()
        ctk.CTkLabel(self, text=fname,
                     font=ctk.CTkFont(size=13, weight="bold"),
                     text_color="#1A5276").pack(pady=(2, 4))

        ctk.CTkLabel(self, text="저장 위치",
                     font=ctk.CTkFont(size=11), text_color="gray").pack()
        ctk.CTkLabel(self, text=folder, wraplength=460,
                     font=ctk.CTkFont(size=11)).pack(pady=(2, 12))

        if warn_cnt:
            ctk.CTkLabel(self,
                         text=f"⚠️  주소 미수정 항목 {warn_cnt}건 — '주소검증결과' 열 확인 권장",
                         font=ctk.CTkFont(size=11), text_color="#E67E22",
                         wraplength=460).pack(pady=(0, 10))

        btn_row = ctk.CTkFrame(self, fg_color="transparent")
        btn_row.pack(fill="x", padx=30, pady=(0, 20))
        ctk.CTkButton(btn_row, text="📂 저장 폴더 열기", height=40,
                      command=lambda: self._open_folder(folder)).pack(
            side="left", expand=True, padx=(0, 6))
        ctk.CTkButton(btn_row, text="닫기", height=40,
                      fg_color="gray", hover_color="#555",
                      command=self.destroy).pack(side="left", expand=True, padx=(6, 0))

    @staticmethod
    def _open_folder(path: str):
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
        self.title("🚚 배송 경로 자동 정리")
        self.geometry("660x800")
        self.resizable(False, False)
        self.file_path = ""
        self.start_lat = None
        self.start_lon = None
        self._build_ui()

    def _build_ui(self):
        hdr = ctk.CTkFrame(self, fg_color="#1A5276", corner_radius=0, height=64)
        hdr.pack(fill="x")
        hdr.pack_propagate(False)
        ctk.CTkLabel(hdr, text="🚚  배송 경로 자동 정리 프로그램",
                     font=ctk.CTkFont(size=18, weight="bold"),
                     text_color="white").pack(expand=True)

        body = ctk.CTkFrame(self, fg_color="transparent")
        body.pack(fill="both", expand=True, padx=28, pady=16)

        # ① API 키
        self._label(body, "① 카카오 API 키")
        self.api_entry = ctk.CTkEntry(
            body, placeholder_text="카카오 REST API 키를 입력하세요",
            height=38, show="*")
        self.api_entry.pack(fill="x", pady=(0, 14))

        # ② 출발지 주소
        self._label(body, "② 출발 창고 / 사무실 주소")
        ar = ctk.CTkFrame(body, fg_color="transparent")
        ar.pack(fill="x")
        self.addr_entry = ctk.CTkEntry(
            ar, placeholder_text="[주소 찾기] 버튼을 눌러 카카오 우편번호로 검색하세요",
            height=38, state="disabled")
        self.addr_entry.pack(side="left", fill="x", expand=True, padx=(0, 8))
        ctk.CTkButton(ar, text="🔍 주소 찾기", width=110, height=38,
                      command=self._find_origin).pack(side="left")
        self.addr_ok = ctk.CTkLabel(body, text="", text_color="#1A5276",
                                     font=ctk.CTkFont(size=11))
        self.addr_ok.pack(anchor="w", pady=(4, 14))

        # ③ 파일
        self._label(body, "③ 배송 목록 엑셀 파일 (.xlsx)")
        fr = ctk.CTkFrame(body, fg_color="transparent")
        fr.pack(fill="x")
        ctk.CTkButton(fr, text="📁 파일 선택", width=110, height=38,
                      command=self._pick_file).pack(side="left", padx=(0, 8))
        self.file_lbl = ctk.CTkLabel(
            fr, text="선택된 파일 없음", text_color="gray",
            font=ctk.CTkFont(size=11), wraplength=430, anchor="w")
        self.file_lbl.pack(side="left", fill="x", expand=True)

        # 실행 버튼
        self.run_btn = ctk.CTkButton(
            body, text="▶  배송 순서 자동 정리 시작",
            height=50, font=ctk.CTkFont(size=15, weight="bold"),
            fg_color="#1A5276", hover_color="#154360",
            command=self._start)
        self.run_btn.pack(fill="x", pady=18)

        # 진행 바
        self.step_lbl = ctk.CTkLabel(body, text="",
                                      font=ctk.CTkFont(size=12, weight="bold"),
                                      text_color="#1A5276")
        self.step_lbl.pack(anchor="w")
        self.bar = ctk.CTkProgressBar(body, height=14)
        self.bar.pack(fill="x", pady=(4, 2))
        self.bar.set(0)
        self.pct_lbl = ctk.CTkLabel(body, text="",
                                     font=ctk.CTkFont(size=11), text_color="gray")
        self.pct_lbl.pack(anchor="e", pady=(0, 10))

        # 로그
        self._label(body, "작업 현황")
        self.log = ctk.CTkTextbox(
            body, height=230,
            font=ctk.CTkFont(family="맑은 고딕", size=11),
            state="disabled", wrap="word")
        self.log.pack(fill="both", expand=True)

    def _label(self, p, t):
        ctk.CTkLabel(p, text=t, font=ctk.CTkFont(size=13, weight="bold")).pack(
            anchor="w", pady=(0, 4))

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
            self.addr_ok.configure(text=f"✅ 확인된 주소: {r['address']}")

    def _pick_file(self):
        p = filedialog.askopenfilename(filetypes=[("Excel 파일", "*.xlsx *.xls")])
        if p:
            self.file_path = p
            self.file_lbl.configure(text=os.path.basename(p), text_color="#1A5276")

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
        self.run_btn.configure(state="disabled", text="⏳ 작업 진행 중...")
        self._clear_log()
        self.bar.set(0)
        threading.Thread(target=self._pipeline, daemon=True).start()

    # ── 로그 / 진행 헬퍼 (스레드 안전) ───────────────────────────────────────
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

    def _step(self, txt, pct):
        self.after(0, self.step_lbl.configure, {"text": txt})
        self.after(0, self.bar.set, pct)
        self.after(0, self.pct_lbl.configure, {"text": f"{int(pct * 100)}%"})

    def _reset_btn(self):
        self.after(0, self.run_btn.configure,
                   {"state": "normal", "text": "▶  배송 순서 자동 정리 시작"})

    def _open_fix(self, headers, name, orig, rev, event, holder):
        AddressFixDialog(self, headers, name, orig, rev, event, holder)

    # ── 파이프라인 ───────────────────────────────────────────────────────────
    def _pipeline(self):
        key     = self.api_entry.get().strip()
        headers = {"Authorization": f"KakaoAK {key}"}

        try:
            # 1단계 — 엑셀 읽기
            self._step("1단계 — 엑셀 파일 읽는 중...", 0.05)
            self._log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
            self._log("1단계: 엑셀 파일 읽기")
            self._log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
            try:
                df = pd.read_excel(self.file_path, header=0)
            except Exception as e:
                self._log(f"❌ 파일 읽기 실패: {e}")
                self._reset_btn()
                return

            if '택배받을 주소' not in df.columns:
                self._log("❌ '택배받을 주소' 열이 없습니다.")
                self._reset_btn()
                return

            df = df.dropna(subset=['택배받을 주소'])
            df['택배받을 주소'] = df['택배받을 주소'].astype(str).str.strip()
            total = len(df)
            self._log(f"✅ {total}건 확인 완료")

            # 2단계 — 위치 확인 (지오코딩)
            self._step("2단계 — 각 배송지 위치 확인 중...", 0.15)
            self._log("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
            self._log("2단계: 각 배송지 위치 확인")
            self._log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
            lats, lons, k_addrs = [], [], []
            for i, (_, row) in enumerate(df.iterrows()):
                r = geocode(row['택배받을 주소'], headers)
                time.sleep(0.15)
                if r:
                    lats.append(r['lat']); lons.append(r['lon'])
                    k_addrs.append(r['kakao_road_addr'])
                    self._log(f"  ✅ ({i+1}/{total}) {row.get('이름','')} — 위치 확인")
                else:
                    lats.append(None); lons.append(None); k_addrs.append('')
                    self._log(f"  ⚠️  ({i+1}/{total}) {row.get('이름','')} — 위치 못 찾음")
                self._step("2단계 — 각 배송지 위치 확인 중...",
                           0.15 + (i + 1) / total * 0.15)

            df['Latitude']      = lats
            df['Longitude']     = lons
            df['카카오_확인주소'] = k_addrs
            self._log(f"\n✅ 2단계 완료 — {sum(1 for v in lats if v)}/{total}건")

            # 3단계 — 주소 정확도 검사 + 불일치 시 우편번호 검색으로 수정
            self._step("3단계 — 주소 정확도 검사 중...", 0.32)
            self._log("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
            self._log("3단계: 주소 정확도 검사")
            self._log("  ⚠️ 불일치 항목은 팝업에서 우편번호 검색으로 수정할 수 있습니다")
            self._log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
            revs, verdicts, fix_cnt = [], [], 0

            for i, (di, row) in enumerate(df.iterrows()):
                lat, lon = row['Latitude'], row['Longitude']
                name     = row.get('이름', f'항목{i+1}')
                if pd.notna(lat) and pd.notna(lon):
                    rev     = reverse_geocode(lat, lon, headers)
                    time.sleep(0.15)
                    verdict = verify_address(row['택배받을 주소'], rev)
                else:
                    rev, verdict = '', '위치없음'

                if verdict not in ('일치', '위치없음', '확인불가'):
                    self._log(f"  ⚠️  ({i+1}/{total}) {name} — 불일치 → 팝업 확인 필요")
                    self._step("⚠️ 불일치 — 팝업에서 올바른 주소를 검색해주세요",
                               0.32 + (i + 1) / total * 0.13)
                    ev, holder = threading.Event(), {}
                    self.after(0, self._open_fix,
                               headers, name, row['택배받을 주소'], rev, ev, holder)
                    ev.wait()
                    if holder:
                        df.at[di, 'Latitude']      = holder['lat']
                        df.at[di, 'Longitude']     = holder['lon']
                        df.at[di, '카카오_확인주소'] = holder['address']
                        rev     = holder.get('reverse', holder['address'])
                        verdict = holder.get('verdict', '수정됨')
                        fix_cnt += 1
                        self._log(f"     → 수정: {holder['address']}")
                    else:
                        self._log("     → 원본 그대로 사용")
                else:
                    icon = "✅" if verdict == '일치' else "➖"
                    self._log(f"  {icon} ({i+1}/{total}) {name} — {verdict}")

                revs.append(rev); verdicts.append(verdict)
                self._step("3단계 — 주소 정확도 검사 중...",
                           0.32 + (i + 1) / total * 0.13)

            df['역지오코딩_주소'] = revs
            df['주소검증결과']   = verdicts
            warn_cnt = sum(1 for v in verdicts
                           if v not in ('일치', '위치없음', '확인불가', '수정됨'))
            self._log(f"\n✅ 3단계 완료"
                      + (f" — {fix_cnt}건 수정, ⚠️ {warn_cnt}건 미수정"
                         if fix_cnt or warn_cnt else " — 모두 정상"))

            # 4단계 — 도로 이동 시간 계산
            self._step("4단계 — 도로 이동 시간 계산 중... (시간 소요)", 0.46)
            self._log("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
            self._log("4단계: 도로 이동 시간 계산  (배송지가 많을수록 시간이 걸립니다)")
            self._log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
            vdf   = df.dropna(subset=['Latitude', 'Longitude']).copy()
            nodes = [{'id': -1, 'name': '출발지',
                      'lat': self.start_lat, 'lon': self.start_lon}]
            for idx, row in vdf.iterrows():
                nodes.append({'id':   idx,
                              'name': row.get('이름', f'배송지{idx}'),
                              'lat':  row['Latitude'],
                              'lon':  row['Longitude']})

            def _prog(done, tot):
                self._step("4단계 — 도로 이동 시간 계산 중...",
                           0.46 + done / tot * 0.24)
                self._log(f"  → {done} / {tot} 경로 계산 완료")

            clear_checkpoint()
            matrix = build_time_matrix(nodes, headers, progress_cb=_prog)
            self._log(f"✅ 4단계 완료 — {len(nodes)}개 지점 완료")

            # 5단계 — 최적 배송 순서
            self._step("5단계 — 최적 배송 순서 계산 중... (최대 3분)", 0.72)
            self._log("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
            self._log("5단계: 최적 배송 순서 계산 (최대 3분)")
            self._log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
            ordered = optimize_route(nodes, matrix)
            if ordered is None:
                self._log("❌ 순서 계산 실패")
                self._reset_btn()
                return
            mapping = {nodes[ni]['id']: step for step, ni in enumerate(ordered, 1)}
            self._log(f"✅ 5단계 완료 — {len(ordered)}건 순서 결정")

            # 6단계 — 결과 저장
            self._step("6단계 — 결과 파일 저장 중...", 0.90)
            self._log("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
            self._log("6단계: 결과 파일 저장")
            self._log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
            out = self._save_xlsx(mapping)
            if out is None:
                self._reset_btn()
                return

            self._step("✅ 모든 작업 완료!", 1.0)
            self._log(f"\n🎉 완료!")
            self._log(f"   파일명: {os.path.basename(out)}")
            self._log(f"   위치:   {os.path.dirname(out)}")
            self.after(0, lambda: DoneDialog(self, out, warn_cnt))

        except Exception as e:
            self._log(f"\n❌ 오류: {e}")
            self.after(0, lambda: messagebox.showerror("오류", f"오류가 발생했습니다:\n{e}"))
        finally:
            self._reset_btn()

    # ── xlsx 저장 ─────────────────────────────────────────────────────────────
    def _save_xlsx(self, mapping: dict):
        """
        원본 xlsx 복사 후 배송순서 열만 업데이트.
        pandas index → xlsx 행번호: df_idx + 2 (헤더 1행 + 0-base 보정)
        """
        try:
            wb   = load_workbook(self.file_path)
            ws   = wb.active
            hrow = [c.value for c in ws[1]]

            if '배송순서' in hrow:
                col = hrow.index('배송순서') + 1
                self._log("   '배송순서' 열 존재 → 덮어씁니다")
            else:
                ws.insert_cols(1)
                ws.cell(row=1, column=1, value='배송순서')
                col = 1
                self._log("   '배송순서' 열 없음 → 첫 번째 열에 추가")

            for df_idx, order_val in mapping.items():
                ws.cell(row=df_idx + 2, column=col, value=order_val)

            base = os.path.splitext(self.file_path)[0]
            out  = f"{base}_배송순서완성.xlsx"
            wb.save(out)
            self._log(f"✅ 저장 완료: {os.path.basename(out)}")
            return out
        except Exception as e:
            self._log(f"❌ 저장 실패: {e}")
            return None


# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # PyInstaller + multiprocessing Windows 필수 설정
    multiprocessing.freeze_support()
    App().mainloop()
