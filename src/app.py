"""
app.py — 배송 경로 자동 정리 프로그램
비개발자용 Windows GUI  |  GitHub Actions → PyInstaller → .exe 배포
"""

import os
import sys
import threading
import time

import customtkinter as ctk
from tkinter import filedialog, messagebox
import pandas as pd
from openpyxl import load_workbook

# PyInstaller 실행 시 sys._MEIPASS 경로 처리
BASE = getattr(sys, '_MEIPASS', os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE)

from core.geocoder  import geocode, reverse_geocode, verify_address, search_address
from core.optimizer import build_time_matrix, optimize_route, clear_checkpoint

ctk.set_appearance_mode("light")
ctk.set_default_color_theme("blue")


# ─────────────────────────────────────────────────────────────────────────────
# 공용 주소 검색 팝업
# ─────────────────────────────────────────────────────────────────────────────
class AddressSearchDialog(ctk.CTkToplevel):
    def __init__(self, parent, headers: dict,
                 title="주소 찾기", hint=""):
        super().__init__(parent)
        self.headers = headers
        self.result  = None
        self.title(title)
        self.geometry("540x440")
        self.resizable(False, False)
        self.grab_set()
        self._build(hint)

    def _build(self, hint):
        ctk.CTkLabel(self,
                     text=hint or "주소를 입력하고 검색하세요",
                     font=ctk.CTkFont(size=13, weight="bold"),
                     wraplength=490).pack(pady=(18, 8), padx=20)

        row = ctk.CTkFrame(self, fg_color="transparent")
        row.pack(fill="x", padx=20)
        self.entry = ctk.CTkEntry(row,
                                  placeholder_text="예) 광주 북구 ○○로 ○○",
                                  width=340, height=36)
        self.entry.pack(side="left", padx=(0, 8))
        self.entry.bind("<Return>", lambda _: self._search())
        ctk.CTkButton(row, text="🔍 검색", width=90, height=36,
                      command=self._search).pack(side="left")

        self.status = ctk.CTkLabel(self, text="", text_color="gray",
                                   font=ctk.CTkFont(size=11))
        self.status.pack(pady=(6, 2))
        ctk.CTkLabel(self, text="검색 결과 — 클릭하여 선택",
                     font=ctk.CTkFont(size=12)).pack(anchor="w", padx=20)

        import tkinter as tk
        lf = ctk.CTkFrame(self)
        lf.pack(fill="both", expand=True, padx=20, pady=(4, 0))
        self.lb = tk.Listbox(lf, font=("맑은 고딕", 10),
                             selectbackground="#2E86C1",
                             activestyle="none", relief="flat",
                             bd=0, highlightthickness=0)
        self.lb.pack(fill="both", expand=True)
        self.lb.bind("<Double-Button-1>", lambda _: self._confirm())
        self._items = []

        btn_row = ctk.CTkFrame(self, fg_color="transparent")
        btn_row.pack(fill="x", padx=20, pady=12)
        ctk.CTkButton(btn_row, text="✅ 이 주소로 선택", height=38,
                      command=self._confirm).pack(
            side="left", expand=True, padx=(0, 6))
        ctk.CTkButton(btn_row, text="닫기", height=38,
                      fg_color="gray", hover_color="#555",
                      command=self.destroy).pack(
            side="left", expand=True, padx=(6, 0))

    def _search(self):
        q = self.entry.get().strip()
        if not q:
            return
        self.status.configure(text="검색 중...")
        self.update()
        self._items = search_address(q, self.headers)
        self.lb.delete(0, "end")
        if self._items:
            for r in self._items:
                self.lb.insert("end", r['address'])
            self.status.configure(
                text=f"{len(self._items)}건 — 원하는 주소를 선택하세요")
        else:
            self.status.configure(text="결과 없음. 다른 검색어를 시도해보세요.")

    def _confirm(self):
        sel = self.lb.curselection()
        if not sel:
            messagebox.showwarning("선택 필요", "주소를 선택해주세요.", parent=self)
            return
        self.result = self._items[sel[0]]
        self.destroy()


# ─────────────────────────────────────────────────────────────────────────────
# 주소 불일치 수정 팝업
# ─────────────────────────────────────────────────────────────────────────────
class AddressFixDialog(ctk.CTkToplevel):
    """
    백그라운드 스레드가 threading.Event 로 사용자 응답을 기다림.
    result_holder 에 수정된 좌표/주소를 담아 전달.
    """
    def __init__(self, parent, headers: dict,
                 name: str, orig: str, rev: str,
                 event: threading.Event, result_holder: dict):
        super().__init__(parent)
        self.headers       = headers
        self.event         = event
        self.result_holder = result_holder
        self._pending      = None

        self.title("⚠️ 주소 확인 필요")
        self.geometry("580x570")
        self.resizable(False, False)
        self.grab_set()
        self.protocol("WM_DELETE_WINDOW", self._skip)
        self._build(name, orig, rev)

    def _build(self, name, orig, rev):
        # 헤더 바
        hdr = ctk.CTkFrame(self, fg_color="#C0392B",
                           corner_radius=0, height=52)
        hdr.pack(fill="x")
        hdr.pack_propagate(False)
        ctk.CTkLabel(hdr, text="⚠️  주소를 확인해주세요",
                     font=ctk.CTkFont(size=15, weight="bold"),
                     text_color="white").pack(expand=True)

        body = ctk.CTkFrame(self, fg_color="transparent")
        body.pack(fill="both", expand=True, padx=24, pady=10)

        ctk.CTkLabel(body, text=f"수령인:  {name}",
                     font=ctk.CTkFont(size=13, weight="bold")).pack(
            anchor="w", pady=(0, 8))

        for label, text, bg in [
            ("📋 엑셀에 입력된 주소", orig,  "#EBF5FB"),
            ("🗺️ 지도에서 확인된 실제 주소",
             rev or "(위치를 찾지 못했습니다)", "#FDEDEC"),
        ]:
            ctk.CTkLabel(body, text=label,
                         font=ctk.CTkFont(size=11),
                         text_color="gray").pack(anchor="w")
            ctk.CTkLabel(body, text=text, wraplength=520,
                         font=ctk.CTkFont(size=12),
                         fg_color=bg, corner_radius=6,
                         padx=10, pady=6).pack(fill="x", pady=(2, 10))

        ctk.CTkLabel(body,
                     text="두 주소가 다릅니다. 올바른 주소를 검색해 선택해주세요.",
                     font=ctk.CTkFont(size=12),
                     text_color="#E74C3C",
                     wraplength=520).pack(anchor="w", pady=(0, 10))

        ctk.CTkLabel(body, text="올바른 주소 검색",
                     font=ctk.CTkFont(size=12,
                                      weight="bold")).pack(anchor="w")
        sr = ctk.CTkFrame(body, fg_color="transparent")
        sr.pack(fill="x", pady=(4, 0))
        self.s_entry = ctk.CTkEntry(sr,
                                    placeholder_text="주소 입력 후 검색",
                                    height=36, width=360)
        self.s_entry.pack(side="left", padx=(0, 8))
        self.s_entry.bind("<Return>", lambda _: self._search())
        ctk.CTkButton(sr, text="🔍 검색", width=90, height=36,
                      command=self._search).pack(side="left")

        self.s_status = ctk.CTkLabel(body, text="",
                                     text_color="gray",
                                     font=ctk.CTkFont(size=11))
        self.s_status.pack(anchor="w", pady=(4, 2))

        import tkinter as tk
        lf = ctk.CTkFrame(body, height=110)
        lf.pack(fill="x")
        lf.pack_propagate(False)
        self.lb = tk.Listbox(lf, font=("맑은 고딕", 10),
                             selectbackground="#2E86C1",
                             activestyle="none", relief="flat",
                             bd=0, highlightthickness=0)
        self.lb.pack(fill="both", expand=True)
        self.lb.bind("<<ListboxSelect>>", self._on_select)
        self.lb.bind("<Double-Button-1>", lambda _: self._confirm())
        self._items = []

        self.verify_lbl = ctk.CTkLabel(body, text="",
                                        font=ctk.CTkFont(size=12),
                                        wraplength=520)
        self.verify_lbl.pack(anchor="w", pady=(8, 0))

        btn_row = ctk.CTkFrame(body, fg_color="transparent")
        btn_row.pack(fill="x", pady=(10, 0))
        self.ok_btn = ctk.CTkButton(btn_row,
                                     text="✅ 이 주소로 수정",
                                     height=40, state="disabled",
                                     command=self._confirm)
        self.ok_btn.pack(side="left", expand=True, padx=(0, 6))
        ctk.CTkButton(btn_row, text="원본 그대로 사용",
                       height=40, fg_color="gray",
                       hover_color="#555",
                       command=self._skip).pack(
            side="left", expand=True, padx=(6, 0))

    def _search(self):
        q = self.s_entry.get().strip()
        if not q:
            return
        self.s_status.configure(text="검색 중...")
        self.update()
        self._items = search_address(q, self.headers)
        self.lb.delete(0, "end")
        self.verify_lbl.configure(text="")
        self.ok_btn.configure(state="disabled")
        self._pending = None
        if self._items:
            for r in self._items:
                self.lb.insert("end", r['address'])
            self.s_status.configure(
                text=f"{len(self._items)}건 — 선택 후 '이 주소로 수정' 클릭")
        else:
            self.s_status.configure(
                text="결과 없음. 다른 검색어를 시도해보세요.")

    def _on_select(self, _=None):
        sel = self.lb.curselection()
        if not sel:
            return
        chosen = self._items[sel[0]]
        rev    = reverse_geocode(chosen['lat'], chosen['lon'], self.headers)
        verdict = verify_address(chosen['address'], rev)
        if verdict == '일치':
            self.verify_lbl.configure(
                text=f"✅ 재검증 완료: {rev}", text_color="#1A8A1A")
        else:
            self.verify_lbl.configure(
                text=f"⚠️ 재검증: {rev}  (저장되지만 확인 권장)",
                text_color="#E67E22")
        self._pending = {'lat':     chosen['lat'],
                         'lon':     chosen['lon'],
                         'address': chosen['address'],
                         'reverse': rev,
                         'verdict': verdict}
        self.ok_btn.configure(state="normal")

    def _confirm(self):
        if not self._pending:
            messagebox.showwarning(
                "선택 필요", "검색 후 주소를 선택해주세요.", parent=self)
            return
        self.result_holder.update(self._pending)
        self.event.set()
        self.destroy()

    def _skip(self):
        self.result_holder.clear()
        self.event.set()
        self.destroy()


# ─────────────────────────────────────────────────────────────────────────────
# 메인 앱
# ─────────────────────────────────────────────────────────────────────────────
class App(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title("🚚 배송 경로 자동 정리")
        self.geometry("660x800")
        self.resizable(False, False)
        self.file_path    = ""
        self.start_lat    = None
        self.start_lon    = None
        self._build_ui()

    # ── UI ───────────────────────────────────────────────────────────────────
    def _build_ui(self):
        hdr = ctk.CTkFrame(self, fg_color="#1A5276",
                           corner_radius=0, height=64)
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
            body,
            placeholder_text="카카오 REST API 키를 입력하세요",
            height=38, show="*")
        self.api_entry.pack(fill="x", pady=(0, 14))

        # ② 출발지
        self._label(body, "② 출발 창고 / 사무실 주소")
        ar = ctk.CTkFrame(body, fg_color="transparent")
        ar.pack(fill="x")
        self.addr_entry = ctk.CTkEntry(
            ar,
            placeholder_text="[주소 찾기] 버튼을 눌러 검색하세요",
            height=38, state="disabled")
        self.addr_entry.pack(side="left", fill="x",
                             expand=True, padx=(0, 8))
        ctk.CTkButton(ar, text="🔍 주소 찾기",
                      width=110, height=38,
                      command=self._find_origin).pack(side="left")
        self.addr_ok = ctk.CTkLabel(body, text="",
                                     text_color="#1A5276",
                                     font=ctk.CTkFont(size=11))
        self.addr_ok.pack(anchor="w", pady=(4, 14))

        # ③ 파일
        self._label(body, "③ 배송 목록 엑셀 파일 (.xlsx)")
        fr = ctk.CTkFrame(body, fg_color="transparent")
        fr.pack(fill="x")
        ctk.CTkButton(fr, text="📁 파일 선택",
                      width=110, height=38,
                      command=self._pick_file).pack(side="left",
                                                    padx=(0, 8))
        self.file_lbl = ctk.CTkLabel(fr, text="선택된 파일 없음",
                                      text_color="gray",
                                      font=ctk.CTkFont(size=11),
                                      wraplength=430, anchor="w")
        self.file_lbl.pack(side="left", fill="x", expand=True)

        # 실행 버튼
        self.run_btn = ctk.CTkButton(
            body, text="▶  배송 순서 자동 정리 시작",
            height=50,
            font=ctk.CTkFont(size=15, weight="bold"),
            fg_color="#1A5276", hover_color="#154360",
            command=self._start)
        self.run_btn.pack(fill="x", pady=18)

        # 진행 바
        self.step_lbl = ctk.CTkLabel(
            body, text="",
            font=ctk.CTkFont(size=12, weight="bold"),
            text_color="#1A5276")
        self.step_lbl.pack(anchor="w")
        self.bar = ctk.CTkProgressBar(body, height=14)
        self.bar.pack(fill="x", pady=(4, 2))
        self.bar.set(0)
        self.pct_lbl = ctk.CTkLabel(body, text="",
                                     font=ctk.CTkFont(size=11),
                                     text_color="gray")
        self.pct_lbl.pack(anchor="e", pady=(0, 10))

        # 로그
        self._label(body, "작업 현황")
        self.log = ctk.CTkTextbox(
            body, height=230,
            font=ctk.CTkFont(family="맑은 고딕", size=11),
            state="disabled", wrap="word")
        self.log.pack(fill="both", expand=True)

    def _label(self, p, t):
        ctk.CTkLabel(p, text=t,
                     font=ctk.CTkFont(size=13, weight="bold")).pack(
            anchor="w", pady=(0, 4))

    # ── 이벤트 ───────────────────────────────────────────────────────────────
    def _find_origin(self):
        k = self.api_entry.get().strip()
        if not k:
            messagebox.showwarning("API 키 필요",
                                   "먼저 카카오 API 키를 입력해주세요.")
            return
        dlg = AddressSearchDialog(
            self, {"Authorization": f"KakaoAK {k}"},
            title="출발 창고/사무실 주소 찾기")
        self.wait_window(dlg)
        if dlg.result:
            r = dlg.result
            self.start_lat, self.start_lon = r['lat'], r['lon']
            self.addr_entry.configure(state="normal")
            self.addr_entry.delete(0, "end")
            self.addr_entry.insert(0, r['address'])
            self.addr_entry.configure(state="disabled")
            self.addr_ok.configure(
                text=f"✅ 확인된 주소: {r['address']}")

    def _pick_file(self):
        p = filedialog.askopenfilename(
            filetypes=[("Excel 파일", "*.xlsx *.xls")])
        if p:
            self.file_path = p
            self.file_lbl.configure(
                text=os.path.basename(p), text_color="#1A5276")

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
        self.run_btn.configure(state="disabled",
                               text="⏳ 작업 진행 중...")
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
        self.after(0, self.pct_lbl.configure,
                   {"text": f"{int(pct * 100)}%"})

    def _reset_btn(self):
        self.after(0, self.run_btn.configure,
                   {"state": "normal",
                    "text": "▶  배송 순서 자동 정리 시작"})

    # ── 불일치 팝업 (메인 스레드 실행) ────────────────────────────────────────
    def _open_fix(self, headers, name, orig, rev, event, holder):
        AddressFixDialog(self, headers, name, orig, rev, event, holder)

    # ── 파이프라인 ───────────────────────────────────────────────────────────
    def _pipeline(self):
        key     = self.api_entry.get().strip()
        headers = {"Authorization": f"KakaoAK {key}"}

        try:
            # 1단계 ─ 엑셀 읽기
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

            # 2단계 ─ 위치 확인 (지오코딩)
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
                    self._log(f"  ✅ ({i+1}/{total}) "
                              f"{row.get('이름','')} — 위치 확인")
                else:
                    lats.append(None); lons.append(None)
                    k_addrs.append('')
                    self._log(f"  ⚠️  ({i+1}/{total}) "
                              f"{row.get('이름','')} — 위치 못 찾음")
                self._step("2단계 — 각 배송지 위치 확인 중...",
                           0.15 + (i + 1) / total * 0.15)

            df['Latitude']      = lats
            df['Longitude']     = lons
            df['카카오_확인주소'] = k_addrs
            self._log(f"\n✅ 2단계 완료 — "
                      f"{sum(1 for v in lats if v)}/{total}건")

            # 3단계 ─ 주소 정확도 검사 + 불일치 수정
            self._step("3단계 — 주소 정확도 검사 중...", 0.32)
            self._log("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
            self._log("3단계: 주소 정확도 검사")
            self._log("  ⚠️ 불일치 항목은 팝업에서 수정할 수 있습니다")
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
                    self._log(f"  ⚠️  ({i+1}/{total}) {name} "
                              f"— 불일치 → 팝업 확인 필요")
                    self._step(
                        "⚠️ 불일치 — 팝업에서 올바른 주소를 선택해주세요",
                        0.32 + (i + 1) / total * 0.13)
                    ev, holder = threading.Event(), {}
                    self.after(0, self._open_fix,
                               headers, name,
                               row['택배받을 주소'], rev, ev, holder)
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
                           if v not in ('일치','위치없음','확인불가','수정됨'))
            self._log(f"\n✅ 3단계 완료"
                      + (f" — {fix_cnt}건 수정, ⚠️ {warn_cnt}건 미수정"
                         if fix_cnt or warn_cnt else " — 모두 정상"))

            # 4단계 ─ 도로 이동 시간 계산
            self._step("4단계 — 도로 이동 시간 계산 중... (시간 소요)", 0.46)
            self._log("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
            self._log("4단계: 도로 이동 시간 계산")
            self._log("  (배송지가 많을수록 시간이 걸립니다)")
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

            # 5단계 ─ 최적 배송 순서
            self._step("5단계 — 최적 배송 순서 계산 중... (최대 3분)", 0.72)
            self._log("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
            self._log("5단계: 최적 배송 순서 계산 (최대 3분)")
            self._log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
            ordered = optimize_route(nodes, matrix)
            if ordered is None:
                self._log("❌ 순서 계산 실패")
                self._reset_btn()
                return
            mapping = {nodes[ni]['id']: step
                       for step, ni in enumerate(ordered, 1)}
            self._log(f"✅ 5단계 완료 — {len(ordered)}건 순서 결정")

            # 6단계 ─ 결과 저장
            self._step("6단계 — 결과 파일 저장 중...", 0.90)
            self._log("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
            self._log("6단계: 결과 파일 저장")
            self._log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
            out = self._save_xlsx(mapping, df)
            if out is None:
                self._reset_btn()
                return

            self._step("✅ 모든 작업 완료!", 1.0)
            self._log(f"\n🎉 완료!  저장 위치: {out}")
            msg = (f"배송 순서 정리가 완료되었습니다!\n\n저장 위치:\n{out}"
                   + (f"\n\n⚠️ 주소 미수정 {warn_cnt}건 — 파일 확인 권장"
                      if warn_cnt else ""))
            self.after(0, lambda: messagebox.showinfo("작업 완료", msg))

        except Exception as e:
            self._log(f"\n❌ 오류: {e}")
            self.after(0, lambda: messagebox.showerror(
                "오류", f"오류가 발생했습니다:\n{e}"))
        finally:
            self._reset_btn()

    # ── xlsx 저장 ─────────────────────────────────────────────────────────────
    def _save_xlsx(self, mapping: dict, df: pd.DataFrame) -> str | None:
        try:
            wb = load_workbook(self.file_path)
            ws = wb.active
            hrow = [c.value for c in ws[1]]

            if '배송순서' in hrow:
                col = hrow.index('배송순서') + 1
                self._log("   '배송순서' 열 존재 → 덮어씁니다")
            else:
                ws.insert_cols(1)
                ws.cell(row=1, column=1, value='배송순서')
                col = 1
                self._log("   '배송순서' 열 없음 → 첫 번째 열에 추가")

            tmp = pd.read_excel(self.file_path, header=0)
            tmp = tmp.dropna(subset=['택배받을 주소'])
            for xrow, (di, _) in enumerate(tmp.iterrows(), start=2):
                ws.cell(row=xrow, column=col,
                        value=mapping.get(di, ''))

            base = os.path.splitext(self.file_path)[0]
            out  = f"{base}_배송순서완성.xlsx"
            wb.save(out)
            self._log(f"✅ 저장: {os.path.basename(out)}")
            return out
        except Exception as e:
            self._log(f"❌ 저장 실패: {e}")
            return None


# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    App().mainloop()
