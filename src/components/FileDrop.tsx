import { useRef, useState } from 'react';

export interface FileDropProps {
  /** 파일이 선택되거나 떨어졌을 때 */
  onFile: (file: File) => void;
  /** 파싱 중이면 안내 문구를 바꾼다 */
  busy?: boolean;
}

/** xlsx·csv 드롭존. 클릭하면 파일 선택 창이 뜬다. */
export function FileDrop({ onFile, busy }: FileDropProps) {
  const inputRef = useRef<HTMLInputElement>(null);
  const [over, setOver] = useState(false);

  return (
    <>
      <button
        type="button"
        className={`ro-drop${over ? ' is-over' : ''}`}
        // 읽는 중에 또 고르면 두 파싱이 겹쳐 나중 것이 먼저 것을 덮는다.
        disabled={busy}
        onClick={() => inputRef.current?.click()}
        onDragOver={(e) => {
          e.preventDefault();
          if (!busy) setOver(true);
        }}
        onDragLeave={() => setOver(false)}
        onDrop={(e) => {
          e.preventDefault();
          setOver(false);
          if (busy) return;
          const file = e.dataTransfer.files?.[0];
          if (file) onFile(file);
        }}
      >
        <span className="ro-drop__title">
          {busy ? '파일을 읽는 중…' : '파일을 끌어다 놓거나 클릭해서 선택'}
        </span>
        <span className="ro-hint">'택배받을 주소' 열 자동 인식</span>
      </button>
      <input
        ref={inputRef}
        type="file"
        accept=".xlsx,.xls,.csv"
        hidden
        onChange={(e) => {
          const file = e.target.files?.[0];
          if (file) onFile(file);
          e.target.value = '';
        }}
      />
    </>
  );
}
