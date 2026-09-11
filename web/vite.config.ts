import react from '@vitejs/plugin-react'
import { defineConfig } from 'vitest/config'

// https://vite.dev/config/
export default defineConfig({
  // GitHub Pages는 https://<계정>.github.io/route-optimizer/ 아래에 올라간다.
  base: '/route-optimizer/',
  plugins: [react()],
  test: {
    // core/api/io는 순수 로직이라 DOM이 필요 없다. 기본값은 node로 두고,
    // DOM이 필요한 파일(map/screens/components의 .test.tsx)은 파일 맨 위에
    //   // @vitest-environment jsdom
    // 한 줄을 붙여 개별로 올린다. (vitest 5에서 environmentMatchGlobs는 없어졌다.)
    environment: 'node',
    // .tsx 테스트가 조용히 빠지지 않도록 확장자를 둘 다 잡는다.
    include: ['src/**/*.test.{ts,tsx}'],
  },
})
