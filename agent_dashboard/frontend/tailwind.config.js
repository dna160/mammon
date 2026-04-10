/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{js,jsx}'],
  theme: {
    extend: {
      colors: {
        mammon: {
          bg:      '#0D1117',
          card:    '#161B22',
          hover:   '#21262D',
          border:  '#30363D',
          text:    '#C9D1D9',
          muted:   '#8B949E',
          accent:  '#58A6FF',
          green:   '#3FB950',
          red:     '#F85149',
          yellow:  '#D29922',
          engineA: '#F97316',
          engineB: '#A78BFA',
          engineC: '#34D399',
        },
      },
      fontFamily: {
        mono: ['JetBrains Mono', 'Fira Code', 'Consolas', 'monospace'],
      },
    },
  },
  plugins: [],
};
