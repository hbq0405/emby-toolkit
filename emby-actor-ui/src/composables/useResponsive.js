import { ref, onMounted, onUnmounted, computed } from 'vue'

const breakpoints = {
  mobile: 480,
  tablet: 768,
  desktop: 1024,
}

function detectDevice() {
  const w = window.innerWidth
  if (w < breakpoints.mobile) return 'phone'
  if (w < breakpoints.tablet) return 'mobile'
  if (w < breakpoints.desktop) return 'tablet'
  return 'desktop'
}

const breakpoint = ref(detectDevice())
const isMobile = ref(breakpoint.value === 'phone' || breakpoint.value === 'mobile')
const isTablet = ref(breakpoint.value === 'tablet')
const isDesktop = ref(breakpoint.value === 'desktop')
const isTouchDevice = ref(false)

let resizeTimer = null
function onResize() {
  clearTimeout(resizeTimer)
  resizeTimer = setTimeout(() => {
    const bp = detectDevice()
    breakpoint.value = bp
    isMobile.value = bp === 'phone' || bp === 'mobile'
    isTablet.value = bp === 'tablet'
    isDesktop.value = bp === 'desktop'
  }, 100)
}

export function useResponsive() {
  const safeAreaBottom = computed(() => {
    if (isMobile.value || isTablet.value) return 'env(safe-area-inset-bottom, 0px)'
    return '0px'
  })

  const contentPadding = computed(() => {
    if (breakpoint.value === 'phone') return '10px'
    if (breakpoint.value === 'mobile') return '12px'
    if (breakpoint.value === 'tablet') return '16px'
    return '24px'
  })

  const contentGap = computed(() => {
    if (breakpoint.value === 'phone') return '12px'
    if (breakpoint.value === 'mobile') return '14px'
    return '20px'
  })

  onMounted(() => {
    isTouchDevice.value = 'ontouchstart' in window || navigator.maxTouchPoints > 0
    window.addEventListener('resize', onResize)
  })

  onUnmounted(() => {
    window.removeEventListener('resize', onResize)
    clearTimeout(resizeTimer)
  })

  return {
    breakpoint,
    isMobile,
    isTablet,
    isDesktop,
    isTouchDevice,
    safeAreaBottom,
    contentPadding,
    contentGap,
  }
}
