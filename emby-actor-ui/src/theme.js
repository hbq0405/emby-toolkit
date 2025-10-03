export const themes = {
  // ================= 主题一: 赛博科技 =================
  default: {
    name: '赛博科技',
    light: {
      custom: { '--card-bg-color': 'rgba(255, 255, 255, 0.85)', '--card-border-color': 'rgba(0, 0, 0, 0.1)', '--card-shadow-color': 'rgba(0, 0, 0, 0.08)', '--accent-color': '#007aff', '--accent-glow-color': 'rgba(0, 122, 255, 0.2)', '--text-color': '#1a1a1a' },
      naive: {
        common: { primaryColor: '#007aff', bodyColor: '#f0f2f5' },
        Card: { color: 'rgba(255, 255, 255, 0.85)', borderColor: 'rgba(0, 0, 0, 0.1)' },
        Layout: { siderColor: '#f5f7fa' },
        Menu: { itemTextColor: '#4c5b6a', itemIconColor: '#4c5b6a', itemTextColorHover: 'var(--n-common-primary-color)', itemIconColorHover: 'var(--n-common-primary-color)', itemTextColorActive: 'var(--n-common-primary-color)', itemIconColorActive: 'var(--n-common-primary-color)', itemTextColorActiveHover: 'var(--n-common-primary-color)', itemIconColorActiveHover: 'var(--n-common-primary-color)' }
      }
    },
    dark: {
      custom: { '--card-bg-color': 'rgba(26, 27, 30, 0.7)', '--card-border-color': 'rgba(255, 255, 255, 0.1)', '--card-shadow-color': 'rgba(0, 0, 0, 0.3)', '--accent-color': '#00a1ff', '--accent-glow-color': 'rgba(0, 161, 255, 0.4)', '--text-color': '#ffffff' },
      naive: {
        common: { primaryColor: '#00a1ff', primaryColorHover: '#33b4ff', primaryColorPressed: '#0090e6', primaryColorSuppl: '#00a1ff', bodyColor: '#101014', cardColor: 'rgba(26, 27, 30, 0.7)' },
        Card: { color: 'rgba(26, 27, 30, 0.7)', borderColor: 'rgba(255, 255, 255, 0.1)' },
        Layout: { siderColor: '#101418' },
        Menu: { itemTextColor: '#a8aeb3', itemIconColor: '#a8aeb3', itemTextColorHover: '#ffffff', itemIconColorHover: '#ffffff', itemTextColorActive: 'var(--n-common-primary-color)', itemIconColorActive: 'var(--n-common-primary-color)', itemTextColorActiveHover: 'var(--n-common-primary-color)', itemIconColorActiveHover: 'var(--n-common-primary-color)' },
        Switch: { railColorActive: '#00a1ff' }, Slider: { fillColor: '#00a1ff' }, Checkbox: { colorChecked: '#00a1ff', checkMarkColor: '#ffffff', borderChecked: '#00a1ff' }, Button: { textColorPrimary: '#ffffff' }
      }
    }
  },

  // ================= 主题二: 玻璃拟态 =================
  glass: {
    name: '玻璃拟态',
    light: {
      custom: { '--card-bg-color': 'rgba(255, 255, 255, 0.6)', '--card-border-color': 'rgba(0, 0, 0, 0.1)', '--card-shadow-color': 'rgba(0, 0, 0, 0.1)', '--accent-color': '#e91e63', '--accent-glow-color': 'rgba(233, 30, 99, 0.3)', '--text-color': '#1a1a1a' },
      naive: {
        common: { primaryColor: '#e91e63', bodyColor: '#f0f2f5' },
        Card: { color: 'rgba(255, 255, 255, 0.6)', borderColor: 'rgba(0, 0, 0, 0.1)' },
        Layout: { siderColor: 'rgba(250, 245, 255, 0.7)' },
        Menu: { itemTextColor: '#6c5f78', itemIconColor: '#6c5f78', itemTextColorActive: 'var(--n-common-primary-color)', itemIconColorActive: 'var(--n-common-primary-color)' }
      }
    },
    dark: {
      custom: { '--card-bg-color': 'rgba(30, 30, 30, 0.5)', '--card-border-color': 'rgba(255, 255, 255, 0.15)', '--card-shadow-color': 'rgba(0, 0, 0, 0.4)', '--accent-color': '#c33cff', '--accent-glow-color': 'rgba(195, 60, 255, 0.5)', '--text-color': '#f0f0f0' },
      naive: {
        common: { primaryColor: '#c33cff', primaryColorHover: '#d063ff', primaryColorPressed: '#b623f5', primaryColorSuppl: '#c33cff', bodyColor: '#101014', cardColor: 'rgba(30, 30, 30, 0.5)' },
        Card: { color: 'rgba(30, 30, 30, 0.5)', borderColor: 'rgba(255, 255, 255, 0.15)' },
        Layout: { siderColor: 'rgba(15, 10, 30, 0.6)' },
        Menu: { itemTextColor: '#b0a4c7', itemIconColor: '#b0a4c7', itemTextColorActive: '#ffffff', itemIconColorActive: '#ffffff' },
        Switch: { railColorActive: '#c33cff' }, Slider: { fillColor: '#c33cff' }, Checkbox: { colorChecked: '#c33cff', checkMarkColor: '#ffffff', borderChecked: '#c33cff' }, Button: { textColorPrimary: '#ffffff' }
      }
    }
  },

  // ================= 主题三: 落日浪潮 =================
  synthwave: {
    name: '落日浪潮',
    light: {
      custom: { '--card-bg-color': 'rgba(240, 230, 255, 0.8)', '--card-border-color': 'rgba(228, 90, 216, 0.6)', '--card-shadow-color': 'rgba(0, 0, 0, 0.1)', '--accent-color': '#ff3d8d', '--accent-glow-color': 'rgba(255, 61, 141, 0.4)', '--text-color': '#1a1a1a' },
      naive: {
        common: { primaryColor: '#ff3d8d', bodyColor: '#f0f2f5' },
        Card: { color: 'rgba(240, 230, 255, 0.8)', borderColor: 'rgba(228, 90, 216, 0.6)' },
        Layout: { siderColor: '#f2eaff' },
        Menu: { itemTextColor: '#6c5f78', itemIconColor: '#6c5f78', itemTextColorActive: 'var(--n-common-primary-color)', itemIconColorActive: 'var(--n-common-primary-color)' }
      }
    },
    dark: {
      custom: { '--card-bg-color': 'rgba(29, 15, 54, 0.75)', '--card-border-color': 'rgba(255, 110, 199, 0.5)', '--card-shadow-color': 'rgba(0, 0, 0, 0.5)', '--accent-color': '#00f7ff', '--accent-glow-color': 'rgba(0, 247, 255, 0.6)', '--text-color': '#f5f5f5' },
      naive: {
        common: { primaryColor: '#00f7ff', primaryColorHover: '#33faff', primaryColorPressed: '#00e0e6', primaryColorSuppl: '#00f7ff', bodyColor: '#101014', cardColor: 'rgba(29, 15, 54, 0.75)' },
        Card: { color: 'rgba(29, 15, 54, 0.75)', borderColor: 'rgba(255, 110, 199, 0.5)' },
        Layout: { siderColor: '#160b2f' },
        Menu: { itemTextColor: '#b39ff3', itemIconColor: '#b39ff3', itemTextColorActive: '#ffffff', itemIconColorActive: '#ffffff' },
        Switch: { railColorActive: '#00f7ff' }, Slider: { fillColor: '#00f7ff' }, Checkbox: { colorChecked: '#00f7ff', checkMarkColor: '#000000', borderChecked: '#00f7ff' }, Button: { textColorPrimary: '#000000' }
      }
    }
  },

  // ================= 主题四: 全息机甲 =================
  holo: {
    name: '全息机甲',
    light: {
      custom: { '--card-bg-color': 'rgba(230, 245, 255, 0.85)', '--card-border-color': 'rgba(20, 120, 220, 0.4)', '--card-shadow-color': 'rgba(0, 0, 0, 0.08)', '--accent-color': '#0d6efd', '--accent-glow-color': 'rgba(13, 110, 253, 0.3)', '--text-color': '#061a40' },
      naive: {
        common: { primaryColor: '#0d6efd', bodyColor: '#f0f2f5' },
        Card: { color: 'rgba(230, 245, 255, 0.85)', borderColor: 'rgba(20, 120, 220, 0.4)' },
        Layout: { siderColor: '#e6f7ff' },
        Menu: { itemTextColor: '#061a40', itemIconColor: '#061a40', itemTextColorActive: 'var(--n-common-primary-color)', itemIconColorActive: 'var(--n-common-primary-color)' }
      }
    },
    dark: {
      custom: { '--card-bg-color': 'rgba(10, 25, 47, 0.8)', '--card-border-color': 'rgba(100, 255, 218, 0.3)', '--card-shadow-color': 'rgba(0, 0, 0, 0.4)', '--accent-color': '#64ffda', '--accent-glow-color': 'rgba(100, 255, 218, 0.5)', '--text-color': '#ccd6f6' },
      naive: {
        common: { primaryColor: '#64ffda', primaryColorHover: '#83ffdf', primaryColorPressed: '#4ff0c8', primaryColorSuppl: '#64ffda', bodyColor: '#0a192f', cardColor: 'rgba(10, 25, 47, 0.8)' },
        Card: { color: 'rgba(10, 25, 47, 0.8)', borderColor: 'rgba(100, 255, 218, 0.3)' },
        Layout: { siderColor: '#0a192f' },
        Menu: { itemTextColor: '#8892b0', itemIconColor: '#8892b0', itemTextColorActive: 'var(--n-common-primary-color)', itemIconColorActive: 'var(--n-common-primary-color)' },
        Switch: { railColorActive: '#64ffda' }, Slider: { fillColor: '#64ffda' }, Checkbox: { colorChecked: '#64ffda', checkMarkColor: '#000000', borderChecked: '#64ffda' }, Button: { textColorPrimary: '#000000' }
      }
    }
  },

  // ================= 主题五: 美漫硬派 =================
  comic: {
    name: '美漫硬派',
    light: {
      custom: { '--card-bg-color': '#f0f0f0', '--card-border-color': '#000000', '--card-shadow-color': 'rgba(0, 0, 0, 0.2)', '--accent-color': '#d93025', '--accent-glow-color': 'rgba(217, 48, 37, 0.4)', '--text-color': '#000000' },
      naive: {
        common: { primaryColor: '#d93025', bodyColor: '#f0f0f0' },
        Card: { color: '#f0f0f0', borderColor: '#000000' },
        Layout: { siderColor: '#e6e6e6' },
        Menu: { itemTextColor: '#333333', itemIconColor: '#333333', itemTextColorActive: '#ffffff', itemIconColorActive: '#ffffff', itemTextColorActiveHover: '#ffffff', itemIconColorActiveHover: '#ffffff' }
      }
    },
    dark: {
      custom: { '--card-bg-color': '#2c2c2c', '--card-border-color': '#000000', '--card-shadow-color': 'rgba(0, 0, 0, 0.7)', '--accent-color': '#ffcc00', '--accent-glow-color': 'rgba(255, 204, 0, 0.5)', '--text-color': '#ffffff' },
      naive: {
        common: { primaryColor: '#ffcc00', primaryColorHover: '#ffde5c', primaryColorPressed: '#f0c200', primaryColorSuppl: '#ffcc00', bodyColor: '#1e1e1e', cardColor: '#2c2c2c' },
        Card: { color: '#2c2c2c', borderColor: '#000000' },
        Layout: { siderColor: '#3a3a3a' },
        Menu: { itemTextColor: '#e0e0e0', itemIconColor: '#e0e0e0', itemTextColorActive: '#000000', itemIconColorActive: '#000000', itemTextColorActiveHover: '#000000', itemIconColorActiveHover: '#000000' },
        Switch: { railColorActive: '#ffcc00' }, Slider: { fillColor: '#ffcc00' }, Checkbox: { colorChecked: '#ffcc00', checkMarkColor: '#000000', borderChecked: '#ffcc00' }, Button: { textColorPrimary: '#000000' }
      }
    }
  },

  // ================= 主题六: 森海秘境 =================
  forest: {
    name: '森海秘境',
    light: {
      custom: { '--card-bg-color': 'rgba(255, 255, 255, 0.8)', '--card-border-color': 'rgba(46, 125, 50, 0.2)', '--card-shadow-color': 'rgba(0, 0, 0, 0.08)', '--accent-color': '#2E7D32', '--accent-glow-color': 'rgba(46, 125, 50, 0.3)', '--text-color': '#41444B' },
      naive: {
        common: { primaryColor: '#2E7D32', primaryColorHover: '#388E3C', primaryColorPressed: '#1B5E20', primaryColorSuppl: '#2E7D32', bodyColor: '#F5F5F0' },
        Card: { color: 'rgba(255, 255, 255, 0.8)', borderColor: 'rgba(46, 125, 50, 0.2)' },
        Layout: { siderColor: '#E8E5DA' },
        Menu: { itemTextColor: '#5D6168', itemIconColor: '#5D6168', itemTextColorActive: 'var(--n-common-primary-color)', itemIconColorActive: 'var(--n-common-primary-color)' },
        Switch: { railColorActive: '#2E7D32' }, Slider: { fillColor: '#2E7D32' }, Checkbox: { colorChecked: '#2E7D32', checkMarkColor: '#ffffff', borderChecked: '#2E7D32' }, Button: { textColorPrimary: '#ffffff' }
      }
    },
    dark: {
      custom: { '--card-bg-color': 'rgba(32, 36, 32, 0.75)', '--card-border-color': 'rgba(166, 226, 46, 0.2)', '--card-shadow-color': 'rgba(0, 0, 0, 0.4)', '--accent-color': '#A6E22E', '--accent-glow-color': 'rgba(166, 226, 46, 0.4)', '--text-color': '#E6E6E6' },
      naive: {
        common: { primaryColor: '#A6E22E', primaryColorHover: '#b7f04d', primaryColorPressed: '#95cc2a', primaryColorSuppl: '#A6E22E', bodyColor: '#1a1d1a', cardColor: 'rgba(32, 36, 32, 0.75)' },
        Card: { color: 'rgba(32, 36, 32, 0.75)', borderColor: 'rgba(166, 226, 46, 0.2)' },
        Layout: { siderColor: '#202420' },
        Menu: { itemTextColor: '#a0a79a', itemIconColor: '#a0a79a', itemTextColorActive: 'var(--n-common-primary-color)', itemIconColorActive: 'var(--n-common-primary-color)' },
        Switch: { railColorActive: '#A6E22E' }, Slider: { fillColor: '#A6E22E' }, Checkbox: { colorChecked: '#A6E22E', checkMarkColor: '#000000', borderChecked: '#A6E22E' }, Button: { textColorPrimary: '#000000' }
      }
    }
  },

  // ================= 主题七: 赤色警戒 =================
  alert: {
    name: '赤色警戒',
    light: {
      custom: { '--card-bg-color': 'rgba(255, 255, 255, 0.85)', '--card-border-color': 'rgba(0, 0, 0, 0.1)', '--card-shadow-color': 'rgba(0, 0, 0, 0.1)', '--accent-color': '#F44336', '--accent-glow-color': 'rgba(244, 67, 54, 0.3)', '--text-color': '#222222' },
      naive: {
        common: { primaryColor: '#F44336', primaryColorHover: '#E53935', primaryColorPressed: '#C62828', primaryColorSuppl: '#F44336', bodyColor: '#F8F8F8' },
        Card: { color: 'rgba(255, 255, 255, 0.85)', borderColor: 'rgba(0, 0, 0, 0.1)' },
        Layout: { siderColor: '#FFFFFF' },
        Menu: { itemTextColor: '#555555', itemIconColor: '#555555', itemTextColorActive: 'var(--n-common-primary-color)', itemIconColorActive: 'var(--n-common-primary-color)' },
        Switch: { railColorActive: '#F44336' }, Slider: { fillColor: '#F44336' }, Checkbox: { colorChecked: '#F44336', checkMarkColor: '#ffffff', borderChecked: '#F44336' }, Button: { textColorPrimary: '#ffffff' }
      }
    },
    dark: {
      custom: { '--card-bg-color': 'rgba(25, 25, 25, 0.7)', '--card-border-color': 'rgba(249, 38, 114, 0.25)', '--card-shadow-color': 'rgba(0, 0, 0, 0.5)', '--accent-color': '#F92672', '--accent-glow-color': 'rgba(249, 38, 114, 0.5)', '--text-color': '#CCCCCC' },
      naive: {
        common: { primaryColor: '#F92672', primaryColorHover: '#fc538d', primaryColorPressed: '#f81062', primaryColorSuppl: '#F92672', bodyColor: '#0D0D0D', cardColor: 'rgba(25, 25, 25, 0.7)' },
        Card: { color: 'rgba(25, 25, 25, 0.7)', borderColor: 'rgba(249, 38, 114, 0.25)' },
        Layout: { siderColor: '#141414' },
        Menu: { itemTextColor: '#888888', itemIconColor: '#888888', itemTextColorActive: 'var(--n-common-primary-color)', itemIconColorActive: 'var(--n-common-primary-color)' },
        Switch: { railColorActive: '#F92672' }, Slider: { fillColor: '#F92672' }, Checkbox: { colorChecked: '#F92672', checkMarkColor: '#ffffff', borderChecked: '#F92672' }, Button: { textColorPrimary: '#ffffff' }
      }
    }
  },
};