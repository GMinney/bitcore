export const SUPPORTED_CURRENCIES = ['THT', 'ETH'];
// export const SUPPORTED_CURRENCIES = ['THT', 'tTHT'];
export const API_ROOT = 'https://api.thoughtcore.io/api';
export const API_ROOT_ETH = 'https://api-eth.thoughtcore.io/api';
export const ETH_DEFAULT_REFRESH_INTERVAL = 300000;
export const UTXO_DEFAULT_REFRESH_INTERVAL = 600000;
export const COIN = 100000000;
export const DEFAULT_RBF_SEQ_NUMBER = 0xffffffff;

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export const colorCodes: any = {
  ETH: '#6B71D6',
  THT: '#2222FF'
};

// Media breakpoints
export const size = {
  mobileS: '320px',
  mobileM: '375px',
  mobileL: '425px',
  tablet: '768px',
  laptop: '1024px',
  laptopL: '1440px',
  desktop: '2560px',
};

export const device = {
  mobileS: `(min-width: ${size.mobileS})`,
  mobileM: `(min-width: ${size.mobileM})`,
  mobileL: `(min-width: ${size.mobileL})`,
  tablet: `(min-width: ${size.tablet})`,
  laptop: `(min-width: ${size.laptop})`,
  laptopL: `(min-width: ${size.laptopL})`,
  desktop: `(min-width: ${size.desktop})`,
  desktopL: `(min-width: ${size.desktop})`,
};
