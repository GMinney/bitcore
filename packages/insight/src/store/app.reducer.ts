import {createSlice, PayloadAction} from '@reduxjs/toolkit';

/**
 * Define types for the state
 */
interface AppState {
  loading: boolean;
  theme: string;
  currency?: string;
  network?: string;
}

/**
 * Define Initial state
 */
const getInitialTheme = (): string => {
  if (typeof window !== 'undefined') {
    const storedTheme = window.localStorage.getItem('theme');
    if (storedTheme) {
      return storedTheme;
    }
    if (window.matchMedia?.('(prefers-color-scheme: dark)').matches) {
      return 'dark';
    }
  }
  return 'light';
};

const initialState: AppState = {
  loading: false,
  theme: getInitialTheme(),
  network: '',
  currency: '',
};

/**
 * Reducer and State modifications
 */
export const appSlice = createSlice({
  name: 'app',
  initialState,
  reducers: {
    // Use the PayloadAction type to declare the contents of `action.payload`
    changeTheme: (state, action: PayloadAction<'dark' | 'light'>) => {
      if (typeof window !== 'undefined') {
        window.localStorage.setItem('theme', action.payload);
      }
      state.theme = action.payload;
    },
    changeNetwork: (state, action: PayloadAction<string>) => {
      state.network = action.payload.toLowerCase();
    },
    changeCurrency: (state, action: PayloadAction<string>) => {
      state.currency = action.payload.toUpperCase();
    },
  },
});

/**
 * Reducer Export
 */
export default appSlice.reducer;
