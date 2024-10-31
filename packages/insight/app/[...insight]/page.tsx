'use client';

import { SWRConfig } from 'swr';
import { fetcher } from '@/api/api';
import Body from '@/components/body';
import { ThemeProvider } from 'styled-components';
import { BitPayDarkTheme, BitPayLightTheme } from '@/assets/styles/bitpay';
import { GlobalStyles } from '@/assets/styles/global';
import { useAppSelector } from '@/lib/utilities/hooks';
import 'nprogress/nprogress.css';
import nProgress from 'nprogress';
import NoSSR from './NoSSR';

export default function Insight() {
  const theme = useAppSelector(({ APP }) => APP.theme);
  const colorScheme = theme === 'dark' ? BitPayDarkTheme : BitPayLightTheme;
  nProgress.configure({ showSpinner: false });
  return (
      <NoSSR>
      <ThemeProvider theme={colorScheme}>
        <GlobalStyles />

        <SWRConfig
          value={{
            fetcher,
          }}>
          <Body />
        </SWRConfig>
        
      </ThemeProvider>
      </NoSSR>
  );
}


