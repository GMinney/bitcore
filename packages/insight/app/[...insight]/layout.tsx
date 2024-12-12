import type { Metadata } from 'next';
import './globals.css';
import React from 'react';
import StoreProvider from './StoreProvider';

// import NoSSR from './NoSSR';

// const geistSans = localFont({
//   src: './fonts/GeistVF.woff',
//   variable: '--font-geist-sans',
//   weight: '100 900',
// });
// const geistMono = localFont({
//   src: './fonts/GeistMonoVF.woff',
//   variable: '--font-geist-mono',
//   weight: '100 900',
// });

export const metadata: Metadata = {
  title: 'Home | Insight',
  description: 'Thought Insight Blockchain Explorer',
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  //${geistSans.variable} ${geistMono.variable}

  return (
    <html lang='en'>
      <body className={'antialiased'}>
        <StoreProvider>{children}</StoreProvider>
      </body>
    </html>
  );
}
