'use client';

import {SUPPORTED_CURRENCIES} from '@/lib/utilities/constants';
import {SecondaryTitle} from '@/assets/styles/titles';
import CurrencyTile from '@/components/currency-tile';
import Masonry from 'react-masonry-css';
import {motion} from 'framer-motion';
import {routerFadeIn} from '@/lib/utilities/animations';
import React, { useEffect } from 'react';
import {useAppDispatch} from '@/lib/utilities/hooks';
import {UnknownAction} from 'redux';
import {ThunkDispatch} from 'redux-thunk';
import {RootState} from '@/lib/store';

import {changeCurrency, changeNetwork} from '@/lib/store/app.actions';

export default function Home() {
  const breakpointColumnsObj = {
    default: 3,
    1200: 2,
    768: 1,
  };

  const dispatch = useAppDispatch<AppDispatch>();

  useEffect(() => {
    dispatch(changeCurrency(''));
    dispatch(changeNetwork(''));
  }, [dispatch]);

  return (
    
    <motion.div variants={routerFadeIn} animate='animate' initial='initial'>
      <SecondaryTitle>Latest Blocks</SecondaryTitle>
      <Masonry
        breakpointCols={breakpointColumnsObj}
        className='currency-masonry-grid'
        columnClassName='currency-masonry-grid_column'>
        {SUPPORTED_CURRENCIES.map(currency => {
          return <CurrencyTile currency={currency} key={currency} />;
        })}
      </Masonry>
    </motion.div>
  );
}

export type AppDispatch = ThunkDispatch<RootState, unknown, UnknownAction>;

