'use client;'

import {useDispatch} from 'react-redux';
import {UnknownAction} from 'redux';
import {ThunkDispatch} from 'redux-thunk';
import {RootState} from '../../store';

export type AppDispatch = ThunkDispatch<RootState, unknown, UnknownAction>;

export const useAppDispatch = () => {
  return useDispatch<AppDispatch>();
};
