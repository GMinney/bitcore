import { useDispatch, useSelector, useStore } from 'react-redux';
import { UnknownAction } from 'redux';
import { ThunkDispatch } from 'redux-thunk';
import { RootState, AppStore } from '../../store';

export type AppDispatch = ThunkDispatch<RootState, unknown, UnknownAction>;

export const useAppSelector = useSelector.withTypes<RootState>();
export const useAppStore = useStore.withTypes<AppStore>();
export const useAppDispatch = useDispatch.withTypes<AppDispatch>();
