/* eslint-disable @typescript-eslint/no-explicit-any */
import styled from 'styled-components';
import {size} from '@/lib/utilities/constants';

export const Grid = styled.div.attrs<{
  $columns?: any;
  $margin?: any;
}>(props => ({
   $columns: props.$columns,
   $margin: props.$margin
  }))`
  display: grid;
  grid-column-gap: 4%;
  grid-template-columns: repeat(${props => props.$columns || 2}, 48%);
  margin: ${props => props.$margin || 0};

  @media screen and (max-width: ${size.tablet}) {
    grid-template-columns: repeat(${props => props.$columns || 1}, 100%);
  }
`;
