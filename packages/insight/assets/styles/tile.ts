/* eslint-disable @typescript-eslint/no-explicit-any */
import styled, { css } from 'styled-components';
import { Truncate } from './global';
import { size } from '@/lib/utilities/constants';
import { NeutralSlate, Slate30 } from './colors';

export const Tile = styled.div.attrs<{
  $withBorderBottom?: any;
  $invertedBorderColor?: boolean;
  $margin?: string;
  $padding?: string;
}>((props) => ({
  $withBorderBottom: props.$withBorderBottom,
  $invertedBorderColor: props.$invertedBorderColor,
  $margin: props.$margin,
  $padding: props.$padding,
}))`
  justify-content: space-between;
  display: flex;
  margin: ${({ $margin }) => $margin || 0};
  padding: ${({ $padding }) => $padding || '10px 0'};

  ${({ $withBorderBottom, $invertedBorderColor }) => {
    if ($withBorderBottom) {
      return css`
        border-style: solid;
        border-width: 0 0 1px 0;
        border-color: ${({ theme: { dark } }) => (dark ? '#1F1F1F' : Slate30)};
      `;
    }

    if ($invertedBorderColor) {
      return css`
        border-style: solid;
        border-width: 0 0 1px 0;
        border-color: ${({ theme: { dark } }) => (dark ? '#090909' : NeutralSlate)};
      `;
    }
  }};
`;

export const TileDescription = styled.div.attrs<{
  $value?: any;
  $noTruncate?: any;
  $margin?: string;
  $padding?: string;
  $width?: string;
  $textAlign?: string;
}>((props) => ({
  $value: props.$value,
  $noTruncate: props.$noTruncate,
  $margin: props.$margin,
  $padding: props.$padding,
  $width: props.$width,
  $textAlign: props.$textAlign,
}))`
  ${(props) => {
    if (!props.$noTruncate) {
      return Truncate();
    }
  }}

  font-style: normal;
  font-weight: ${(props) => (props.$value ? 'normal' : '500')};
  font-size: ${(props) => (props.$value ? '16px' : '18px')};
  line-height: 25px;
  white-space: nowrap;
  margin: ${(props) => props.$margin || 0};
  padding: ${(props) => props.$padding || 0};
  width: ${(props) => props.$width || '100%'};
  text-align: ${(props) => props.$textAlign || 'left'};
  display: inline;

  @media screen and (max-width: ${size.mobileL}) {
    font-size: ${(props) => (props.$value ? '14px' : '16px')};
  }
`;

interface TileLinkProps {
  disabled?: boolean;
}

export const TileLink = styled(TileDescription)<TileLinkProps>`
  color: ${({ disabled, theme: { colors } }) => (disabled ? 'inherit' : colors.link)};
  cursor: ${({ disabled }) => (disabled ? 'default' : 'pointer')};
`;
