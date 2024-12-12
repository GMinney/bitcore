/* eslint-disable @typescript-eslint/no-explicit-any */
import styled, { css } from 'styled-components';
import { device, size } from '@/lib/utilities/constants';
import { motion } from 'framer-motion';
import { Action, Error, LightBlack, NeutralSlate, Slate, Slate30, Warning, White } from './colors';

export const TransactionTile = styled(motion.div)`
  background-color: ${({ theme: { dark } }) => (dark ? LightBlack : NeutralSlate)};
  margin: 0.5rem auto;
  padding: 1rem;
`;

export const TxsPlusSign = styled.span`
  width: 20px;
  height: 20px;
  border-radius: 25px;
  background-color: ${({ theme: { dark } }) => (dark ? '#0F0F0F' : '#1A1A1A')};
  text-align: center;
  line-height: 20px;
  color: ${Slate};
  font-size: 20px;
  margin-right: 0.5rem;
  display: inline-block;

  &:hover {
    cursor: pointer;
  }
`;

export const TransactionTileHeader = styled.div`
  display: flex;
`;

export const TransactionTileBody = styled.div`
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  flex-wrap: wrap;
  margin: 1rem 0;
`;

export const TransactionTileFlex = styled.div.attrs<{
  $justifyContent?: string;
}>((props) => ({
  $justifyContent: props.$justifyContent,
}))`
  display: flex;
  justify-content: ${(props) => props.$justifyContent || 'space-between'};
  flex-wrap: wrap;
  align-items: center;

  @media screen and (max-width: ${size.tablet}) {
    justify-content: center;
  }
`;

export enum Type {
  Five = 41.66667,
  One = 8.33333,
  Six = 50,
  Three = 25,
  Nine = 75,
  Twelve = 100,
}

export const TransactionBodyCol = styled.div.attrs<{
  $type?: any;
  $textAlign?: string;
  $backgroundColor?: string;
  $textTAlign?: string;
  $padding?: string;
}>((props) => ({
  $type: props.$type,
  $textAlign: props.$textAlign,
  $backgroundColor: props.$backgroundColor,
  $textTAlign: props.$textTAlign,
  $padding: props.$padding,
}))`
  width: 100%;
  max-width: 100%;
  flex: 0 0 100%;
  padding: ${(props) => props.$padding || '1rem'};
  text-align: ${(props) => props.$textAlign || 'left'};
  background-color: ${({ $backgroundColor, theme: { dark } }) => $backgroundColor || (dark ? '#303030' : Slate30)};

  @media screen and ${device.tablet} {
    ${(props) =>
      props.$textTAlign &&
      css`
        text-align: ${props.$textTAlign};
      `};
    flex: 0 0 ${(props) => Type[props.$type]}%;
    width: ${(props) => Type[props.$type]}%;
    max-width: ${(props) => Type[props.$type]}%;
  }
}
`;

export const TransactionChip = styled.div.attrs<{
  $primary?: any;
  $warning?: any;
  $error?: any;
  $margin?: any;
  $errorText?: any;
}>((props) => ({
  $primary: props.$primary,
  $warning: props.$warning,
  $error: props.$error,
  $margin: props.$margin,
  $errorText: props.$errorText,
}))`
  padding: 0.5rem 1rem;
  background-color: ${({ theme: { dark } }) => (dark ? '#303030' : Slate30)};
  font-size: 16px;
  margin: ${(props) => props.$margin || 0};
  text-align: center;
  height: 2.5rem;

  ${(props) => {
    if (props.$primary) {
      return css`
        color: ${White};
        background-color: ${Action};
      `;
    }

    if (props.$warning) {
      return css`
        color: ${White};
        background-color: ${Warning};
      `;
    }

    if (props.$error) {
      return css`
        color: ${White};
        background-color: ${Error};
      `;
    }

    if (props.$errorText) {
      return css`
        color: ${Error};
      `;
    }
  }};

  @media screen and (max-width: ${size.laptop}) {
    max-width: 150px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  @media screen and (max-width: ${size.tablet}) {
    font-size: 14px;
    width: 175px;
    margin: 0.5rem;
  }

  @media screen and (max-width: ${size.mobileL}) {
    margin: 0.5rem 0;
  }
`;

export const ArrowDiv = styled.div.attrs<{
  $margin: string;
}>((props) => ({
  $margin: props.$margin,
}))`
  width: 25px;
  position: relative;
  margin: ${(props) => props.$margin};

  img {
    cursor: pointer;
  }
`;

export const ScriptText = styled.p`
  margin: 0.2rem 0;
  white-space: normal;
  word-wrap: break-word;
`;

export const SpanLink = styled.span`
  &:hover {
    cursor: pointer;
  }
`;
