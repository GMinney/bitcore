'use client';

import styled, { useTheme } from 'styled-components';
import BitPayLogoDark from '@/assets/images/bitpay-logo-blue.svg';
import BitPayLogoLight from '@/assets/images/bitpay-logo-white.svg';
import { Feather, SlateDark, White } from '@/assets/styles/colors';
import { FooterHeight } from '@/assets/styles/global';
import { memo } from 'react';
import Image from 'next/image';

const FooterDiv = styled.div`
  display: flex;
  padding: 1rem calc((100% - 992px) / 4);
  background: ${({ theme: { dark } }) => (dark ? '#090909' : Feather)};
  align-items: center;
  justify-content: space-between;
  height: ${FooterHeight};
  @media screen and (max-width: 992px) {
    padding: 1rem;
  }
`;

const Version = styled.div`
  color: ${({ theme: { dark } }) => (dark ? White : SlateDark)};
  font-size: 16px;
  line-height: 25px;
`;

const BitPayLink = styled.div`
  height: 25px;

  &:hover {
    cursor: pointer;
  }
`;

const Footer = () => {
  const theme = useTheme();

  return (
    <FooterDiv>
      <BitPayLink>


        <div style={{ width: '89px', height: '25px', position: 'relative' }}>
          <Image
            src={theme.dark ? BitPayLogoDark.src : BitPayLogoLight.src}
            alt='BitPay logo'
            onClick={() => window.open('https://bitpay.com', '_blank')}
            fill={true}
          />
        </div>

      </BitPayLink>

      <Version>v9.0.0</Version>
    </FooterDiv>
  );
};

export default memo(Footer);
