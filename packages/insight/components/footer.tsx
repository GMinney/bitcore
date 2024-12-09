'use client';

import styled, { useTheme } from 'styled-components';
import ThoughtLogoDark from '@/assets/images/thoughtnetwork-logo-blue.svg';
import ThoughtLogoLight from '@/assets/images/thoughtnetwork-logo-white.svg';
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

const ThoughtLink = styled.div`
  height: 25px;

  &:hover {
    cursor: pointer;
  }
`;

const Footer = () => {
  const theme = useTheme();

  return (
    <FooterDiv>
      <ThoughtLink>


        <div style={{ width: '89px', height: '25px', position: 'relative' }}>
          <Image
            src={theme.dark ? ThoughtLogoDark.src : ThoughtLogoLight.src}
            alt='Thought logo'
            onClick={() => window.open('https://thought.live', '_blank')}
            fill={true}
          />
        </div>

      </ThoughtLink>

      <Version>v9.0.0</Version>
    </FooterDiv>
  );
};

export default memo(Footer);
