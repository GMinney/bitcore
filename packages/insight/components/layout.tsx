'use client';

import Header from './header';
import styled, { useTheme } from 'styled-components';
import Search from './search';
import { device } from '@/lib/utilities/constants';
import { MainTitle } from '@/assets/styles/titles';
import { ReactNode, useState, memo } from 'react';
import Footer from './footer';
import PlusBackgroundDark from '@/assets/images/plus-dark-background.svg';
import PlusBackgroundLight from '@/assets/images/plus-light-background.svg';
import { AnimatePresence, motion } from 'framer-motion';
import { ErrorExitAnime, fadeIn, fadeInTransition } from '@/lib/utilities/animations';
import { Parallax } from 'react-parallax';
import Info from './info';
import { useLocation } from 'react-router-dom';
import { FooterHeight, HeaderHeight } from '@/assets/styles/global';
import { Feather } from '@/assets/styles/colors';

const BodyWrapper = styled.main.attrs<{ $margintop?: boolean }>(props => ({ $margintop: props.$margintop }))`
  padding: 1rem calc((100% - 992px) / 4);
  justify-content: space-between;
  margin-bottom: 1rem;
  margin-top:  ${props => (props.$margintop ? `${HeaderHeight} auto` : '1rem auto')};

  @media screen and (max-width: 992px) {
    padding: 1rem;
  }

  @media screen and ${device.tablet} {
    margin: ${props => (props.$margintop ? `${HeaderHeight} auto` : '1rem auto')};
  }
`;

const MobileSearch = styled.div`
  display: flex;
  width: 100%;
  margin-top: ${HeaderHeight};

  padding: 1rem calc((100% - 992px) / 4);
  @media screen and (max-width: 992px) {
    padding: 1rem;
  }

  @media screen and ${device.tablet} {
    display: none;
  }
`;

const HomePageSearch = styled(motion.div)`
  margin-top: ${HeaderHeight};
  padding: 5rem calc((100% - 992px) / 4) 4rem;
  background-size: 100%;
  @media screen and (max-width: 992px) {
    padding: 2rem 1rem;
  }
`;

const BodyWrapperContent = styled(motion.div)`
  overflow-x: hidden;
`;

const BodyContainer = styled.div`
  min-height: calc(100vh - ${HeaderHeight} - ${FooterHeight});
`;

const ParallaxBackgroundHeight = styled.div`
  min-height: 42vh;

  @media screen and (max-width: 1200px) {
    min-height: auto;
  }
`;

const ParallaxDiv = styled(Parallax)`
  background-color: ${({ theme: { dark } }) => (dark ? '#090909' : Feather)};
`;

const Layout = ({ children }: { children?: ReactNode }) => {
  const theme = useTheme();
  //console.log('theme', theme.dark, PlusBackgroundDark.src);
  const url: string = theme.dark ? PlusBackgroundDark.src : PlusBackgroundLight.src;
  const location = useLocation();

  const [searchError, setSearchError] = useState('');

  const isHomePage = () => {
    return location.pathname === '/';
  };

  const isTestnet = () => {
    const network = location.pathname.split('/')[2]?.toLowerCase();
    return network && network !== 'mainnet';
  };

  const searchAnime = {
    initial: {
      opacity: 0,
    },
    animate: {
      opacity: 1,
      transition: {
        bounce: 0,
        duration: 0.05,
        ease: 'linear',
        staggerChildren: 0.02,
      },
    },
  };


  //       <motion.div variants={fadeIn} animate='animate' initial='initial'></motion.div>
        
  
  return (
    <div>

      <Header setSearchError={setSearchError} />
      <BodyContainer>
        {isHomePage() && (
          <ParallaxDiv bgImage={url} strength={400}>
            <ParallaxBackgroundHeight>
              <HomePageSearch variants={searchAnime} animate='animate' initial='initial'>
                <MainTitle variants={fadeInTransition}>Insight Blockchain Explorer</MainTitle>
                <motion.div variants={fadeInTransition}>
                  <Search setErrorMessage={setSearchError} />
                </motion.div>

                <AnimatePresence>
                  {searchError && (
                    <motion.div variants={ErrorExitAnime} exit='exit'>
                      <Info message={searchError} type={'error'} />
                    </motion.div>
                  )}
                </AnimatePresence>
              </HomePageSearch>
            </ParallaxBackgroundHeight>
          </ParallaxDiv>
        )}

        {!isHomePage() && (
          <MobileSearch>
            <Search id='headerSearch' setErrorMessage={setSearchError} />
          </MobileSearch>
        )}

        <BodyWrapper $margintop={!isHomePage()}>
          <BodyWrapperContent>
            <AnimatePresence>
              {searchError && !isHomePage() && (
                <motion.div variants={ErrorExitAnime} exit='exit'>
                  <Info message={searchError} type={'error'} />
                </motion.div>
              )}
            </AnimatePresence>
            <AnimatePresence>
              {isTestnet() && (
                <Info
                  message={'This is a test network. Testnet currencies have no real-world value.'}
                  type={'info'}
                  textAlign={'center'}
                />
              )}
            </AnimatePresence>
            {children}
          </BodyWrapperContent>
        </BodyWrapper>
      </BodyContainer>

      <motion.div variants={fadeIn} animate='animate' initial='initial'>
        <Footer />
      </motion.div>
    </div>
  );
};
export default memo(Layout);
