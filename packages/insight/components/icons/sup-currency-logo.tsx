import styled from 'styled-components';
import { motion, useAnimation } from 'framer-motion';
import { imageFadeIn } from '@/lib/utilities/animations';
import Image from 'next/image';

const CurrencyIcon = styled(motion.sup)`
  margin-left: 3px;
`;

const SupCurrencyLogo = ({ currency }: { currency: string }) => {
  const animationControls = useAnimation();
  const imgSrc = `https://thoughtnetwork.com/img/icon/currencies/${currency}.svg`;

  return (
    <CurrencyIcon variants={imageFadeIn} initial='initial' animate={animationControls}>
      <Image
        src={imgSrc}
        width={22}
        height={22}
        alt={currency + ' logo'}
        onLoad={() => animationControls.start('animate')}
      />


    </CurrencyIcon>
  );
};

export default SupCurrencyLogo;
