/* eslint-disable @typescript-eslint/no-explicit-any */
import {FC, memo} from 'react';
import Image from 'next/image';
import {buildTime, getApiRoot, getDefaultRefreshInterval} from '@/lib/utilities/helper-methods';
import styled, {css} from 'styled-components';
import {Tile} from '@/assets/styles/tile';
import LargeThinSpinner from '@/assets/images/large-thin-spinner.svg';
import {Spinner} from '@/assets/styles/spinner';

import {colorCodes} from '@/lib/utilities/constants';
 import {useNavigate} from 'react-router-dom';
import {useApi} from '@/api/api';
import {Error, SlateDark, White} from '@/assets/styles/colors';

import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
} from 'chart.js';
import { Line } from 'react-chartjs-2';
//import { useRouter } from 'next/navigation';

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend
);

const gutter = '1.5rem';

const LightBackground: {[key in string]: string} = {
  BTC: '#FFF1E0',
  BCH: '#EFFFF6',
  ETH: '#EBECF6',
  LTC: '#FAFAFA',
  DOGE: '#FDF8E6',
};

const DarkBackground: {[key in string]: string} = {
  BTC: '#0C0700',
  BCH: '#020A05',
  ETH: '#06070F',
  LTC: '#0A0A0A',
  DOGE: '#0B0903',
};

const CurrencyTileDiv = styled.div.attrs<{ $currency: string}>(props => ({ $currency: props.$currency }))`
  padding: ${gutter};
  text-align: left;
  border-radius: 8px;
  background: ${({ $currency, theme: {dark} }) => dark ? DarkBackground[$currency] : LightBackground[$currency]};
  box-shadow: ${({theme: {dark}}) => (dark ? '0px 5px 20px -5px rgba(0, 0, 0, 0.18)' : 'none')};
  margin-bottom: 2rem;

  &:hover {
    cursor: pointer;
  }
`;

const CurrencyTileHeader = styled.div`
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: ${gutter};
`;

const CurrencyName = styled.p`
  text-align: right;
  font-size: 16px;
  line-height: 18px;
  margin: 0 0 0.1rem 0;
`;

const CurrencyPrice = styled.p`
  font-size: 14px;
  text-align: right;
  margin: 0;
`;

interface CurrencyTileDescProps {
  value?: any;
}

const CurrencyTileDesc = styled.p<CurrencyTileDescProps>`
  margin: 0;
  font-weight: ${({value}) => (value ? 'normal' : '500')};
  font-size: 14px;
  line-height: 27px;
  color: ${({theme: {dark}}) => (dark ? White : SlateDark)};
`;

interface PositionDivProps {
  error?: any;
}

const PositionDiv = styled(Spinner)<PositionDivProps>`
  min-height: 200px;
  display: flex;
  justify-content: center;
  align-items: center;
  ${({error}) =>
    error &&
    css`
      color: ${Error};
      font-size: 16px;
    `}
`;

const ChartContainer = styled.div`
  max-height: 100px;
  margin: 2rem -${gutter};
`;

interface CurrencyTileProps {
  currency: string;
}
const CurrencyTile: FC<CurrencyTileProps> = ({currency}) => {
  //const navigate = useNavigate();
  const apiRoot = getApiRoot(currency);
  const refreshInterval = getDefaultRefreshInterval(currency);
  //const router = useRouter();
  const navigate = useNavigate();
  let price;

  const url = `${apiRoot}/${currency}/mainnet/block?limit=1`;
  const {data, error} = useApi(url, {refreshInterval});
  const {data: priceDetails} = useApi(`https://bitpay.com/rates/${currency}/usd`);
  const {data: priceDisplay} = useApi(
    `https://bitpay.com/currencies/prices?currencyPairs=["${currency}:USD"]`,
  );

  if (priceDetails?.data) {
    const {
      data: {rate},
    } = priceDetails;
    price = rate;
  }

  let priceList: any[] = [];
  if (priceDisplay?.data) {
    priceList = priceDisplay.data[0].priceDisplay;
  }

  if (error) {
    return (
      <CurrencyTileDiv $currency={currency}>
        <PositionDiv error>Error getting latest block</PositionDiv>
      </CurrencyTileDiv>
    );
  }

  if (!data) {
    return (
      <CurrencyTileDiv $currency={currency}>
        <PositionDiv>
          <Image src={LargeThinSpinner} height={30} width={30} alt='spinner' />
        </PositionDiv>
      </CurrencyTileDiv>
    );
  }

  const {height, time, transactionCount, size} = data[0];



  const gotoAllBlocks = async () => {
    await navigate(`/${currency}/mainnet/blocks`);
    //router.push(`/insight/${currency}/mainnet/blocks`);
    // return <Navigate to={`/insight/${currency}/mainnet/blocks`} replace={true} />
  };

  const imgSrc = `https://bitpay.com/img/icon/currencies/${currency}.svg`;

  const chartData = {
    labels: priceList,
    datasets: [
      {
        data: priceList,
        fill: false,
        spanGaps: true,
        borderColor: colorCodes[currency],
        borderWidth: 2,
        pointRadius: 0,
      },
    ],
  };

  const options = {
    scales: {
      x: {
        display: false,
      },
      y: {
        display: false,
      },
    },
    plugins: {
      legend: {
        display: false,
      },
    },
    events: [], // don't listen for any default events like mouseover, click, etc.
    responsive: true,
    maintainAspectRatio: false,
    tension: 0.5,
  };

  return (
    <CurrencyTileDiv $currency={currency} onClick={gotoAllBlocks} key={currency}>
      <CurrencyTileHeader>
        <Image src={imgSrc} width={35} height={35} alt={currency + ' logo'} />
        <div>
          <CurrencyName>{currency}</CurrencyName>
          {price && <CurrencyPrice>{price} USD</CurrencyPrice>}
        </div>
      </CurrencyTileHeader>

      {priceList.length > 0 && (
        <ChartContainer>
          <Line
            key={currency}
            data={chartData}
            options={options}
            aria-label='price line chart'
            role='img'
          />
        </ChartContainer>
      )}

      <Tile $padding='0'>
        <CurrencyTileDesc>Height</CurrencyTileDesc>
        <CurrencyTileDesc value>{height}</CurrencyTileDesc>
      </Tile>

      <Tile $padding='0'>
        <CurrencyTileDesc>Mined</CurrencyTileDesc>
        <CurrencyTileDesc value>{buildTime(time)}</CurrencyTileDesc>
      </Tile>

      <Tile $padding='0'>
        <CurrencyTileDesc>Transaction</CurrencyTileDesc>
        <CurrencyTileDesc value>{transactionCount}</CurrencyTileDesc>
      </Tile>

      <Tile $padding='0'>
        <CurrencyTileDesc>Size</CurrencyTileDesc>
        <CurrencyTileDesc value>{size}</CurrencyTileDesc>
      </Tile>
    </CurrencyTileDiv>
  );
};

export default memo(CurrencyTile);
