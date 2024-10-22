import React from 'react';
import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import Layout from './layout';
import Home from '../pages';
import Blocks from '../pages/blocks';
import Block from '../pages/block';
import TransactionHash from '../pages/transaction';
import Address from '../pages/address';
import Search from '../pages/search';

function Body() {
  return (
    <Layout>
      <BrowserRouter basename={'/insight'}>
        <Routes>
          <Route path='/' element={<Home />} />
          <Route path='/:currency/:network/blocks' element={<Blocks />} />
          <Route path='/:currency/:network/block/:block' element={<Block />} />
          <Route path='/:currency/:network/tx/:tx' element={<TransactionHash />} />
          <Route path='/:currency/:network/address/:address' element={<Address />} />
          <Route path='/search' element={<Search />} />
          {/* 404 redirect to home page */}
          <Route path='*' element={<Navigate to='/' />} />
        </Routes>
      </BrowserRouter>

    </Layout>
  );
}

export default Body;
