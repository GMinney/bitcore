'use client';

import React from 'react';
import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import Layout from './layout';
import Home from '@/app/[...insight]/Home';
import Blocks from '@/app/[...insight]/blocks';
import Block from '@/app/[...insight]/block';
import TransactionHash from '@/app/[...insight]/transaction';
import Address from '@/app/[...insight]/address';
import Search from '@/app/[...insight]/search';

function Body() {
  return (

      <BrowserRouter basename={'/insight'} >
        <Layout>
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
        </Layout>
      </BrowserRouter>

  );
}

export default Body;
