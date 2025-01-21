'use strict';

var chai = require('chai');
var Net = require('net');
var Socks5Client = require('socks5-client');
var Socks5ClientSocket = Socks5Client.Socket;

/* jshint unused: false */
var should = chai.should;
var expect = chai.expect;
var sinon = require('sinon');
var fs = require('fs');

var thoughtcore = require('thoughtcore-lib');
var _ = thoughtcore.deps._;
var P2P = require('../');
var Peer = P2P.Peer;
var EventEmitter = require('events').EventEmitter;
var Messages = P2P.Messages;
var messages = new Messages();
var Networks = thoughtcore.Networks;

describe('Peer', function () {

  describe('Integration test', function () {
    it('parses this stream of data from a connection', function (callback) {
      var peer = new Peer('');
      var stub = sinon.stub();
      var dataCallback;
      var connectCallback;
      var expected = {
        version: 1,
        verack: 1,
        inv: 18,
        addr: 4
      };
      var received = {
        version: 0,
        verack: 0,
        inv: 0,
        addr: 0
      };
      stub.on = function () {
        if (arguments[0] === 'data') {
          dataCallback = arguments[1];
        }
        if (arguments[0] === 'connect') {
          connectCallback = arguments[1];
        }
      };
      stub.write = function () { };
      stub.connect = function () {
        connectCallback();
      };
      peer._getSocket = function () {
        return stub;
      };
      peer.on('connect', function () {
        dataCallback(fs.readFileSync('./test/data/connection.log'));
      });
      var check = function (message) {
        received[message.command]++;
        if (_.isEqual(received, expected)) {
          callback();
        }
      };
      peer.on('version', check);
      peer.on('verack', check);
      peer.on('addr', check);
      peer.on('inv', check);
      peer.connect();
    });
  });

  it('create instance', function () {
    var peer = new Peer('localhost');
    expect(peer.host).to.equal('localhost');
    expect(peer.network).to.equal(Networks.livenet);
    expect(peer.port).to.equal(Networks.livenet.port);
  });

  it('create instance setting a port', function () {
    var peer = new Peer({ host: 'localhost', port: 10618 });
    expect(peer.host).to.equal('localhost');
    expect(peer.network).to.equal(Networks.livenet);
    expect(peer.port).to.equal(10618);
  });

  it('create instance setting a network', function () {
    var peer = new Peer({ host: 'localhost', network: Networks.testnet });
    expect(peer.host).to.equal('localhost');
    expect(peer.network.name).to.equal(Networks.testnet);
    // Default testnet doesn't have a port, it is contained in the variants properties
    // peer.port.to.equal(Networks.testnet.port);
  });

  it('create instance setting a network from string', function () {
    var peer = new Peer({ host: 'localhost', network: 'testnet' });
    expect(peer.host).to.equal('localhost');
    expect(peer.network.name).to.equal(Networks.testnet);
    // Default testnet doesn't have a port, it is contained in the variants properties
    // peer.port.to.equal(Networks.testnet.port);
  });

  it('create instance setting a network from xpubkey', function () {
    var peer = new Peer({ host: 'localhost', network: 0x043587cf });
    expect(peer.host).to.equal('localhost');
    expect(peer.network.name).to.equal(Networks.regtest);
    // Default testnet doesn't have a port, it is contained in the variants properties
    // peer.port.to.equal(Networks.testnet.port);
  });

  it('create instance setting a custom network', function () {
    const customNetwork = new class Network { constructor(port, networkMagic) { this.port = port; this.networkMagic = networkMagic } }(1234, 0x1234567);
    var peer = new Peer({ host: 'localhost', network: customNetwork });
    expect(peer.host).to.equal('localhost');
    expect(peer.network.name).to.equal(customNetwork.network);
    expect(peer.port).to.equal(customNetwork.port);
  });

  it('create instance setting port and network', function () {
    var peer = new Peer({ host: 'localhost', port: 8111, network: Networks.testnet });
    expect(peer.host).to.equal('localhost');
    expect(peer.network.name).to.equal(Networks.testnet);
    expect(peer.port).to.equal(8111);
  });

  it('create instance without new', function () {
    var peer = Peer({ host: 'localhost', port: 8111, network: Networks.testnet });
    expect(peer.host).to.equal('localhost');
    expect(peer.network.name).to.equal(Networks.testnet);
    expect(peer.port).to.equal(8111);
  });

  it('set a proxy', function () {
    var peer, peer2, socket;

    peer = new Peer('localhost');
    expect(peer.proxy).to.be.an('undefined');
    socket = peer._getSocket();
    socket.should.to.be.instanceof(Net.Socket);

    peer2 = peer.setProxy('127.0.0.1', 9050);
    expect(peer2.proxy.host).to.equal('127.0.0.1');
    expect(peer2.proxy.port).to.equal(9050);
    socket = peer2._getSocket();
    expect(socket).to.be.an.instanceof(Socks5ClientSocket);

    peer.to.equal(peer2);
  });

  it('send pong on ping', function (done) {
    var peer = new Peer({ host: 'localhost' });
    var pingMessage = messages.Ping();
    peer.sendMessage = function (message) {
      expect(message.command).to.equal('pong');
      expect(message.nonce).to.equal(pingMessage.nonce);
      done();
    };
    peer.emit('ping', pingMessage);
  });

  it('relay error from socket', function (done) {
    var peer = new Peer({ host: 'localhost' });
    var socket = new EventEmitter();
    socket.connect = sinon.spy();
    socket.destroy = sinon.spy();
    peer._getSocket = function () {
      return socket;
    };
    var error = new Error('error');
    peer.on('error', function (err) {
      expect(err).to.equal(error);
      done();
    });
    peer.connect();
    peer.socket.emit('error', error);
  });

  it('will not disconnect twice on disconnect and error', function (done) {
    var peer = new Peer({ host: 'localhost' });
    var socket = new EventEmitter();
    socket.connect = sinon.stub();
    socket.destroy = sinon.stub();
    peer._getSocket = function () {
      return socket;
    };
    peer.on('error', sinon.stub());
    peer.connect();
    var called = 0;
    peer.on('disconnect', function () {
      called++;
      called.should.not.be.above(1);
      done();
    });
    peer.disconnect();
    peer.socket.emit('error', new Error('fake error'));
  });

  it('disconnect with max buffer length', function (done) {
    var peer = new Peer({ host: 'localhost' });
    var socket = new EventEmitter();
    socket.connect = sinon.spy();
    peer._getSocket = function () {
      return socket;
    };
    peer.disconnect = function () {
      done();
    };
    peer.connect();
    var buffer = Buffer.from(Array(Peer.MAX_RECEIVE_BUFFER + 1));
    peer.socket.emit('data', buffer);

  });

  it('should send version on version if not already sent', function (done) {
    var peer = new Peer({ host: 'localhost' });
    var commands = {};
    peer.sendMessage = function (message) {
      commands[message.command] = true;
      if (commands.verack && commands.version) {
        done();
      }
    };
    peer.socket = {};
    peer.emit('version', {
      version: 'version',
      subversion: 'subversion',
      startHeight: 'startHeight'
    });
  });

  it('should not send version on version if already sent', function (done) {
    var peer = new Peer({ host: 'localhost' });
    peer.versionSent = true;
    var commands = {};
    peer.sendMessage = function (message) {
      expect(message.command).to.not.equal('version');
      done();
    };
    peer.socket = {};
    peer.emit('version', {
      version: 'version',
      subversion: 'subversion',
      startHeight: 'startHeight'
    });
  });

  it('relay set properly', function () {
    var peer = new Peer({ host: 'localhost' });
    expect(peer.relay).to.equal(true);
    var peer2 = new Peer({ host: 'localhost', relay: false });
    expect(peer2.relay).to.equal(false);
    var peer3 = new Peer({ host: 'localhost', relay: true });
    expect(peer3.relay).to.equal(true);
  });

  it('relay setting respected', function () {
    [true, false].forEach(function (relay) {
      var peer = new Peer({ host: 'localhost', relay: relay });
      var peerSendMessageStub = sinon.stub(Peer.prototype, 'sendMessage').callsFake(
        function (message) {
          expect(message.relay).to.equal(relay);
        });
      peer._sendVersion();
      peerSendMessageStub.restore();
    });
  });

});
