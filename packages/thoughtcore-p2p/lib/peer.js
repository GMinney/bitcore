'use strict';

var Buffers = require('./buffers');
var EventEmitter = require('events').EventEmitter;
var Net = require('net');
var Socket = Net.Socket;
var Socks5Client = require('socks5-client');
var thoughtcore = require('thoughtcore-lib');
var Networks = thoughtcore.Networks;
var Messages = require('./messages');
var $ = thoughtcore.util.preconditions;
var util = require('util');
var logger = require('./logger');

/**
 * The Peer constructor will create an instance of Peer to send and receive messages
 * using the standard Thought protocol. A Peer instance represents one connection
 * on the Thought network. To create a new peer connection provide the host and port
 * options and then invoke the connect method. Additionally, a newly connected socket
 * can be provided instead of host and port.
 *
 * @example
 * ```javascript
 *
 * var peer = new Peer({host: '127.0.0.1'}).setProxy('127.0.0.1', 9050);
 * peer.on('tx', function(tx) {
 *  console.log('New transaction: ', tx.id);
 * });
 * peer.connect();
 * ```
 *
 * @param {Object} options
 * @param {String} options.host - IP address of the remote host
 * @param {Number} options.port - Port number of the remote host
 * @param {Network} options.network - The network configuration
 * @param {Boolean=} options.relay - An option to disable automatic inventory relaying from the remote peer
 * @param {Net.Socket=} options.socket - An existing connected socket

 * @returns {Peer} A new instance of Peer.
 * @constructor
 */
function Peer(options) {

  if (!(this instanceof Peer)) {
    return new Peer(options);
  }

  if (options.socket) {
    this.socket = options.socket;
    this.host = this.socket.remoteAddress;
    this.port = this.socket.remotePort;
    this.status = Peer.STATUS.CONNECTED;
    this._addSocketEventHandlers();
  } else {
    this.host = options.host || 'localhost';
    this.status = Peer.STATUS.DISCONNECTED;
    this.port = options.port;
  }

  if (options.network && options.network.constructor.name !== 'Network') {
    options.network = Networks.get(options.network);
  }
  this.network = options.network || Networks.defaultNetwork;

  if (!this.port) {
    this.port = this.network.port;
  }

  this.messages = options.messages || new Messages({
    network: this.network,
    Block: thoughtcore.Block,
    Transaction: thoughtcore.Transaction
  });

  this.dataBuffer = new Buffers();

  this.version = 0;
  this.bestHeight = 0;
  this.subversion = null;
  this.relay = options.relay === false ? false : true;
  this.versionSent = false;
  this.verackSent = false;

  // set message handlers
  var self = this;

  // Receive a verack message and emit a ready event - peerready
  this.on('verack', function () {
    logger.debug(`thoughtcore-p2p verack recv, emitting a ready event`);
    self.status = Peer.STATUS.READY;
    self.emit('ready');
  });

  this.on('version', function (message) {
    self.version = message.version;
    self.subversion = message.subversion;
    self.bestHeight = message.startHeight;

    if (!self.verackSent) {
      self._sendVerack();
    }

    if (!self.versionSent) {
      self._sendVersion();
    }
  });

  this.on('ping', function (message) {
    self._sendPong(message.nonce);
  });

  return this;

}
util.inherits(Peer, EventEmitter);

Peer.MAX_RECEIVE_BUFFER = 10000000;
Peer.STATUS = {
  DISCONNECTED: 'disconnected',
  CONNECTING: 'connecting',
  CONNECTED: 'connected',
  READY: 'ready'
};

/**
 * Set a socks5 proxy for the connection. Enables the use of the TOR network.
 * @param {String} host - IP address of the proxy
 * @param {Number} port - Port number of the proxy
 * @returns {Peer} The same Peer instance.
 */
Peer.prototype.setProxy = function (host, port) {
  $.checkState(this.status === Peer.STATUS.DISCONNECTED);
  logger.debug(`thoughtcore-p2p peer setting proxy...`)
  this.proxy = {
    host: host,
    port: port
  };
  return this;
};

/**
 * Init the connection with the remote peer.
 * @returns {Peer} The same peer instance.
 */
Peer.prototype.connect = function () {
  logger.debug(`thoughtcore-p2p peer connect, getting socket..`);
  this.socket = this._getSocket();
  this.status = Peer.STATUS.CONNECTING;

  var self = this;
  this.socket.on('connect', function (ev) {
    self.status = Peer.STATUS.CONNECTED;
    self.emit('connect');
    logger.debug(`thoughtcore-p2p peer connect, sending version message..`);
    self._sendVersion();
  });

  logger.debug(`thoughtcore-p2p peer: Adding socket event handlers`);

  this._addSocketEventHandlers();

  logger.debug(`thoughtcore-p2p peer: Attempting to connect to ${this.host}:${this.port}`);
  this.socket.connect(this.port, this.host);
  return this;
};

Peer.prototype._addSocketEventHandlers = function () {
  var self = this;

  this.socket.on('error', self._onError.bind(this));

  // This is getting hit
  this.socket.on('end', self.disconnect.bind(this));

  this.socket.on('data', function (data) {
    self.dataBuffer.push(data);

    if (self.dataBuffer.length > Peer.MAX_RECEIVE_BUFFER) {
      // TODO: handle this case better
      return self.disconnect();
    }
    try {
      self._readMessage();
    } catch (e) {
      return self.disconnect();
    }
  });
};

Peer.prototype._onError = function (e) {
  this.emit('error', e);
  if (this.status !== Peer.STATUS.DISCONNECTED) {
    this.disconnect();
  }
};

/**
 * Disconnects the remote connection.
 * @returns {Peer} The same peer instance.
 */
Peer.prototype.disconnect = function () {
  this.status = Peer.STATUS.DISCONNECTED;
  if (this.socket !== undefined) {
    this.socket.destroy;
  }
  this.emit('disconnect');
  return this;
};

/**
 * Send a Message to the remote peer.
 * @param {Message} message - A message instance
 */
Peer.prototype.sendMessage = function (message) {
  this.socket.write(message.toBuffer());
};

/**
 * Internal function that sends VERSION message to the remote peer.
 */
Peer.prototype._sendVersion = function () {
  // todo: include sending local ip address
  var message = this.messages.Version({ relay: this.relay });
  this.versionSent = true;
  this.sendMessage(message);
};

/**
 * Internal function that sends VERSION message to the remote peer.
 */
Peer.prototype._sendVerack = function () {
  // todo: include sending local ip address
  var message = this.messages.Verack();
  this.verackSent = true;
  this.sendMessage(message);
};

/**
 * Send a PONG message to the remote peer.
 */
Peer.prototype._sendPong = function (nonce) {
  var message = this.messages.Pong(nonce);
  this.sendMessage(message);
};

/**
 * Internal function that tries to read a message from the data buffer
 */
Peer.prototype._readMessage = function () {
  logger.debug(`thoughtcore-p2p peer: Parse Buffer Peer.prototype._readMessage`);
  var message = this.messages.parseBuffer(this.dataBuffer);
  logger.debug(`thoughtcore-p2p peer: Peer.prototype._readMessage, message parsed`);
  if (message) {
    logger.debug(`thoughtcore-p2p peer: Peer.prototype._readMessage, message ${message.command}, ${message}`);
    this.emit(message.command, message);
    logger.debug(`thoughtcore-p2p peer: Peer.prototype._readMessage, _readMessage `);
    this._readMessage();
  }
};

/**
 * Internal function that creates a socket using a proxy if necessary.
 * @returns {Net.Socket} A Socket instance not yet connected.
 */
Peer.prototype._getSocket = function () {
  if (this.proxy) {
    return Socks5Client.createConnection(this.proxy.host, this.proxy.port);
  }

  return new Socket();
};

module.exports = Peer;
