import _ from "lodash";
import data from "./spec.js";

'use strict';

export class format {
  constructor(message, args) {
    return message
    .replace('{0}', args[0])
    .replace('{1}', args[1])
    .replace('{2}', args[2]);
  }
  traverseNode = function (parent, errorDefinition) {
    var NodeError = function () {
      if (_.isString(errorDefinition.message)) {
        this.message = format(errorDefinition.message, arguments);
      } else if (_.isFunction(errorDefinition.message)) {
        this.message = errorDefinition.message.apply(null, arguments);
      } else {
        throw new Error('Invalid error definition for ' + errorDefinition.name);
      }
      this.stack = this.message + '\n' + (new Error()).stack;
    };
    NodeError.prototype = Object.create(parent.prototype);
    NodeError.prototype.name = parent.prototype.name + errorDefinition.name;
    parent[errorDefinition.name] = NodeError;
    if (errorDefinition.errors) {
      this.childDefinitions(NodeError, errorDefinition.errors);
    }
    return NodeError;
  };
  
  /* jshint latedef: false */
  childDefinitions = function (parent, childDefinitions) {
    _.each(childDefinitions, function (childDefinition) {
      this.traverseNode(parent, childDefinition);
    });
  };
  /* jshint latedef: true */
  
  traverseRoot = function (parent, errorsDefinition) {
    this.childDefinitions(parent, errorsDefinition);
    return parent;
  };
}


// This needs fixed below
var thoughtcore = {};

thoughtcore.Error = function () {
  this.message = 'Internal error';
  this.stack = this.message + '\n' + (new Error()).stack;
};

thoughtcore.Error.prototype = Object.create(Error.prototype);

thoughtcore.Error.prototype.name = 'thoughtcore.Error';

traverseRoot(thoughtcore.Error, data);

export { function (spec) {
      return traverseNode(thoughtcore.Error, spec);
    } };

    // These dipshits are really grabbing their docs from their own domain and parsing them in the runtime
