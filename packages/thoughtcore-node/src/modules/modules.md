# Modules
Modules are loaded before services are started. This allows code to hook into services and register classes, event handlers, etc that alter the behaviors of services.

## Known Modules
The modules in this table will automatically register with `thoughtcore-node` if your `thoughtcore.config.json` contains a valid configuration for their respective chains.

| Chain          | Module         | Module Path (Relative to ModuleManager) |
| -------------- | -------------- | -------------- |
| THT            | thought        | ./thought      |

If there is a custom or third-party module you'd like to use, follow the example below.

## Example - Syncing BCH
Let's say we have a node_module, named `thoughtcore-node-bch` with the following code

```
// index.js

module.exports = class ThoughtCashModule {
  constructor(services) {
    services.Libs.register('BCH', 'thoughtcore-lib-cash', 'thoughtcore-p2p-cash');
    services.P2P.register('BCH', services.P2P.get('THT'));
  }
}
```

The module has the following dependencies
```
// package.json

  "dependencies": {
    "thoughtcore-lib-cash": "^8.3.4",
    "thoughtcore-p2p-cash": "^8.3.4"
  }

```

We could add this module by adding `thoughtcore-node-bch` to the modules array in thoughtcore.config.json

```
    modules: ['./thought', 'thoughtcore-node-bch'],
```
