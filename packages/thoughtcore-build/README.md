# Thoughtcore Build

**A helper to add tasks to gulp.**

## Getting started

Install with:

```sh
npm install thoughtcore-build
```

And use and require in your gulp file:

```javascript
var gulp = require('gulp');
var thoughtcoreTasks = require('thoughtcore-build');

thoughtcoreTasks('submodule');
gulp.task('default', ['lint', 'test', 'browser', 'coverage']);
```

### Notes

- There's no default task to allow for each submodule to set up their own configuration
- If the module is node-only, avoid adding the browser tasks with:

```javascript
var thoughtcoreTasks = require('thoughtcore-build');
thoughtcoreTasks('submodule', {skipBrowsers: true});
```

## Contributing

See [CONTRIBUTING.md](https://github.com/thoughtnetwork/thoughtcore/blob/master/Contributing.md) on the main thoughtcore repo for information about how to contribute.

## License

Code released under [the MIT license](https://github.com/thoughtnetwork/thoughtcore/blob/master/LICENSE).

Copyright 2013-2019 Thought, Inc. Thoughtcore is a trademark maintained by Thought, Inc.