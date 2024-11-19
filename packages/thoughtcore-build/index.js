'use strict';

var gulp = require('gulp');

//var jshint = require('gulp-jshint');
//var mocha = require('gulp-mocha');
var rename = require('gulp-rename');
//var shell = require('gulp-shell');
var terser = require('gulp-terser');
var exec = require('child_process').exec;
//var bump = require('gulp-bump');
//var git = require('gulp-git');
var fs = require('fs');

function ignoreerror() {
  /* jshint ignore:start */ // using `this` in this context is weird
  this.emit('end');
  /* jshint ignore:end */
}

async function startGulp(name, opts) {
  var mocha = await import('gulp-mocha');

  var task = {};
  opts = opts || {};
  var browser = !opts.skipBrowser;
  var fullname = name ? 'thoughtcore-' + name : 'thoughtcore';
  var files = ['lib/**/*.js'];
  var tests = ['test/**/*.js'];
  var alljs = files.concat(tests);

  var buildPath = './node_modules/thoughtcore-build/';
  var buildModulesPath = buildPath + 'node_modules/';
  var buildBinPath = buildPath + 'node_modules/.bin/';

  var browserifyPath = buildBinPath + 'browserify';
  var karmaPath = buildBinPath + 'karma';
  var platoPath = buildBinPath + 'plato';
  var istanbulPath = buildBinPath + 'istanbul';
  var mochaPath = buildBinPath + '_mocha';

  // newer version of node? binaries are in lower level of node_module path
  if (!fs.existsSync(browserifyPath)) {
    browserifyPath = './node_modules/.bin/browserify';
  }

  if (!fs.existsSync(karmaPath)) {
    karmaPath = './node_modules/.bin/karma';
  }

  if (!fs.existsSync(istanbulPath)) {
    istanbulPath = './node_modules/.bin/istanbul';
  }

  if (!fs.existsSync(platoPath)) {
    platoPath = './node_modules/.bin/plato';
  }

  if (!fs.existsSync(mochaPath)) {
    mochaPath = './node_modules/.bin/_mocha';
  }

  /**
   * testing
   */
  var testmocha = function () {
    return gulp.src(tests).pipe(mocha({
      reporter: 'spec'
    }));
  };

  // task['test:karma'] = shell.task([
  //   karmaPath + '  start ' + buildPath + 'karma.conf.js --single-run '
  // ]);

  task['test:karma'] = function () {
    let command = karmaPath + '  start ' + buildPath + 'karma.conf.js --single-run '
    exec($({command}), function (err, stdout, stderr) {
      console.log(stdout);
      console.log(stderr);
      //console.log(err)
    });
  };

  task['test:node'] = testmocha;
  task['test:node:nofail'] = function () {
    return testmocha().on('error', ignoreerror);
  };


  task['noop'] = function () { };

  /**
   * file generation
   */
  if (browser) {

    var browserifyCommand;

    if (name !== 'lib') {
      browserifyCommand = browserifyPath + ' --require ./index.js:' + fullname + ' --external thoughtcore-lib -o ' + fullname + '.js';
    } else {
      browserifyCommand = browserifyPath + ' --require ./index.js:thoughtcore-lib -o thoughtcore-lib.js';
    }

    task['browser:uncompressed'] = function () {
      exec($({browserifyCommand}), function (err, stdout, stderr) {
        console.log(stdout);
        console.log(stderr);
        //console.log(err)
      });
    };

    task['browser:terser'] = function () {
      return gulp.src(fullname + '.js')
        .pipe(terser({
          mangle: true,
          compress: true
        }))
        .pipe(rename(fullname + '.min.js'))
        .pipe(gulp.dest('.'))
        .on('error', console.error);
    };

    task['browser:compressed'] =
      gulp.series(task['browser:uncompressed'], task['browser:terser']);

    // task['browser:maketests'] = shell.task([
    //   'find test/ -type f -name "*.js" | xargs ' + browserifyPath + ' -t brfs -o tests.js'
    // ]);

    task['browser:maketests'] = function () {
      let command = 'find test/ -type f -name "*.js" | xargs ' + browserifyPath + ' -t brfs -o tests.js';
      exec($({command}), function (err, stdout, stderr) {
        console.log(stdout);
        console.log(stderr);
        //console.log(err)
      });
    };

    task['browser'] = task['browser:compressed'];
  }

  /**
   * code quality and documentation
   */

  //  task['lint']= function() {
  //    return gulp.src(alljs)
  //      .pipe(jshint())
  //      .pipe(jshint.reporter('default'));
  //  };

  //  task['plato']= shell.task([platoPath + ' -d report -r -l .jshintrc -t ' + fullname + ' lib']);

  //task['coverage'] = shell.task([istanbulPath + ' cover ' + mochaPath + ' -- --recursive']);

  // task['coveralls'] = gulp.series(task['coverage'], function() {
  //   gulp.src('coverage/lcov.info').pipe(coveralls());
  // });

  /**
   * watch tasks
   */

  task['watch:test'] = function () {
    //// todo: only run tests that are linked to file changes by doing
    //// something smart like reading through the require statements
    return gulp.watch(alljs, gulp.series('test'));
  };

  task['watch:test:node'] = function () {
    //// todo: only run tests that are linked to file changes by doing
    //// something smart like reading through the require statements
    return gulp.watch(alljs, gulp.series('test:node'));
  };

  if (browser) {
    task['watch:test:browser'], function () {
      // todo: only run tests that are linked to file changes by doing
      // something smart like reading through the require statements
      return gulp.watch(alljs, task['test:browser']);
    };
  }

  task['watch:coverage'] = function () {
    // todo: only run tests that are linked to file changes by doing
    // something smart like reading through the require statements
    return gulp.watch(alljs, task[coverage]);
  };

  task['watch:lint'] = function () {
    //// todo: only lint files that are linked to file changes by doing
    //// something smart like reading through the require statements
    return gulp.watch(alljs, task[lint]);
  };

  if (browser) {
    task['watch:browser'] = function () {
      return gulp.watch(alljs, task[browser]);
    };
  }

  if (browser) {
    task['test:browser'] = gulp.series(task['browser:uncompressed'], task['browser:maketests'], task['test:karma']);
    task['test'] = gulp.series(task['test:node'], task['test:browser']);
  } else {
    task['test'] = task['test:node'];
  }
  task['default'] = task['test'];

  /**
   * Release automation
   */

  //task['release:install'] = shell.task(['npm install']);
  var releaseFiles = ['./package.json'];
  return task;
}

module.exports = startGulp;

