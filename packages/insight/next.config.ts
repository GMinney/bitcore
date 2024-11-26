import type { NextConfig } from 'next';

const nextConfig: NextConfig = {
  /* config options here */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  output: "standalone",
  webpack: (config: any) => {
    // Add fallbacks for Node.js core modules
    config.resolve.fallback = {
      ...config.resolve.fallback,
      assert: require.resolve('assert/'),
      buffer: require.resolve('buffer'),
      crypto: require.resolve('crypto-browserify'),
      stream: require.resolve('stream-browserify'),
      url: require.resolve('url/'),
      vm: require.resolve('vm-browserify')
    };

    // Add ProvidePlugin to make Buffer and process available globally
    // config.plugins.push(
    //   new webpack.ProvidePlugin({
    //     Buffer: ['buffer', 'Buffer'],
    //   }),
    //   new webpack.ProvidePlugin({
    //     process: 'process/browser.js'
    //   })
    // );

    // Exclude .wav files from being included in Webpack
    config.module.rules.push({
      test: /\.wav$/,
      loader: 'file-loader',
      options: {
        emitFile: false,
      },
    });

    return config;
  },
  redirects: async () => {
    return [
      // Basic redirect
      {
        source: '/',
        destination: '/insight',
        permanent: true,
      },
      // Wildcard path matching
      // {
      //   source: '/blog/:slug',
      //   destination: '/news/:slug',
      //   permanent: true,
      // },
    ];
  },
};

export default nextConfig;
