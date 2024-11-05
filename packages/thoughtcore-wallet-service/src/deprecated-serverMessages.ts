export const serverMessages = (wallet, appName, appVersion) => {
  if (!appVersion || !appName) return;

  if (wallet.network == 'livenet' && appVersion.major == 5) {
    return {
      title: 'Deprecated Test message',
      body: 'Only for thoughtnetwork, old wallets',
      link: 'http://thoughtnetwork.com',
      id: 'thoughtnetwork1',
      dismissible: true,
      category: 'critical',
      app: 'thoughtnetwork'
    };
  }
};
