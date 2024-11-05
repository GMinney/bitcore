export const serverMessages = (wallet, appName, appVersion) => {
  if (!appVersion || !appName) return;

  const serverMessages = [];
  if (wallet.network == 'livenet' && appVersion.major == 5 && wallet.createdOn < 1443461026) {
    serverMessages.push({
      title: 'Test message',
      body: 'Only for thoughtnetwork, old wallets',
      link: 'http://thoughtnetwork.com',
      id: 'thoughtnetwork1',
      dismissible: true,
      category: 'critical',
      app: 'thoughtnetwork',
      priority: 2
    });
  }
  if (wallet.network == 'livenet') {
    serverMessages.push({
      title: 'Test message 2',
      body: 'Only for thoughtnetwork livenet wallets',
      link: 'http://thoughtnetwork.com',
      id: 'thoughtnetwork2',
      dismissible: true,
      category: 'critical',
      app: 'thoughtnetwork',
      priority: 1
    });
  }
  return serverMessages;
};
