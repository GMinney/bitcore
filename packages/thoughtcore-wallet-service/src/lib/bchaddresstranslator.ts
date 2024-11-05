import _ from 'lodash';
const Thoughtcore_ = {
  tht: require('thoughtcore-lib'),
  bch: require('thoughtcore-lib-cash')
};

export class BCHAddressTranslator {
  static getAddressCoin(address) {
    try {
      new Thoughtcore_['tht'].Address(address);
      return 'legacy';
    } catch (e) {
      try {
        const a = new Thoughtcore_['bch'].Address(address);
        if (a.toLegacyAddress() == address) return 'copay';
        return 'cashaddr';
      } catch (e) {
        return;
      }
    }
  }

  // Supports 3 formats:  legacy (1xxx, mxxxx); Copay: (Cxxx, Hxxx), Cashaddr(qxxx);
  static translate(addresses, to, from?) {
    let wasArray = true;
    if (!_.isArray(addresses)) {
      wasArray = false;
      addresses = [addresses];
    }
    from = from || BCHAddressTranslator.getAddressCoin(addresses[0]);

    let ret;
    if (from == to) {
      ret = addresses;
    } else {
      ret = _.filter(
        _.map(addresses, x => {
          const thoughtcore = Thoughtcore_[from == 'legacy' ? 'tht' : 'bch'];
          let orig;

          try {
            orig = new thoughtcore.Address(x).toObject();
          } catch (e) {
            return null;
          }

          if (to == 'cashaddr') {
            return Thoughtcore_['bch'].Address.fromObject(orig).toCashAddress(true);
          } else if (to == 'copay') {
            return Thoughtcore_['bch'].Address.fromObject(orig).toLegacyAddress();
          } else if (to == 'legacy') {
            return Thoughtcore_['tht'].Address.fromObject(orig).toString();
          }
        })
      );
    }
    if (wasArray) return ret;
    else return ret[0];
  }
}
