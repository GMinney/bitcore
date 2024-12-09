import { unitAfterHelper, unitBeforeHelper } from '../helpers/unit';

describe('server', function () {
  before(unitBeforeHelper);
  after(unitAfterHelper);

  it('should have a test which runs', function () {
    (import('chai')).then((chai) => {
      const expect = chai.expect
      expect(true).to.equal(true);
    });
  });
});
