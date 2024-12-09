
import { unitAfterHelper, unitBeforeHelper } from '../helpers/unit';

describe('logger', function () {
  before(unitBeforeHelper);
  after(unitAfterHelper);

  it('should have a test which runs', function () {

    (import('chai')).then(() => {
      chai.expect(true).to.equal(true);
    });
  });
});
