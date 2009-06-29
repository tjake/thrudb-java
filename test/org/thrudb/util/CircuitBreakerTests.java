package org.thrudb.util;

import junit.framework.TestCase;

public class CircuitBreakerTests extends TestCase {

	public void testBreaker() {
		try {
			CircuitBreaker breaker = new CircuitBreaker(5, 1);

			assertTrue(breaker.allow());
			breaker.success();

			// do 6 failures, we have to go above the threshold...

			boolean allow = breaker.allow();
			assertTrue(allow);
			if (allow)
				breaker.failure();

			// 1
			allow = breaker.allow();
			assertTrue(allow);
			if (allow)
				breaker.failure();

			// 2
			allow = breaker.allow();
			assertTrue(allow);
			if (allow)
				breaker.failure();

			// 3
			allow = breaker.allow();
			assertTrue(allow);
			if (allow)
				breaker.failure();

			// 4
			allow = breaker.allow();
			assertTrue(allow);
			if (allow)
				breaker.failure();

			// 5
			allow = breaker.allow();
			assertTrue(allow);
			if (allow)
				breaker.failure();

			// 6
			allow = breaker.allow();
			assertTrue(!allow);

			// now sleep more than timeout, we should be allowed, in half-open
			Thread.sleep(2000);
			allow = breaker.allow();
			assertTrue(allow);
			if (allow)
				breaker.failure();

			// we failed in half-open should now go back to open
			allow = breaker.allow();
			assertTrue(!allow);

			// now sleep more than timeout, we should be allowed, in half-open,
			// but this time we'll succeed
			Thread.sleep(2000);
			allow = breaker.allow();
			assertTrue(allow);
			if (allow)
				breaker.success();

			// we should now be back in closed
			allow = breaker.allow();
			assertTrue(allow);
		} catch (Exception e) {
			fail(e.getLocalizedMessage());
		}
	}

}
