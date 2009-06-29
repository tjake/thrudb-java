package org.thrudb.util;

import org.apache.log4j.Logger;

public class CircuitBreaker {
	private enum State {
		CLOSED,
		HALF_OPEN,
		OPEN
	}
	
	private static Logger logger = Logger.getLogger(CircuitBreaker.class);
	private State state     = State.CLOSED;
	private long  nextCheck = 0;
	
	private int   failureRate   = 0;
	private int   threshold     = 1;
	private int   timeoutInSecs = 1;
	
	
	public CircuitBreaker(int threshold, int timeoutInSecs){
		if(threshold > 0)
			this.threshold = threshold;
		
		if(timeoutInSecs > 0)
			this.timeoutInSecs = timeoutInSecs;

	}
	
	public boolean allow() {
		
		if(state == State.OPEN && nextCheck < System.currentTimeMillis()/1000){
			logger.debug("allow:  going half-open");
			state = State.HALF_OPEN;
		}
		
		return state == State.CLOSED || state == State.HALF_OPEN;				
	}
	
	public void success(){
		if(state == State.HALF_OPEN){
			reset();
		}
	}
	
	public void failure(){
		if(state == State.HALF_OPEN){
			logger.debug("failure: in half-open, trip");
			trip();
		} else {
			++failureRate;
			
			if(failureRate > threshold){
				logger.debug("failure: reached threash, tripped");
				trip();	
			}
		}
	}
	
	private void reset(){
		state       = State.CLOSED;
		failureRate = 0;		
	}
	
	private void trip(){
		
		if(state != State.OPEN){
			logger.debug("trip: tripped");
			
			state = State.OPEN;
			nextCheck = System.currentTimeMillis()/1000 + timeoutInSecs;
		}
		
	}
}
