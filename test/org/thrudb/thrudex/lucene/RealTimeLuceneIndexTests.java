package org.thrudb.thrudex.lucene;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import junit.framework.TestCase;

public class RealTimeLuceneIndexTests extends TestCase {

	final String INDEX_BASE_PATH = "test_indexes";
	final String INDEX_NAME      = "test_index";
	
	public RealTimeLuceneIndexTests(){
		super();
		
		ExecutorService pool = Executors.newFixedThreadPool(5);
	
		
	}
	
	@Override
	protected void setUp() throws Exception {
		// TODO Auto-generated method stub
		super.setUp();
	}

	@Override
	protected void tearDown() throws Exception {
		// TODO Auto-generated method stub
		super.tearDown();
	}
	
	
	
}
