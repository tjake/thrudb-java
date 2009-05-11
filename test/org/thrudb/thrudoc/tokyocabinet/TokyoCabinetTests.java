package org.thrudb.thrudoc.tokyocabinet;

import junit.framework.TestCase;
import tokyocabinet.BDB;
import tokyocabinet.BDBCUR;
import tokyocabinet.HDB;

public class TokyoCabinetTests extends TestCase {
	BDB bdb;
	
	@Override
	public void setUp() {
		// create the object
		bdb = new BDB();

		// open the database
		if (!bdb.open("casket.tcb", BDB.OWRITER | BDB.OCREAT)) {
			int ecode = bdb.ecode();
			fail("open error: " + bdb.errmsg(ecode));
		}
	}
	
	@Override
	public void tearDown(){
		bdb.vanish();
		
		// close the database
		if (!bdb.close()) {
			int ecode = bdb.ecode();
			fail("close error: " + bdb.errmsg(ecode));
		}
	}
	
	public void testBdb() {

		// store records
		if (!bdb.put("foo", "hop") || !bdb.put("bar", "step")
				|| !bdb.put("baz", "jump")) {
			int ecode = bdb.ecode();
			System.err.println("put error: " + bdb.errmsg(ecode));
		}

		// retrieve records
		String value = bdb.get("foo");
		if (value != null) {
			System.out.println(value);
		} else {
			int ecode = bdb.ecode();
			System.err.println("get error: " + bdb.errmsg(ecode));
		}

		// traverse records
		BDBCUR cur = new BDBCUR(bdb);
		cur.first();
		String key;
		while ((key = cur.key2()) != null) {
			value = cur.val2();
			if (value != null) {
				System.out.println(key + ":" + value);
			}
			cur.next();
		}

	}
	
	public void testIncr() {
		
		int val = 0;
		for(int i=0; i<100; i++)
			val = bdb.addint("key", 1);
		
		assertEquals(val,100);
	}

	public void testInsertPerformance() {
		long start = System.currentTimeMillis();
	
		for(int i=0; i<1000000; i++){
			if(!bdb.put("key"+i, "123456")){
				fail(bdb.errmsg());
			}
		}
	
		bdb.sync();
		long end   = System.currentTimeMillis();
	
		//System.err.println((end-start)/1000.0);
	
		//System.err.println(bdb.range("key1", true, "key40", true, -1));
	
	}
	
	public void testTransactions(){
		bdb.tranbegin();
		
		int val = 0;
		for(int i=0; i<100; i++)
			val = bdb.addint("key", 1);
		
		assertEquals(100,val);
		
		bdb.tranabort();
		
		assertEquals(0,bdb.addint("key",0));
		
	}
	
	public void testAppend() {
		
		for(int i=0; i<1000; i++){
			if(!bdb.putdup("key", "val"+i))
				fail(bdb.errmsg());
		}
				
		assertEquals(bdb.rnum(),1000);
		
		//push front
		BDBCUR c = new BDBCUR(bdb);
		if(!c.jump("key"))
			fail("cursor failed");
		
		for(int i=0; i<100; i++)
			c.next();
		
		assertEquals(c.key2(),"key");
		assertEquals(c.val2(),"val100");
		
//		System.err.println(bdb.getlist("key"));

	}
	
}
