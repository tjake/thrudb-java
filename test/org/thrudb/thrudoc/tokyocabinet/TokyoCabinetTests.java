package org.thrudb.thrudoc.tokyocabinet;

import java.util.List;

import junit.framework.TestCase;

import org.thrudb.thrudoc.InvalidKeyException;

public class TokyoCabinetTests extends TestCase {
	TokyoCabinetDB tdb;
	
	@Override
	public void setUp() {
		try{
			tdb = new TokyoCabinetDB("","unittest");
			tdb.erase();
		}catch(Throwable t){
			t.printStackTrace();
			fail(t.getMessage());
		}
	}
	
	@Override
	public void tearDown(){
		
	}
	
	
	/**
	 * 
	 */
	public void testCRUD(){
		try{
		
			//Create
			for(int i=0; i<100; i++){
				tdb.put("key"+Integer.toString(i), ("value"+Integer.toString(i)).getBytes());
			}
			
			//Retrieve
			for(int i=0; i<100; i++){
				String val = new String(tdb.get("key"+Integer.toString(i)));
				assertEquals("value"+Integer.toString(i), val);
			}
			
			//Update
			for(int i=0; i<100; i++){
				tdb.put("key"+Integer.toString(i), ("updated value"+Integer.toString(i)).getBytes());
			}
			
			//re-Retrieve
			for(int i=0; i<100; i++){
				String val = new String(tdb.get("key"+Integer.toString(i)));
				assertEquals("updated value"+Integer.toString(i), val);
			}
			
			//Delete
			for(int i=0; i<100; i++){
				tdb.remove("key"+Integer.toString(i));		
			}
			
			
			//re-Retrieve
			for(int i=0; i<100; i++){
				
				try{
					tdb.get("key"+Integer.toString(i));
					fail("key still exists after removed");
				}catch(InvalidKeyException ex){
					//this is good
				}
			}
			
		}catch(Throwable t){
			t.printStackTrace();
			fail(t.getLocalizedMessage());
		}
	}
	
	public void testIncr() {
		
		int val = 0;
		for(int i=0; i<100; i++)
			val = tdb.incr("key", 1);
		
		assertEquals(100,val);
	
		for(int i=0; i<100; i++)
			val = tdb.decr("key", 1);
		
		assertEquals(0,val);
	
	}
	
	
	public void testPushPopBack(){
		try{
			tdb.push_back("a", ("val").getBytes());
			tdb.push_back("z", ("val").getBytes());
		
			for(int i=0; i<100; i++)
				tdb.push_back("key", ("value"+Integer.toString(i)).getBytes());

			assertEquals(100,tdb.length("key"));

			
			for(int i=99; i>=0; i--)
				assertEquals("value"+Integer.toString(i),new String(tdb.pop_back("key"))); 

			assertEquals(0,tdb.length("key"));

			
		}catch(Throwable t){
			
			t.printStackTrace();
			fail(t.getLocalizedMessage());
		}
	}

	public void testPushPopFront(){
		try{
			tdb.push_back("a", ("val").getBytes());
			tdb.push_back("z", ("val").getBytes());
		
			for(int i=0; i<100; i++)
				tdb.push_front("key", ("value"+Integer.toString(i)).getBytes());
	
			assertEquals(100,tdb.length("key"));
			
			for(int i=99; i>=0; i--)
				assertEquals("value"+Integer.toString(i),new String(tdb.pop_front("key"))); 
		
			assertEquals(0,tdb.length("key"));
			
		}catch(Throwable t){
			
			t.printStackTrace();
			fail(t.getLocalizedMessage());
		}
	}

	public void testListCRUD(){
	
		try{

			for(int i=0; i<100; i++)
				tdb.push_back("key", ("value"+String.valueOf(i)).getBytes());
			
			assertEquals(100,tdb.length("key"));
			
			tdb.insert_at("key", ("funk").getBytes(), 77);
			
			assertEquals(101,tdb.length("key"));
			assertEquals("funk", new String(tdb.retrieve_at("key", 77)));
			
			tdb.replace_at("key", ("soul").getBytes(), 77);
			
			assertEquals("soul", new String(tdb.retrieve_at("key", 77)));
			
			assertEquals("soul", new String(tdb.remove_at("key", 77)));
			
			assertEquals(100, tdb.length("key"));
	
			
			List<byte[]> res = tdb.range("key", 90, 10000);
			assertEquals(10,res.size());
			
			for(int i=0; i<10; i++){
				assertEquals("value"+String.valueOf(i+90), new String(res.get(i)));
			}
			
		}catch(Throwable t){
			t.printStackTrace();
			fail(t.getLocalizedMessage());
		}
	}
	
}