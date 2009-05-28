package org.thrudb.thrudoc.tokyocabinet;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.thrudb.thrudoc.ThrudocBackend;

import tokyocabinet.BDB;
import tokyocabinet.BDBCUR;

/**
 * Implements thrudoc api using TokyoCabinet.
 * 
 * @author jake
 *
 */
public class TokyoCabinetDB implements ThrudocBackend {

	private Logger logger = Logger.getLogger(getClass());
	private String docRoot;
	private String bucketName;
	private BDB    bdb;
	
	/**
	 * Allocates a new tokyo cabinet bdb.
	 * 
	 * Works like a key/value store with list functionality
	 * If the db is not on disk it creates it.
	 * 
	 * @param docRoot      where all the dbs live.
	 * @param bucketName   db name really
	 * 
	 * @throws TException
	 */
	public TokyoCabinetDB(String docRoot, String bucketName) throws TException{
		this.docRoot    = docRoot;
		this.bucketName = bucketName;
		
		int bdbFlags = BDB.OWRITER;
	
		//verify db file
		String dbFileName = docRoot+File.separatorChar+bucketName+".tcb";
		File   dbFile     = new File(dbFileName);
		
		if(dbFile.isFile() && !dbFile.canWrite())
			throw new TException(dbFileName+" is not writable");
	
		if(dbFile.isDirectory())
			throw new TException(dbFileName+" should not be a directory");
		
		if(!dbFile.exists())
			bdbFlags |= BDB.OCREAT; 
		
		
		bdb = new BDB();
		if(!bdb.open(dbFileName,bdbFlags)){
			throw new TException(bdb.errmsg());
		}		
	}
	
	
	/**
	 * Gets a key from the db.
	 * 
	 * @param key the key name
	 * @return the value for this key (binary)
	 */
	public byte[] get(String key) {
		
		byte[] value = bdb.get(key.getBytes());
		
		return value;
	}
	
	
	/**
	 * Creates or Replaces a key in the db with a binary value
	 * @param key the key name
	 * @param value the binary value
	 */
	public void put(String key, byte[] value) {			
		
		bdb.put(key.getBytes(), value);
	}
	
	/**
	 * Removes a key/value from the db.
	 * 
	 * @param key the name of key to remove
	 */
	public void remove(String key) {
		bdb.out(key);
	}
	
	/**
	 * Returns a list of keys that have a lexical order greater than the seed.
	 * 
	 * @param seed the starting key to search from
	 * @param limit the max results
	 * @return list of keys greater than the seed
	 */
	@SuppressWarnings("unchecked")
	public List<String> scan(String seed, int limit) {
		return bdb.fwmkeys(seed, limit);
	}
	
	/**
	 * Increments a counter by a specified amount.
	 * 
	 * @param key the counter name
	 * @param amount the amount to increment by
	 * @return
	 */
	public int incr(String key, int amount){
		return bdb.addint(key, amount);
	}
	
	
	/**
	 * Decrements a counter by a specified amount
	 * 
	 * @param key the counter name
	 * @param amount the amount to decrement by
	 * @return
	 */
	public int decr(String key, int amount){
		
		if(amount > 0)
			amount *= -1;
		
		return bdb.addint(key, amount);
	}
	
	
	@SuppressWarnings("unchecked")
	public void push_back(String key, byte[] value) throws TException {
		
		//first get the next key.
		List<byte[]> nextKey = bdb.fwmkeys(key.getBytes(),2);
		
		BDBCUR cursor = new BDBCUR(bdb);
		
		//look for edge cases
		
		//no forward keys
		if(nextKey.isEmpty() ){
			this.put(key, value);
			return;
		}
				
		//the input is all that was found. jump to last record.
		if( Arrays.equals(nextKey.get(0),key.getBytes()) && nextKey.size() == 1){
			if(!cursor.last())
				throw new RuntimeException("Can't jump to last record");
		}else{
		
			//jump to key ahead of our input key
			if(nextKey.size() == 1){		
				if(!cursor.jump(nextKey.get(0)))
					throw new RuntimeException("Key should exist but failed to jump to it's location");				
			} else {
				if(!cursor.jump(nextKey.get(1)))
					throw new RuntimeException("Key should exist but failed to jump to it's location");
			}		
		}
		
		//back one should be the input key, if it's not then the key 
		//does not exist yet, so we add it
		if(!Arrays.equals(cursor.key(),key.getBytes())){
			if(!cursor.prev()){
				this.put(key, value);
				return;
			}
		}
		
		// the key under this cursor should match, if not we add a new key
		//based on input
		if(!Arrays.equals(cursor.key(),key.getBytes())){
			this.put(key, value);
			return;
		}
		
		//we are at the right spot, now append the value
		if(!cursor.put(value, BDBCUR.CPAFTER))		
			throw new TException(bdb.errmsg());
			
	}
	
	@SuppressWarnings("unchecked")
	public byte[] pop_back(String key) throws TException{
		
		//first get the next key.
		List<byte[]> nextKey = bdb.fwmkeys(key.getBytes(),2);
		
		BDBCUR cursor = new BDBCUR(bdb);
		
		if(nextKey.isEmpty()){
			return new byte[]{};		
		}
		
		//first entry should match, if not the input key is missing so fail
		if(!Arrays.equals(nextKey.get(0),key.getBytes())){
			
			return new byte[]{};
		
		}
		
		//the input key is at the end of the index so jump to end
		if(nextKey.size() == 1) {
			
			if(!cursor.last())
				throw new RuntimeException("Unable to jump to last key");
			
		} else {
			
			if(!cursor.jump(nextKey.get(1)))
				throw new RuntimeException("Key should exist but failed to jump to it's location");
			
		}
		
			
		//move back one, to tail of the input key
		if(!cursor.prev())
			throw new RuntimeException("Key should exist but failed to jump to it's location");
		
		if(!Arrays.equals(cursor.key(),key.getBytes()))
			throw new TException("key mismatch "+key+" vs "+cursor.key2());
	
		byte[] value = cursor.val();
		
		//delete
		if(!cursor.out())
			throw new TException(bdb.errmsg());
		
		
		return value;
	}
	
	public void push_front(String key, byte[] value) throws TException {

		BDBCUR cursor = new BDBCUR(bdb);
		
		if(!cursor.jump(key)){
			this.put(key, value);
			return;
		}
		
		//not matching key so add new
		if(!Arrays.equals(cursor.key(),key.getBytes())){
			this.put(key, value);
			return;
		}
		
		if(!cursor.put(value, BDBCUR.CPBEFORE))
			throw new TException(bdb.errmsg());
		
	}
	
	public byte[] pop_front(String key) throws TException{
		
		BDBCUR cursor = new BDBCUR(bdb);
		
		if(!cursor.jump(key))
			return new byte[]{};
		
		if(!Arrays.equals(cursor.key(),key.getBytes()))
			return new byte[]{};
	
		
		//save value
		byte[] value = cursor.val();
			
		
		//delete
		if(!cursor.out())
			throw new TException(bdb.errmsg());
		
		return value;
	}
	
	private BDBCUR goto_position(String key, int position) {
		if(position < 0)
			return null;
		
		BDBCUR cursor = new BDBCUR(bdb);
		
		if(!cursor.jump(key))
			return null;
		
		int currentPos = 0;
		int checkMod = 10; //verify key every few records
		while(currentPos < position){
			cursor.next();
			
			if(currentPos % checkMod == 0){
				
				if(!Arrays.equals(cursor.key(), key.getBytes()))
					return null;
				
			}
			
			currentPos++;
		}
		
		//at position
		if(!Arrays.equals(cursor.key(), key.getBytes()))
			return null;
		
		
		return cursor;
	}
	
	public byte[] remove_at(String key, int position){
		
		BDBCUR cursor = this.goto_position(key, position);
		
		//if position not found
		if(cursor == null)
			return null;
		
		byte[] value = cursor.val();
		
		if(!cursor.out())
			throw new RuntimeException("Unable to remove record");
	
		return value;
	}
	
	public void insert_at(String key, byte[] value, int position) {
		BDBCUR cursor = this.goto_position(key, position);
		
		if(cursor == null)
			throw new RuntimeException("Unable to insert at position: "+position);
		
		if(!cursor.put(value, BDBCUR.CPBEFORE))
			throw new RuntimeException("Unable to replace record");
	}
	
	public void replace_at(String key, byte[] value, int position) {
		BDBCUR cursor = this.goto_position(key, position);
		
		if(cursor == null)
			return;
		
		if(!cursor.put(value, BDBCUR.CPCURRENT))
			throw new RuntimeException("Unable to replace record");
	}
	
	public byte[] retrieve_at(String key, int position){
		BDBCUR cursor = this.goto_position(key, position);
		
		//if position not found
		if(cursor == null)
			return null;
		
		byte[] value = cursor.val();
		
	
		return value;
	}
	
	public List<byte[]> range(String key, int start, int end){
		if(start > end || end < 0 || start < 0)
			throw new RuntimeException("Invalid start and/or end context");
		
		BDBCUR cursor = this.goto_position(key, start);
		
		if(cursor == null)
			return null;
		
		int currentPos = 0;
		int distance   = end - start;
		List<byte[]> response = new ArrayList<byte[]>();
		
		while(currentPos <= distance){
			response.add(cursor.val());
			
			if(!cursor.next())
				break;
			
			currentPos++;
		}
		
		return response;
	}
	
	public int length(String key) {
		BDBCUR cursor = new BDBCUR(bdb);
		
		if(!cursor.jump(key))
			return 0;
		
		if(!Arrays.equals(cursor.key(),key.getBytes()))
			return 0;
		
		int length = 0;
		while(Arrays.equals(cursor.key(),key.getBytes())){
			length++;
			
			if(!cursor.next())
				return length;
		}
		
		
		return length;
	}
	
	public boolean erase(){
		return bdb.vanish();
	}
	
}
