package org.thrudb.thrudoc;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.thrudb.thrudoc.Thrudoc.Iface;
import org.thrudb.thrudoc.tokyocabinet.TokyoCabinetDB;


public class ThrudocHandler implements Iface {

	private Logger logger = Logger.getLogger(getClass());
	private volatile Map<String,TokyoCabinetDB> bucketMap = new HashMap<String,TokyoCabinetDB>(); 
	private String docRoot;
	
	public ThrudocHandler(String docRoot){
		this.docRoot = docRoot;
	}
	
	public boolean isValidBucket(String bucketName) throws TException {
		
		if(bucketMap.containsKey(bucketName))
			return true;

		synchronized(bucketMap){
			
			//double lock check
			if(bucketMap.containsKey(bucketName))
				return true;
	
			String dbFileName = docRoot+File.separatorChar+bucketName+".tcb";
			File   dbFile     = new File(dbFileName);
			
			//open this index if it already exists
			if(dbFile.isFile() && dbFile.canWrite()){
				bucketMap.put(bucketName, new TokyoCabinetDB(docRoot,bucketName));
				return true;
			}else{
				return false;
			}
		}
	}
	
	public String admin(String op, String data) throws ThrudocException,
			TException {
		
		
		
		return "ok";
	}

	
	public int decr(String bucket, String key, int amount) throws InvalidBucketException, TException {
		
		if(!isValidBucket(bucket))
			throw new InvalidBucketException();
		
		return 0;
	}

	
	/**
	 * Get's a key from the bucket
	 */
	public byte[] get(String bucket, String key) throws InvalidBucketException, TException {
		
		if(!isValidBucket(bucket))
			throw new InvalidBucketException();
		
		byte[] value = bucketMap.get(bucket).get(key);
		
		if(value == null)
			value = new byte[]{};
		
		return value;
	}

	public void create_bucket(String bucket) throws ThrudocException,
			TException {
		
		if(bucketMap.containsKey(bucket))
			return;
				
		bucketMap.put(bucket, new TokyoCabinetDB(docRoot,bucket));
		
	}

	public void delete_bucket(String bucket) throws ThrudocException,
			TException {
		
		if(!isValidBucket(bucket))
			this.create_bucket(bucket);
		
		
		TokyoCabinetDB db = bucketMap.get(bucket);
		
		if(db == null)
			return; //this can't happen
		
		db.erase();
		
		bucketMap.remove(bucket);
	}

	public Set<String> get_bucket_list() throws ThrudocException, TException {
		return bucketMap.keySet();
	}

	public int incr(String bucket, String key, int amount) throws TException, InvalidBucketException {
		if(!isValidBucket(bucket))
			throw new InvalidBucketException();
		
		return bucketMap.get(bucket).incr(key, amount);
	}

	public int length(String bucket, String key) throws TException, InvalidBucketException {
		if(!isValidBucket(bucket))
			throw new InvalidBucketException();
		
		return bucketMap.get(bucket).length(key);
	}

	public byte[] pop_back(String bucket, String key) throws TException, InvalidBucketException {
		if(!isValidBucket(bucket))
			throw new InvalidBucketException();
		
		return bucketMap.get(bucket).pop_back(key);
	}

	public byte[] pop_front(String bucket, String key) throws TException, InvalidBucketException {
		if(!isValidBucket(bucket))
			throw new InvalidBucketException();
		
		return bucketMap.get(bucket).pop_front(key);
	}

	public void push_back(String bucket, String key, byte[] value)
			throws ThrudocException, InvalidBucketException, TException {
		
		if(!isValidBucket(bucket))
			throw new InvalidBucketException();
		
		bucketMap.get(bucket).push_back(key, value);
		
	}

	public void push_front(String bucket, String key, byte[] value)
			throws ThrudocException, TException, InvalidBucketException {
		
		if(!isValidBucket(bucket))
			throw new InvalidBucketException();
		
		bucketMap.get(bucket).push_front(key, value);
		
	}

	public void put(String bucket, String key, byte[] value) throws InvalidBucketException, TException {
		if(!isValidBucket(bucket))
			throw new InvalidBucketException();
		
		bucketMap.get(bucket).put(key,value);
		
	}

	public List<byte[]> range(String bucket, String key, int start, int end)
			throws TException, InvalidBucketException {
		if(!isValidBucket(bucket))
			throw new InvalidBucketException();
		
		return bucketMap.get(bucket).range(key, start, end);
	}

	public byte[] remove_at(String bucket, String key, int pos)
			throws TException, InvalidBucketException {
		if(!isValidBucket(bucket))
			throw new InvalidBucketException();
		
		return bucketMap.get(bucket).remove_at(key, pos);
	}

	public void remove(String bucket, String key) throws InvalidBucketException, TException {
		if(!isValidBucket(bucket))
			throw new InvalidBucketException();
		
		bucketMap.get(bucket).remove(key);
	}

	public void insert_at(String bucket, String key, byte[] value, int pos)
			throws ThrudocException, InvalidBucketException, TException {

		if(!isValidBucket(bucket))
			throw new InvalidBucketException();
		
		bucketMap.get(bucket).insert_at(key, value, pos);		
	}

	public void replace_at(String bucket, String key, byte[] value, int pos)
			throws ThrudocException, InvalidBucketException, TException {
		
		if(!isValidBucket(bucket))
			throw new InvalidBucketException();
		
		bucketMap.get(bucket).replace_at(key, value, pos);		
	}

	public byte[] retrieve_at(String bucket, String key, int pos)
			throws ThrudocException, InvalidBucketException, TException {
		if(!isValidBucket(bucket))
			throw new InvalidBucketException();
		
		return bucketMap.get(bucket).retrieve_at(key, pos);
	}

	public List<String> scan(String bucket, String seed, int count) throws InvalidBucketException, TException {
		if(!isValidBucket(bucket))
			throw new InvalidBucketException();
		
		
		return bucketMap.get(bucket).scan(seed,count);	
	}

}
