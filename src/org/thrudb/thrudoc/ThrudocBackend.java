package org.thrudb.thrudoc;

import java.util.List;

import org.apache.thrift.TException;

import tokyocabinet.BDBCUR;

public interface ThrudocBackend {

	byte[] get(String key);
	
	void put(String key, byte[] value);
	
	void remove(String key);
	
	List<String> scan(String seed, int limit);
	
	int incr(String key, int amount);
	
	int decr(String key, int amount);
	
	void push_back(String key, byte[] value) throws TException;
	
	byte[] pop_back(String key) throws TException;
	
	void push_front(String key, byte[] value) throws TException;
	
	byte[] pop_front(String key) throws TException;
	
	byte[] remove_at(String key, int position);
	
	void insert_at(String key, byte[] value, int position);
	
	void replace_at(String key, byte[] value, int position);
	
	byte[] retrieve_at(String key, int position);
	
	List<byte[]> range(String key, int start, int end);
	
	int length(String key);
	
	boolean erase();
	
	
}
