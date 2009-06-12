package org.thrudb.util.hash;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.log4j.Logger;

/**
 * Consisteny
 * 
 * Based on information from libketama and
 * http://code.google.com/p/spymemcached/
 * 
 * 
 */
public class ConsistentHash {
	private SortedMap<Long, String> circle = new TreeMap<Long, String>();
	private Logger logger = Logger.getLogger(getClass());

	/**
	 * Puts the key into a circular hash see
	 * http://www8.org/w8-papers/2a-webserver/caching/paper2.html
	 * 
	 * @param key
	 *            any string but for out case its ip:port combo
	 * 
	 * @param weight
	 *            percentage of weight to be applied to a given key 0-100
	 *            default should be 1
	 */
	public void add(String key, double weight) {

		if (weight <= 0.0)
			weight = 0.0;

		if (weight > 100.0)
			weight = 100.0;

		int factor = (int)Math.round(Math.floor(weight * 40.0));
		Long[] codes = getCodeList(key,factor);
		
		for(Long code : codes)
			circle.put(code, key);
		
	}

	private Long[] getCodeList(String key, int times) {
		Long[] codes = new Long[times*4];
		MessageDigest md5;

		try {
			md5 = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException("MD5 not supported", e);
		}

		int idx = 0;
		for (int i = 0; i < times; i++) {
			
			md5.reset();
			md5.update((key + " - " + String.valueOf(i)).getBytes());

			byte[] digest = md5.digest();

			for (int h = 0; h < 4; h++) {
				Long k = ((long) (digest[3 + h * 4] & 0xFF) << 24)
						| ((long) (digest[2 + h * 4] & 0xFF) << 16)
						| ((long) (digest[1 + h * 4] & 0xFF) << 8)
						| (digest[h * 4] & 0xFF);

				codes[idx] = k;
				idx++;
			}
		}

		return codes;
	}

	/**
	 * Walks the tree and removes any keys that match the input
	 * @param key
	 * 		key to delete
	 */
	public synchronized void remove(String key) {
		SortedMap<Long, String> newCircle = new TreeMap<Long, String>();
		
		for(Map.Entry<Long, String> entry : circle.entrySet()){
			if(!entry.getValue().equals(key))
				newCircle.put(entry.getKey(), entry.getValue());
		}
		
		circle = newCircle;
	}

	/**
	 * Returns an appropriate key from the calls to add() based on this String.
	 * 
	 * This means we can send any key and figure out the correct server to store
	 * it on.
	 * 
	 * @param key
	 *            any string
	 * @return a key value from based on calls to add()
	 */
	public String get(String key) {
		if (circle.isEmpty()) {
			return null;
		}

		Long k = getCodeList(key, 1)[0];

		if (!circle.containsKey(k)) {
			SortedMap<Long, String> tailMap = circle.tailMap(k);
			k = tailMap.isEmpty() ? circle.firstKey() : tailMap.firstKey();
		}

		return circle.get(k);
	}

	

}
