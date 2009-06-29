package org.thrudb.thrudoc.zk;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.thrudb.LogEntry;
import org.thrudb.thrudoc.InvalidBucketException;
import org.thrudb.thrudoc.Thrudoc;
import org.thrudb.thrudoc.ThrudocException;
import org.thrudb.thrudoc.Thrudoc.Iface;

public class RetryBackend implements Iface {

	private int maxRetries;
	private Thrudoc.Iface delegateBackend;
	private static Logger logger = Logger.getLogger(RetryBackend.class);

	public RetryBackend(int maxRetries, Thrudoc.Iface delegateBackend) {
		this.delegateBackend = delegateBackend;

		this.maxRetries = maxRetries;
	}
	
	private void handleException(TException e, int tries) throws TException {
		logger.info("caught error:" + e.getLocalizedMessage());

		if (tries == maxRetries)
			throw e;
	}

	public String admin(String op, String data) throws ThrudocException,
			TException {

		return delegateBackend.admin(op, data);
	}

	public void create_bucket(String bucket) throws ThrudocException,
			TException {

		int tries = 0;

		while (++tries < maxRetries) {
			try {
				delegateBackend.create_bucket(bucket);
				return;
			} catch (TException e) {				
				handleException(e, tries);
			}
		}
	}

	public int decr(String bucket, String key, int amount)
			throws ThrudocException, InvalidBucketException, TException {
		
		int tries = 0;

		while (++tries < maxRetries) {
			try {
				return delegateBackend.decr(bucket, key, amount);
				
			} catch (TException e) {
				handleException(e, tries);
			}
		}
		
		return 0; //will never get here
	}

	public void delete_bucket(String bucket) throws ThrudocException,
			TException {
			
		int tries = 0;

		while (++tries < maxRetries) {
			try {
				delegateBackend.delete_bucket(bucket);
				return;
			} catch (TException e) {
				handleException(e, tries);
			}
		}
		

	}

	public byte[] get(String bucket, String key) throws ThrudocException,
			InvalidBucketException, TException {
		int tries = 0;

		while (++tries < maxRetries) {
			try {
				return delegateBackend.get(bucket, key);
				
			} catch (TException e) {
				handleException(e, tries);
			}
		}
		
		return null; //never gets here
	}

	public Set<String> get_bucket_list() throws ThrudocException, TException {
		int tries = 0;

		while (++tries < maxRetries) {
			try {
				return delegateBackend.get_bucket_list();
				
			} catch (TException e) {
				handleException(e, tries);
			}
		}
		
		return null; //never gets here
	}

	public int incr(String bucket, String key, int amount)
			throws ThrudocException, InvalidBucketException, TException {
		int tries = 0;

		while (++tries < maxRetries) {
			try {
				return delegateBackend.incr(bucket, key, amount);
				
			} catch (TException e) {
				handleException(e, tries);
			}
		}
		
		return 0;
	}

	public void insert_at(String bucket, String key, byte[] value, int pos)
			throws ThrudocException, InvalidBucketException, TException {

		int tries = 0;

		while (++tries < maxRetries) {
			try {
				delegateBackend.insert_at(bucket, key, value, pos);
				return;
			} catch (TException e) {
				handleException(e, tries);
			}
		}
		
	}

	public int length(String bucket, String key) throws ThrudocException,
			InvalidBucketException, TException {
		int tries = 0;

		while (++tries < maxRetries) {
			try {
				return delegateBackend.length(bucket, key);
				
			} catch (TException e) {
				handleException(e, tries);
			}
		}
		
		return 0;
	}

	public byte[] pop_back(String bucket, String key) throws ThrudocException,
			InvalidBucketException, TException {
		int tries = 0;

		while (++tries < maxRetries) {
			try {
				return delegateBackend.pop_back(bucket, key);
				
			} catch (TException e) {
				handleException(e, tries);
			}
		}
		
		return null;
	}

	public byte[] pop_front(String bucket, String key) throws ThrudocException,
			InvalidBucketException, TException {
		
		int tries = 0;

		while (++tries < maxRetries) {
			try {
				return delegateBackend.pop_front(bucket, key);
				
			} catch (TException e) {
				handleException(e, tries);
			}
		}
		
		return null;
	}

	public void push_back(String bucket, String key, byte[] value)
			throws ThrudocException, InvalidBucketException, TException {
		int tries = 0;

		while (++tries < maxRetries) {
			try {
				delegateBackend.push_back(bucket, key, value);
				return;
			} catch (TException e) {
				handleException(e, tries);
			}
		}
		

	}

	public void push_front(String bucket, String key, byte[] value)
			throws ThrudocException, InvalidBucketException, TException {
		int tries = 0;

		while (++tries < maxRetries) {
			try {
				delegateBackend.push_front(bucket, key, value);
				return;
			} catch (TException e) {
				handleException(e, tries);
			}
		}
		

	}

	public void put(String bucket, String key, byte[] value)
			throws ThrudocException, InvalidBucketException, TException {
		int tries = 0;

		while (++tries < maxRetries) {
			try {
				delegateBackend.put(bucket, key, value);
				return;
			} catch (TException e) {
				handleException(e, tries);
			}
		}
		
	}

	public List<byte[]> range(String bucket, String key, int start, int end)
			throws ThrudocException, InvalidBucketException, TException {
		int tries = 0;

		while (++tries < maxRetries) {
			try {
				return delegateBackend.range(bucket, key, start, end);
				
			} catch (TException e) {
				handleException(e, tries);
			}
		}
		return null;
	}

	public void remove(String bucket, String key) throws ThrudocException,
			InvalidBucketException, TException {
		
		int tries = 0;

		while (++tries < maxRetries) {
			try {
				delegateBackend.remove(bucket, key);
				return;
			} catch (TException e) {
				handleException(e, tries);
			}
		}
		

	}

	public byte[] remove_at(String bucket, String key, int pos)
			throws ThrudocException, InvalidBucketException, TException {
		int tries = 0;

		while (++tries < maxRetries) {
			try {
				return delegateBackend.remove_at(bucket, key, pos);
				
			} catch (TException e) {
				handleException(e, tries);
			}
		}
		
		return null;
	}

	public void replace_at(String bucket, String key, byte[] value, int pos)
			throws ThrudocException, InvalidBucketException, TException {
		
		int tries = 0;

		while (++tries < maxRetries) {
			try {
				delegateBackend.replace_at(bucket, key, value, pos);
				return;
			} catch (TException e) {
				handleException(e, tries);
			}
		}
		

	}

	public byte[] retrieve_at(String bucket, String key, int pos)
			throws ThrudocException, InvalidBucketException, TException {
		int tries = 0;

		while (++tries < maxRetries) {
			try {
				return delegateBackend.retrieve_at(bucket, key, pos);
				
			} catch (TException e) {
				handleException(e, tries);
			}
		}
		
		return null;
	}

	public List<String> scan(String bucket, String seed, int limit)
			throws ThrudocException, InvalidBucketException, TException {
		int tries = 0;

		while (++tries < maxRetries) {
			try {
				return delegateBackend.scan(bucket, seed, limit);
				
			} catch (TException e) {
				handleException(e, tries);
			}
		}
		
		return null;
	}

	public List<String> getAvailibleServers() throws TException {
		int tries = 0;

		while (++tries < maxRetries) {
			try {
				return delegateBackend.getAvailibleServers();
				
			} catch (TException e) {
				handleException(e, tries);
			}
		}
		return null;
	}

	public List<LogEntry> getLogSince(String bucket, String lsn, int kbLimit)
			throws TException {
		int tries = 0;

		while (++tries < maxRetries) {
			try {
				return delegateBackend.getLogSince(bucket, lsn, kbLimit);
				
			} catch (TException e) {
				handleException(e, tries);
			}
		}
		
		return null;
	}

	public Map<String, Long> getServiceStats() throws TException {
		int tries = 0;

		while (++tries < maxRetries) {
			try {
				return delegateBackend.getServiceStats();
				
			} catch (TException e) {
				handleException(e, tries);
			}
		}
		return null;
	}

	public void ping() throws TException {
	
	}

}
