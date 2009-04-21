package org.thrudb.thrudex.lucene;

import org.apache.lucene.document.Document;
import org.thrudb.thrudex.SearchQuery;
import org.thrudb.thrudex.SearchResponse;
import org.thrudb.thrudex.ThrudexException;


/**
 * Defines the public interaction with a lucene index
 * 
 * @author jake
 *
 */
public interface LuceneIndex {
	static final String DOCUMENT_KEY = "__KEY__";
	static final String PAYLOAD_KEY  = "__PAYLOAD__";
	
	void            put(String key, Document document) throws ThrudexException;
	void            remove(String key) throws ThrudexException;
	SearchResponse  search(SearchQuery query) throws ThrudexException;
	void            optimize() throws ThrudexException;
	
	void            shutdown(); 
	
}
