package org.thrudb.thrudex.lucene;

import org.apache.lucene.document.Document;
import org.apache.lucene.analysis.Analyzer;
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
	
	String          getPayload(String key) throws ThrudexException;
	void            put(String key, Document document, Analyzer analyzer) throws ThrudexException;
	void            remove(String key) throws ThrudexException;
	SearchResponse  search(SearchQuery query, Analyzer analyzer) throws ThrudexException;
	void            optimize() throws ThrudexException;
	
	void            shutdown(); 
	
}
