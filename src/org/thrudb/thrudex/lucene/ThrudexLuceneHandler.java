package org.thrudb.thrudex.lucene;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.thrift.TException;
import org.thrudb.thrudex.Document;
import org.thrudb.thrudex.Element;
import org.thrudb.thrudex.Field;
import org.thrudb.thrudex.FieldType;
import org.thrudb.thrudex.SearchQuery;
import org.thrudb.thrudex.SearchResponse;
import org.thrudb.thrudex.ThrudexException;
import org.thrudb.thrudex.ThrudexExceptionImpl;
import org.thrudb.thrudex.Thrudex.Iface;

/**
 * Manages a set of lucene indexes.
 * We keep this one lucene backend per index
 * @author jake
 *
 */
public class ThrudexLuceneHandler implements Iface {

	private Logger logger = Logger.getLogger(this.getClass().getSimpleName());
	private Map<String,LuceneIndex> indexMap = new HashMap<String, LuceneIndex>();
	private String indexRoot;
	
	public ThrudexLuceneHandler(String indexRoot){
		this.indexRoot = indexRoot;
	}
	
	public String admin(String op, String data) throws ThrudexException,
			TException {
		
		if(op.equals("create_index"))
			addIndex(data);	
		
		if(op.equals("optimize")){
			if(indexMap.containsKey(data)){
				try{
					indexMap.get(data).optimize();
				}catch(ThrudexException e){
					return e.toString();
				}
			}
				
		}
		
		return "ok";
	}
	
	public synchronized void addIndex(String name) throws ThrudexException {
		
		if(name == null || name.trim().equals(""))
			return;
		
		if(indexMap.containsKey(name))
			return;
			
		try{
			indexMap.put(name, new SimpleLuceneIndex(indexRoot,name));
		}catch(IOException e){
			throw new ThrudexException(e.toString());
		}
	}

	/**
	 * Returns the list of available index names
	 */
	public List<String> getIndices() throws TException {
		return new ArrayList<String>(indexMap.keySet());
	}

	/**
	 * This method does nothing, but lets client check the server 
	 */
	public void ping() throws TException {
		
	}

	/**
	 * Add/Replace a document
	 */
	public void put(Document d) throws ThrudexException, TException {
		
		//make sure index is valid
		if(!indexMap.containsKey(d.index))
			throw new ThrudexExceptionImpl("No Index Found: "+d.index);	
		
		//make sure document has a key
		if(!d.isSetKey() || d.key.trim().equals(""))
			throw new ThrudexExceptionImpl("No Document key found");
			
		//Start new lucene document
		org.apache.lucene.document.Document luceneDocument = 
			new org.apache.lucene.document.Document();
		
		luceneDocument.add(
				new org.apache.lucene.document.Field(
						LuceneIndex.DOCUMENT_KEY,d.key,
						org.apache.lucene.document.Field.Store.YES,
						org.apache.lucene.document.Field.Index.NOT_ANALYZED
				)
		);
		
		
		//Add fields
		for(Field field : d.fields){
			
			if(!field.isSetKey())
				throw new ThrudexExceptionImpl("Field key not set");
			
			if(!field.isSetType())
				throw new ThrudexExceptionImpl("FieldType missing");
			
			
			//Convert FieldType to Lucene types
			org.apache.lucene.document.Field.Store fieldStoreType;
			org.apache.lucene.document.Field.Index fieldIndexType;
			
			switch(field.type){
			case FieldType.TEXT: 
				fieldStoreType = org.apache.lucene.document.Field.Store.YES; 
				fieldIndexType = org.apache.lucene.document.Field.Index.ANALYZED;
				break;
			case FieldType.UNSTORED: 
				fieldStoreType = org.apache.lucene.document.Field.Store.NO; 
				fieldIndexType = org.apache.lucene.document.Field.Index.ANALYZED;
				break;
			case FieldType.KEYWORD: 
				fieldStoreType = org.apache.lucene.document.Field.Store.YES; 
				fieldIndexType = org.apache.lucene.document.Field.Index.NOT_ANALYZED;
				break;
			default:
				throw new ThrudexExceptionImpl("Unknown FieldType: "+field.type);
			}
			
			//Create Lucene Field
			org.apache.lucene.document.Field luceneField = 
				new org.apache.lucene.document.Field(field.key, field.value, fieldStoreType,fieldIndexType);

			if(field.isSetWeight())
				luceneField.setBoost(field.weight);
			
			luceneDocument.add(luceneField);
			
			//Create sortable field?
			if(field.isSetSortable() && field.sortable){
				luceneDocument.add(
						new org.apache.lucene.document.Field(
								field.key+"_sort", field.value,
								org.apache.lucene.document.Field.Store.YES,
								org.apache.lucene.document.Field.Index.NOT_ANALYZED
						)
				);
			}
		}
		
		//Add payload
		if(d.isSetPayload()){
			luceneDocument.add(
					new org.apache.lucene.document.Field(
							LuceneIndex.PAYLOAD_KEY, d.payload,
							org.apache.lucene.document.Field.Store.YES,
							org.apache.lucene.document.Field.Index.NOT_ANALYZED
					)
			);
		}
		
		//Document is not ready to put into the index
		indexMap.get(d.index).put(d.key, luceneDocument);
		
	}

	/**
	 * Adds a list of documents to an index
	 * 
	 * Rather than returning on any error, this code captures any 
	 * errors for specific documents and puts them into a list
	 */
	public List<ThrudexException> putList(List<Document> documents)
			throws ThrudexException, TException {
		
		List<ThrudexException> exList = new ArrayList<ThrudexException>();
		
		for(Document document : documents){
			try{
				put(document);
			}catch(ThrudexException ex){

				ex.what += document.key;
				
				exList.add(ex);
			}
		}
		
		return exList;
	}

	/**
	 * Removes a document from an index
	 */
	public void remove(Element el) throws ThrudexException, TException {
		
		//make sure index is valid
		if(!indexMap.containsKey(el.index))
			throw new ThrudexExceptionImpl("No Index Found: "+el.index);	
		
		//make sure document has a key
		if(!el.isSetKey() || el.key.trim().equals(""))
			throw new ThrudexExceptionImpl("No Document key found");
		
		
		indexMap.get(el.index).remove(el.key);
	}

	
	/**
	 * Removes a set of documents.
	 * 
	 * Captures any errors for sub-documents
	 */
	public List<ThrudexException> removeList(List<Element> elements)
			throws ThrudexException, TException {
		
		List<ThrudexException> exList = new ArrayList<ThrudexException>();
		
		for(Element el : elements){
			try{
				remove(el);
			}catch(ThrudexException ex){
				ex.what += el.key;
				
				exList.add(ex);
			}
		}
		
		return exList;
	}

	public SearchResponse search(SearchQuery s) throws ThrudexException,
			TException {
		//make sure index is valid
		if(!indexMap.containsKey(s.index))
			throw new ThrudexExceptionImpl("No Index Found: "+s.index);	
		
		return indexMap.get(s.index).search(s);
	}

	public List<SearchResponse> searchList(List<SearchQuery> queries)
			throws ThrudexException, TException {
		
		List<SearchResponse> responses = new ArrayList<SearchResponse>();
		
		for(SearchQuery query : queries){
			responses.add(search(query));
		}
		
		return responses;
	}

}
