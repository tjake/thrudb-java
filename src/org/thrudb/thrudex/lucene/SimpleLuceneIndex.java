package org.thrudb.thrudex.lucene;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.document.MapFieldSelector;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.thrudb.thrudex.Element;
import org.thrudb.thrudex.SearchQuery;
import org.thrudb.thrudex.SearchResponse;
import org.thrudb.thrudex.ThrudexException;
import org.thrudb.thrudex.ThrudexExceptionImpl;

/**
 * Very straight forward implementation of lucene api
 *
 */
public class SimpleLuceneIndex implements LuceneIndex {

	Analyzer      analyzer = new StandardAnalyzer();
	IndexWriter   writer;
	IndexReader   reader;
	IndexSearcher searcher;
	Directory     directory;
	QueryParser   queryParser = new QueryParser(DOCUMENT_KEY,analyzer);
	AtomicBoolean hasWrite = new AtomicBoolean(false);
	
	Logger logger = Logger.getLogger(getClass());
	
	public SimpleLuceneIndex(String indexRoot, String indexName) throws IOException {
		File rootFile = new File(indexRoot);
		
		if(!rootFile.isDirectory())
			throw new IOException("invalid index root: " + indexRoot);
		
		String indexLocation = indexRoot +"/" + indexName;
		
		boolean createIndex = !IndexReader.indexExists(indexLocation);
		
		if(createIndex){
			writer   = new IndexWriter(indexLocation,analyzer,createIndex,IndexWriter.MaxFieldLength.UNLIMITED);			
			directory = FSDirectory.getDirectory(indexLocation);
		}else{
			directory = FSDirectory.getDirectory(indexLocation);
			
			if(IndexWriter.isLocked(indexLocation)){
				System.err.println("Removing lock on "+indexName);	
				IndexWriter.unlock(directory);
			}
			
			writer   = new IndexWriter(directory,analyzer,IndexWriter.MaxFieldLength.UNLIMITED);	
		}
		
		//open this read only
		reader   = IndexReader.open(directory, true);
		searcher = new IndexSearcher(reader);
	}
	
	public void put(String key, Document document, Analyzer analyzer) throws ThrudexException{
		
		Term term = new Term(DOCUMENT_KEY,key);
		
		try{
			
			writer.updateDocument(term, document, analyzer);
		
			hasWrite.set(true);
			
		}catch(IOException e){
			throw new ThrudexExceptionImpl(e.toString());
		}
	}

	public void remove(String key) throws ThrudexException {
		
		Term term = new Term(DOCUMENT_KEY, key);
		
		try{
			writer.deleteDocuments(term);
			hasWrite.set(true);
			
		}catch(IOException e){
			throw new ThrudexExceptionImpl(e.toString());
		}
	}

	public SearchResponse search(SearchQuery query, Analyzer analyzer) throws ThrudexException {
		
		if(!query.isSetQuery() || query.query.trim().equals(""))
			throw new ThrudexExceptionImpl("Empty Query");
		
		//Parse Query
		Query parsedQuery;
		
		//MySearcher represents the searcher instance we'll use
		//for the duration of this function call, since other threads
		//may change searcher on us...
		IndexSearcher mySearcher;
		try{

			//This section needs to be thread safe
			synchronized(this){
						
				//Commit any prev writes
				if(hasWrite.getAndSet(false)){
					writer.commit();
			
					//Reopen index reader
					IndexReader newReader = reader.reopen();
					if(reader != newReader){	
						//reader.close();
						searcher.close();
						reader   = newReader;			
						searcher = new IndexSearcher(reader);
					}				
				}
				
				mySearcher = searcher;
				
				try{
					parsedQuery = queryParser.parse(query.getQuery());
				}catch(org.apache.lucene.queryParser.ParseException e){
					throw new ThrudexExceptionImpl(e.toString());
				}
			}
		
			//Set Sort
			Sort    sortBy = new Sort();
			
			if(query.isSetSortby() && !query.sortby.trim().equals(""))
				sortBy.setSort(query.getSortby() + "_sort", query.desc);
		
			
			//Search		
			TopFieldDocs result = mySearcher.search(parsedQuery,null,query.offset + query.limit,sortBy);
			
			
			SearchResponse response = new SearchResponse(result.totalHits,new ArrayList<Element>(),null);
	          
			
			FieldSelector fieldSelector;
			if(query.isPayload()){
				fieldSelector = new MapFieldSelector(new String[]{DOCUMENT_KEY,PAYLOAD_KEY});
			}else{
				fieldSelector = new MapFieldSelector(new String[]{DOCUMENT_KEY});
			}
			
			
			for(int i=query.offset; i<result.totalHits && i<(query.offset + query.limit); i++){
				
				Element el = new Element();
				el.setIndex(query.index);
										
				
				Document d = mySearcher.doc(result.scoreDocs[i].doc,fieldSelector);
				el.setKey(d.get(DOCUMENT_KEY));
				
				if(query.isSetPayload() && query.payload)
					el.setPayload(d.get(PAYLOAD_KEY));
			
				response.getElements().add(el);
			}
			
			return response;
			
		}catch(IOException e){
			throw new ThrudexException(e.toString());
		}
			
	}
	
	public void optimize() throws ThrudexException{
		try{
			this.writer.optimize();
		}catch(IOException e){
			throw new ThrudexException(e.toString());
		}
	}
	
	public void shutdown() {
		
	}

    
    public String getPayload(String key) throws ThrudexException {
        // TODO Auto-generated method stub
        return null;
    }

}
