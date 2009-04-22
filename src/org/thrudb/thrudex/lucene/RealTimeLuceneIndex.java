package org.thrudb.thrudex.lucene;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
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
import org.apache.lucene.search.ParallelMultiSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Searchable;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.thrudb.thrudex.Element;
import org.thrudb.thrudex.SearchQuery;
import org.thrudb.thrudex.SearchResponse;
import org.thrudb.thrudex.ThrudexException;
import org.thrudb.thrudex.ThrudexExceptionImpl;

public class RealTimeLuceneIndex implements LuceneIndex, Runnable {

	Analyzer      analyzer = new StandardAnalyzer();
	
	
	IndexWriter   ramWriter;
	IndexReader   ramReader;
	IndexSearcher ramSearcher;
	RAMDirectory  ramDirectory;
	
	IndexReader   prevRamReader;
	IndexSearcher prevRamSearcher;
	RAMDirectory  prevRamDirectory;
		
	IndexWriter   diskWriter;
	IndexReader   diskReader;
	IndexSearcher diskSearcher;
	Directory     diskDirectory;
	RealTimeDiskFilter diskFilter;
	Set<Term>     deletedDocuments; //disk only
	
	QueryParser   queryParser = new QueryParser(DOCUMENT_KEY,analyzer);
	AtomicBoolean hasWrite    = new AtomicBoolean(false);
	
	Thread  monitor;
	boolean shuttingdown = false;
	
	Logger logger = Logger.getLogger(getClass());
	
	
	RealTimeLuceneIndex(String indexRoot, String indexName) throws IOException {
		File rootFile = new File(indexRoot);
		
		if(!rootFile.isDirectory())
			throw new IOException("invalid index root: " + indexRoot);
		
		String indexLocation = indexRoot + "/" + indexName;
		
		boolean createIndex = !IndexReader.indexExists(indexLocation);
		
		if(createIndex){
			diskWriter   = new IndexWriter(indexLocation,analyzer,createIndex,IndexWriter.MaxFieldLength.UNLIMITED);			
			diskDirectory = FSDirectory.getDirectory(indexLocation);
		}else{
			diskDirectory = FSDirectory.getDirectory(indexLocation);
			
			if(IndexWriter.isLocked(indexLocation)){
				logger.warn("Removing lock on "+indexName);	
				IndexWriter.unlock(diskDirectory);
			}
			
			diskWriter   = new IndexWriter(diskDirectory,analyzer,IndexWriter.MaxFieldLength.UNLIMITED);	
		}
		
		//open this read only
		diskReader   = IndexReader.open(diskDirectory, true);
		diskSearcher = new IndexSearcher(diskReader);
		diskFilter   = new RealTimeDiskFilter(diskReader);
		deletedDocuments = new HashSet<Term>();
		
		//
		ramDirectory = new RAMDirectory();
		ramWriter    = new IndexWriter(ramDirectory,analyzer,true, IndexWriter.MaxFieldLength.UNLIMITED);
		ramReader    = IndexReader.open(ramDirectory,true);
		ramSearcher  = new IndexSearcher(ramReader);
		
		//Monitors the index
		monitor = new Thread(this);
		monitor.start();
		
	}
	
	public synchronized void put(String key, Document document) throws ThrudexException{
		
		Term term = new Term(DOCUMENT_KEY,key);
		
		try{
			
			ramWriter.updateDocument(term, document,analyzer);
	
			if(diskFilter.hideTerm(term))
				deletedDocuments.add(term);
					
			hasWrite.set(true);
			
		}catch(IOException e){
			throw new ThrudexExceptionImpl(e.toString());
		}
	}

	public synchronized void remove(String key) throws ThrudexException {
		
		Term term = new Term(DOCUMENT_KEY, key);
		
		try{
			ramWriter.deleteDocuments(term);
			hasWrite.set(true);
			
			if(diskFilter.hideTerm(term))
				deletedDocuments.add(term);
			
			
		}catch(IOException e){
			throw new ThrudexExceptionImpl(e.toString());
		}
	}

	public SearchResponse search(SearchQuery query) throws ThrudexException {
		if(!query.isSetQuery() || query.query.trim().equals(""))
			throw new ThrudexExceptionImpl("Empty Query");
		
		//Parse Query
		Query parsedQuery;
		
		
		//Construct the multiSearcher
		ParallelMultiSearcher multiSearcher = null;
		RealTimeDiskFilter    myFilter      = null;
		try{

			//This section needs to be thread safe
			synchronized(this){
						
				//Commit any prev writes
				if(hasWrite.getAndSet(false)){
					ramWriter.commit();
					
					//Reopen index reader
					IndexReader newReader = ramReader.reopen();
					if(ramReader != newReader){	
						//ramReader.close();
						ramSearcher.close();
						ramReader   = newReader;			
						ramSearcher = new IndexSearcher(ramReader);
					}				
				}
				
				
				Searchable searchers[];
				
		
				if(prevRamSearcher == null)
					searchers = new Searchable[]{ramSearcher, diskSearcher};
				else
					searchers = new Searchable[]{ramSearcher,prevRamSearcher,diskSearcher};
				
				multiSearcher = new ParallelMultiSearcher(searchers);
				
				myFilter      = diskFilter;
			
			
				//parse query
				//TODO: Cache?
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
			TopFieldDocs result = multiSearcher.search(parsedQuery,myFilter,query.offset + query.limit,sortBy);
			
			SearchResponse response = new SearchResponse();
			response.setTotal(result.totalHits);
			
			FieldSelector fieldSelector;
			if(query.isPayload()){
				fieldSelector = new MapFieldSelector(new String[]{DOCUMENT_KEY,PAYLOAD_KEY});
			}else{
				fieldSelector = new MapFieldSelector(new String[]{DOCUMENT_KEY});
			}
			
			
			for(int i=query.offset; i<result.totalHits && i<(query.offset + query.limit); i++){
				
				Element el = new Element();
				el.setIndex(query.index);
										
				
				Document d = multiSearcher.doc(result.scoreDocs[i].doc,fieldSelector);
				el.setKey(d.get(DOCUMENT_KEY));
				
				if(query.isSetPayload() && query.payload)
					el.setPayload(d.get(PAYLOAD_KEY));
			
				response.addToElements(el);
			}
			
			return response;
			
		}catch(IOException e){
			throw new ThrudexException(e.toString());
		}
			
	}
	
	
	public void run() {
		while(true){
			try{
			
				logger.debug("ram dir size: "+ramDirectory.sizeInBytes());
				
				//do nothing until we have enough changes
				if(ramDirectory.sizeInBytes() < 1024*1024*1){	
					Thread.currentThread().sleep(10000);
					continue;
				}
				
				
				
				//We need to merge the indexes together and reopen
				synchronized(this){
					prevRamDirectory = ramDirectory;
					prevRamReader    = ramReader;
					prevRamSearcher  = ramSearcher;			
					IndexWriter prevRamWriter    = ramWriter;
					
					//
					ramDirectory = new RAMDirectory();
					ramWriter    = new IndexWriter(ramDirectory,analyzer,true, IndexWriter.MaxFieldLength.UNLIMITED);
					ramReader    = IndexReader.open(ramDirectory,true);
					ramSearcher  = new IndexSearcher(ramReader);
					
					//Commit any prev writes
					hasWrite.getAndSet(false);
					prevRamWriter.commit();
					prevRamWriter.close(); //done forever
						
					//Reopen index reader
					IndexReader newReader = prevRamReader.reopen();
					if(prevRamReader != prevRamReader){	
						prevRamReader.close();
						prevRamSearcher.close();
						prevRamReader = newReader;			
						prevRamSearcher = new IndexSearcher(prevRamReader);
					}						
				}
				
				
				//Now write the changes to disk
				synchronized(this){
					logger.debug("deleted "+deletedDocuments.size()+" documents");
				
					for(Term term : deletedDocuments){
						diskWriter.deleteDocuments(term);
					}
					
					deletedDocuments.clear();					
				}
				
				logger.debug("Writing "+prevRamReader.numDocs() + " docs to disk");
				//now merge the indexes
				
				diskWriter.addIndexesNoOptimize(new Directory[]{prevRamDirectory});
				
				synchronized(this){			
					
					//any new disk updates?
					for(Term term : deletedDocuments){
						diskWriter.deleteDocuments(term);
					}
					
					deletedDocuments.clear();
					
					diskWriter.commit();
					
					diskReader   = diskReader.reopen();
					diskSearcher = new IndexSearcher(diskReader);
					diskFilter   = new RealTimeDiskFilter(diskReader);
					
					logger.debug("Have "+diskReader.numDocs()+" docs on disk");
					
					prevRamSearcher = null;
					prevRamReader   = null;
					prevRamDirectory= null;
					
				}
				
				logger.debug("finsihed updating disk");
				
			}catch(Exception e){
				logger.info(e);
			}
		}
	}
	
	public synchronized void shutdown() {
		shuttingdown = true;
		try{
			monitor.wait();
		}catch(InterruptedException e){
			logger.error(e);
		}
	}
	
	public void optimize() throws ThrudexException{
		/*try{
			this.writer.optimize();
		}catch(IOException e){
			throw new ThrudexException(e.toString());
		}*/
	}


}
