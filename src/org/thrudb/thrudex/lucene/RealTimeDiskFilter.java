package org.thrudb.thrudex.lucene;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.OpenBitSet;

public class RealTimeDiskFilter extends Filter {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private IndexReader diskReader = null;
	private OpenBitSet  diskFilter = null;
	private Map<Term,Boolean>   termSet    = null;
	private Logger logger = Logger.getLogger(getClass());
	
	
	public RealTimeDiskFilter(IndexReader diskReader) {
		this.diskReader = diskReader;
		diskFilter      = new OpenBitSet(diskReader.maxDoc());
		diskFilter.set(0, diskReader.maxDoc());
		termSet = new HashMap<Term,Boolean>();
	}
	
	
	@Override
	public DocIdSet getDocIdSet(IndexReader reader) throws IOException {
			
		if(reader != diskReader){
			OpenBitSet rset = new OpenBitSet(reader.maxDoc());
			rset.set(0, reader.maxDoc());
		
			return rset;
		}
		
		return diskFilter;		
	}
	
	public boolean hideTerm(Term term) throws IOException {
		
		//check we haven't hidden already
		if(termSet.containsKey(term))
			return termSet.get(term).booleanValue();
		
		
		//Find terms and filter them out
		TermDocs termDocs = diskReader.termDocs(term);
	
		if(termDocs == null){
			//add this term to the termSet
			termSet.put(term, new Boolean(false));
			
			return false;
		}

		boolean found = false;
		
		while(termDocs.next()){
			diskFilter.clear(termDocs.doc());
			found = true;
		}
	
		//add this term to the termSet
		termSet.put(term, new Boolean(found));
		return found;		
	}
	
	public Set<Term> getTermSet(){
		Set<Term> tmpTermSet = new HashSet<Term>();
		
		for(Map.Entry<Term, Boolean> e : termSet.entrySet()){
			if(e.getValue()){
				tmpTermSet.add(e.getKey());
			}
		}
		
		return tmpTermSet;
	}

}
