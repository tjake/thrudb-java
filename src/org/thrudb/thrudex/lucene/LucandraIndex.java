package org.thrudb.thrudex.lucene;

import java.io.IOException;
import java.util.ArrayList;

import lucandra.IndexReader;
import lucandra.IndexWriter;

import org.apache.cassandra.service.Cassandra;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.document.MapFieldSelector;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopFieldDocs;
import org.thrudb.thrudex.Element;
import org.thrudb.thrudex.SearchQuery;
import org.thrudb.thrudex.SearchResponse;
import org.thrudb.thrudex.ThrudexException;
import org.thrudb.thrudex.ThrudexExceptionImpl;

public class LucandraIndex implements LuceneIndex {

    private final Cassandra.Client cassandraClient;
    private final String indexName;

    private static Logger logger = Logger.getLogger(LucandraIndex.class);
    // private final IndexReader indexReader;
    private final IndexWriter indexWriter;

    public LucandraIndex(Cassandra.Client cassandraClient, String indexName) {
        this.cassandraClient = cassandraClient;

        this.indexName = indexName;

        // this.indexReader = new IndexReader(indexName, cassandraClient);
        this.indexWriter = new IndexWriter(indexName, cassandraClient);

    }

    @Override
    public void optimize() throws ThrudexException {

    }

    @Override
    public void put(String key, Document document, Analyzer analyzer) throws ThrudexException {
        try {
            indexWriter.addDocument(document, analyzer);
        } catch (IOException e) {
            throw new ThrudexException(e.getLocalizedMessage());
        }
    }

    @Override
    public void remove(String key) throws ThrudexException {

    }

    @Override
    public SearchResponse search(SearchQuery query, Analyzer analyzer) throws ThrudexException {
        
        long start = System.currentTimeMillis();
        
        IndexReader indexReader = new IndexReader(indexName, cassandraClient);

        if (!query.isSetQuery() || query.query.trim().equals(""))
            throw new ThrudexExceptionImpl("Empty Query");

        // Parse Query
        Query parsedQuery;

        try {
            IndexSearcher mySearcher = new IndexSearcher(indexReader);
            QueryParser queryParser = new QueryParser(DOCUMENT_KEY, analyzer);

            try {
                parsedQuery = queryParser.parse(query.getQuery());
            } catch (org.apache.lucene.queryParser.ParseException e) {
                throw new ThrudexExceptionImpl(e.toString());
            }

            // Set Sort
            Sort sortBy = new Sort();

            if (query.isSetSortby() && !query.sortby.trim().equals(""))
                sortBy.setSort(query.getSortby() + "_sort", query.desc);

            long end = System.currentTimeMillis();
            logger.info("Search setup took: "+(end-start)+"ms");
            
            // Search
            start = System.currentTimeMillis();
            TopFieldDocs result = mySearcher.search(parsedQuery, null, query.offset + query.limit, sortBy);

            end = System.currentTimeMillis();
            logger.info("Search took: "+(end-start)+"ms");
            
            SearchResponse response = new SearchResponse(result.totalHits,new ArrayList<Element>(),null);
           
            FieldSelector fieldSelector;
            if (query.isPayload()) {
                fieldSelector = new MapFieldSelector(new String[] { DOCUMENT_KEY, PAYLOAD_KEY });
            } else {
                fieldSelector = new MapFieldSelector(new String[] { DOCUMENT_KEY });
            }

            for (int i = query.offset; i < result.totalHits && i < (query.offset + query.limit); i++) {

                Element el = new Element();
                el.setIndex(query.index);

                Document d = mySearcher.doc(result.scoreDocs[i].doc, fieldSelector);
                
                el.setKey(new String(d.getBinaryValue(DOCUMENT_KEY),"UTF-8"));
                //el.setKey(d.get(DOCUMENT_KEY));

                if (query.isSetPayload() && query.payload)
                    el.setPayload(new String(d.getBinaryValue(PAYLOAD_KEY),"UTF-8"));

               
                response.getElements().add(el);
            }

            return response;

        } catch (IOException e) {
            throw new ThrudexException(e.toString());
        }
    }

    @Override
    public void shutdown() {

    }

}
