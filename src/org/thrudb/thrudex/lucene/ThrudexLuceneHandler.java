package org.thrudb.thrudex.lucene;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.analysis.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.analysis.StopAnalyzer;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.thrift.TException;
import org.thrudb.thrudex.Document;
import org.thrudb.thrudex.Element;
import org.thrudb.thrudex.Field;
import org.thrudb.thrudex.SearchQuery;
import org.thrudb.thrudex.SearchResponse;
import org.thrudb.thrudex.ThrudexBackendType;
import org.thrudb.thrudex.ThrudexException;
import org.thrudb.thrudex.ThrudexExceptionImpl;
import org.thrudb.thrudex.Thrudex.Iface;

/**
 * Manages a set of lucene indexes. We keep this one lucene backend per index
 * 
 * @author jake
 * 
 */
public class ThrudexLuceneHandler implements Iface {

    private volatile Map<Integer, Analyzer> analyzers = new HashMap<Integer, Analyzer>();
    private Logger logger = Logger.getLogger(this.getClass().getSimpleName());
    private volatile Map<String, LuceneIndex> indexMap = new HashMap<String, LuceneIndex>();
    private String indexRoot;
    private ThrudexBackendType backendType;

    public ThrudexLuceneHandler(String indexRoot, ThrudexBackendType backendType) {
        this.indexRoot = indexRoot;
        this.backendType = backendType;

        Runtime.getRuntime().addShutdownHook(new IndexShutdownHandler(indexMap));
    }

    public synchronized String admin(String op, String data) throws ThrudexException, TException {

        if (op.equals("create_index"))
            addIndex(data);

        if (op.equals("optimize")) {
            if (indexMap.containsKey(data)) {
                try {
                    indexMap.get(data).optimize();
                } catch (ThrudexException e) {
                    return e.toString();
                }
            }
        }

        if (op.equals("shutdown")) {
            if (indexMap.containsKey(data)) {
                indexMap.get(data).shutdown();
                indexMap.remove(data);
            }
        }

        return "ok";
    }

    public synchronized void addIndex(String name) throws ThrudexException {

        if (name == null || name.trim().equals(""))
            return;

        if (indexMap.containsKey(name))
            return;

        try {
            switch (backendType) {
            case SIMPLE:
                indexMap.put(name, new SimpleLuceneIndex(indexRoot, name));
                break;
            case REALTIME:
                indexMap.put(name, new RealTimeLuceneIndex(indexRoot, name));
                break;
            case LUCANDRA:
                indexMap.put(name, new LucandraIndex(name));
                break;
            }
        } catch (IOException e) {
            throw new ThrudexException(e.getLocalizedMessage());
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

        // make sure index is valid
        if (!isValidIndex(d.index))
            throw new ThrudexExceptionImpl("No Index Found: " + d.index);

        // make sure document has a key
        if (!d.isSetKey() || d.key.trim().equals(""))
            throw new ThrudexExceptionImpl("No Document key found");

        // Start new lucene document
        org.apache.lucene.document.Document luceneDocument = new org.apache.lucene.document.Document();

        luceneDocument.add(new org.apache.lucene.document.Field(LuceneIndex.DOCUMENT_KEY, d.key, org.apache.lucene.document.Field.Store.YES,
                org.apache.lucene.document.Field.Index.NO));

        // Start analyzer
        Analyzer defaultAnalyzer = getAnalyzer(org.thrudb.thrudex.Analyzer.STANDARD);
        PerFieldAnalyzerWrapper qAnalyzer = new PerFieldAnalyzerWrapper(defaultAnalyzer);

        // Add fields
        for (Field field : d.fields) {

            if (!field.isSetKey())
                throw new ThrudexExceptionImpl("Field key not set");

            // Convert Field store type to Lucene type
            org.apache.lucene.document.Field.Store fieldStoreType;
            if (field.isStore())
                fieldStoreType = org.apache.lucene.document.Field.Store.YES;
            else
                fieldStoreType = org.apache.lucene.document.Field.Store.NO;

            // Create Lucene Field
            org.apache.lucene.document.Field luceneField = new org.apache.lucene.document.Field(field.key, field.value, fieldStoreType,
                    org.apache.lucene.document.Field.Index.ANALYZED);

            if (field.isSetWeight())
                luceneField.setBoost(field.weight);

            luceneDocument.add(luceneField);

            // Create sortable field?
            if (field.isSetSortable() && field.sortable) {

                luceneDocument.add(new org.apache.lucene.document.Field(field.key + "_sort", field.value, org.apache.lucene.document.Field.Store.YES,
                        org.apache.lucene.document.Field.Index.NOT_ANALYZED));
            }

            // Add field specific analyzer to qAnalyzer
            qAnalyzer.addAnalyzer(field.key, getAnalyzer(field.getAnalyzer()));
        }

        // Add payload
        if (d.isSetPayload()) {
            luceneDocument.add(new org.apache.lucene.document.Field(LuceneIndex.PAYLOAD_KEY, d.payload, org.apache.lucene.document.Field.Store.YES,
                    org.apache.lucene.document.Field.Index.NO));
        }

        // Document is not ready to put into the index
        indexMap.get(d.index).put(d.key, luceneDocument, qAnalyzer);
    }

    /**
     * Adds a list of documents to an index
     * 
     * Rather than returning on any error, this code captures any errors for
     * specific documents and puts them into a list
     */
    public List<ThrudexException> putList(List<Document> documents) throws ThrudexException, TException {

        List<ThrudexException> exList = new ArrayList<ThrudexException>();

        for (Document document : documents) {
            try {
                put(document);
            } catch (ThrudexException ex) {

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

        // make sure index is valid
        if (!isValidIndex(el.index))
            throw new ThrudexExceptionImpl("No Index Found: " + el.index);

        // make sure document has a key
        if (!el.isSetKey() || el.key.trim().equals(""))
            throw new ThrudexExceptionImpl("No Document key found");

        indexMap.get(el.index).remove(el.key);
    }

    /**
     * Removes a set of documents.
     * 
     * Captures any errors for sub-documents
     */
    public List<ThrudexException> removeList(List<Element> elements) throws ThrudexException, TException {

        List<ThrudexException> exList = new ArrayList<ThrudexException>();

        for (Element el : elements) {
            try {
                remove(el);
            } catch (ThrudexException ex) {
                ex.what += el.key;

                exList.add(ex);
            }
        }

        return exList;
    }

    public SearchResponse search(SearchQuery s) throws ThrudexException, TException {
        // make sure index is valid
        if (!isValidIndex(s.index))
            throw new ThrudexExceptionImpl("No Index Found: " + s.index);

        long start = System.currentTimeMillis();

        // Build the query analyzer
        Analyzer defaultAnalyzer = getAnalyzer(s.getDefaultAnalyzer());
        PerFieldAnalyzerWrapper qAnalyzer = new PerFieldAnalyzerWrapper(defaultAnalyzer);
        if (s.isSetFieldAnalyzers()) {
            for (String field : s.fieldAnalyzers.keySet())
                qAnalyzer.addAnalyzer(field, getAnalyzer(s.fieldAnalyzers.get(field)));
        }

        SearchResponse r = indexMap.get(s.index).search(s, qAnalyzer);

        long end = System.currentTimeMillis();

        logger.info("Search took: " + (end - start) + "ms");

        return r;
    }

    public List<SearchResponse> searchList(List<SearchQuery> queries) throws ThrudexException, TException {

        List<SearchResponse> responses = new ArrayList<SearchResponse>();

        for (SearchQuery query : queries) {
            responses.add(search(query));
        }

        return responses;
    }

    public boolean isValidIndex(String indexName) throws ThrudexException {
        if (indexMap.containsKey(indexName))
            return true;

        synchronized (indexMap) {

            // double lock check
            if (indexMap.containsKey(indexName))
                return true;

            if (backendType != ThrudexBackendType.LUCANDRA) {
                String indexLocation = indexRoot + "/" + indexName;

                if (IndexReader.indexExists(indexLocation)) {
                    addIndex(indexName); // really just reopening
                    return true;
                } else {
                    return false;
                }
            } else {
                addIndex(indexName);
                return true;
            }
        }
    }

    protected Analyzer getAnalyzer(int analyzerType) throws ThrudexException {
        Analyzer analyzer = analyzers.get(analyzerType);
        if (analyzer == null) {

            synchronized (analyzers) {

                // double lock check
                if ((analyzer = analyzers.get(analyzerType)) != null)
                    return analyzer;

                switch (analyzerType) {
                case org.thrudb.thrudex.Analyzer.STANDARD:
                    analyzer = new StandardAnalyzer();
                    break;
                case org.thrudb.thrudex.Analyzer.KEYWORD:
                    analyzer = new KeywordAnalyzer();
                    break;
                case org.thrudb.thrudex.Analyzer.SIMPLE:
                    analyzer = new SimpleAnalyzer();
                    break;
                case org.thrudb.thrudex.Analyzer.STOP:
                    analyzer = new StopAnalyzer();
                    break;
                case org.thrudb.thrudex.Analyzer.WHITESPACE:
                    analyzer = new WhitespaceAnalyzer();
                    break;
                default:
                    throw new ThrudexExceptionImpl("Unknown QueryAnalyzer: " + analyzerType);
                }
                analyzers.put(analyzerType, analyzer);
            }
        }
        return (analyzer);
    }

}
