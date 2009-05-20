package org.thrudb.thrudex.lucene;

import java.io.File;
import java.util.ArrayList;

import junit.framework.TestCase;

import org.apache.thrift.TException;
import org.thrudb.thrudex.Document;
import org.thrudb.thrudex.Field;
import org.thrudb.thrudex.FieldType;
import org.thrudb.thrudex.SearchQuery;
import org.thrudb.thrudex.SearchResponse;
import org.thrudb.thrudex.ThrudexException;

public class RealTimeLuceneIndexTests extends TestCase {

	final String INDEX_BASE_PATH = ".";
	final String INDEX_NAME = "test_index";
	ThrudexLuceneHandler index;

	public RealTimeLuceneIndexTests() {
		super();
	}

	@Override
	protected void setUp() throws Exception {
		super.setUp();

		// remove old index if exists
		File dir = new File(INDEX_BASE_PATH + "/" + INDEX_NAME);
		if (dir.isDirectory()) {
			for (String file : dir.list()) {
				File f = new File(dir, file);
				f.delete();
			}
			if (!dir.delete())
				fail("can't remove test index dir");
		}

		index = new ThrudexLuceneHandler(INDEX_BASE_PATH);
		index.addIndex(INDEX_NAME);

	}

	public void testKeywordSearch() {

		try {
			Document d1 = this.newDocument("doc1");
			this.addField(d1, "title", "title number 1", FieldType.TEXT);
			this.addField(d1, "category", "science_fiction", FieldType.UNSTORED);

			index.put(d1);

			SearchQuery search = new SearchQuery();
			search.setIndex(INDEX_NAME);
			
			search.setQuery("category:\"science_fiction\"");
			
			SearchResponse response = index.search(search);
			assertEquals(1, response.total);
			
			
			
		} catch (Throwable t) {
			//t.printStackTrace();
			fail(t.toString());
		} 
	}

	private Document newDocument(String key) {
		Document d = new Document();
		d.setIndex(INDEX_NAME);
		d.setKey(key);
		d.setFields(new ArrayList<Field>());

		return d;
	}

	private void addField(Document doc, String key, String value, int type) {
		Field field = new Field();
		field.setKey(key);
		field.setValue(value);
		field.setType(type);

		doc.getFields().add(field);
	}

}
