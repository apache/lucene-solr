package org.apache.lucene.search;

/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.io.Serializable;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;

/**
 * Unit test for sorting code.
 *
 */

public class TestCustomSearcherSort
extends TestCase
implements Serializable {

    private Directory index = null;
    private Query query = null;
    // reduced from 20000 to 2000 to speed up test...
    private final static int INDEX_SIZE = 2000;

	public TestCustomSearcherSort (String name) {
		super (name);
	}

	public static void main (String[] argv) {
	    TestRunner.run (suite());
	}

	public static Test suite() {
		return new TestSuite (TestCustomSearcherSort.class);
	}


	// create an index for testing
	private Directory getIndex()
	throws IOException {
	        RAMDirectory indexStore = new RAMDirectory ();
	        IndexWriter writer = new IndexWriter (indexStore, new StandardAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
	        RandomGen random = new RandomGen();
	        for (int i=0; i<INDEX_SIZE; ++i) { // don't decrease; if to low the problem doesn't show up
	        Document doc = new Document();
	            if((i%5)!=0) { // some documents must not have an entry in the first sort field
	                doc.add (new Field("publicationDate_", random.getLuceneDate(), Field.Store.YES, Field.Index.NOT_ANALYZED));
	            }
	            if((i%7)==0) { // some documents to match the query (see below) 
	                doc.add (new Field("content", "test", Field.Store.YES, Field.Index.ANALYZED));
	            }
	            // every document has a defined 'mandant' field
	            doc.add(new Field("mandant", Integer.toString(i%3), Field.Store.YES, Field.Index.NOT_ANALYZED));
	            writer.addDocument (doc);
	        }
	        writer.optimize ();
	        writer.close ();
	    return indexStore;
	}

	/**
	 * Create index and query for test cases. 
	 */
	public void setUp() throws Exception {
		index = getIndex();
	    query = new TermQuery( new Term("content", "test"));
	}

	/**
	 * Run the test using two CustomSearcher instances. 
	 */
	public void testFieldSortCustomSearcher() throws Exception {
	  // log("Run testFieldSortCustomSearcher");
		// define the sort criteria
	    Sort custSort = new Sort(new SortField[] {
	            new SortField("publicationDate_"), 
	            SortField.FIELD_SCORE
	    });
	    Searcher searcher = new CustomSearcher (index, 2);
	    // search and check hits
		matchHits(searcher, custSort);
	}
	/**
	 * Run the test using one CustomSearcher wrapped by a MultiSearcher. 
	 */
	public void testFieldSortSingleSearcher() throws Exception {
	  // log("Run testFieldSortSingleSearcher");
		// define the sort criteria
	    Sort custSort = new Sort(new SortField[] {
	            new SortField("publicationDate_"), 
	            SortField.FIELD_SCORE
	    });
	    Searcher searcher = 
	        new MultiSearcher(new Searchable[] {
	                new CustomSearcher (index, 2)});
	    // search and check hits
		matchHits(searcher, custSort);
	}
	/**
	 * Run the test using two CustomSearcher instances. 
	 */
	public void testFieldSortMultiCustomSearcher() throws Exception {
	  // log("Run testFieldSortMultiCustomSearcher");
		// define the sort criteria
	    Sort custSort = new Sort(new SortField[] {
	            new SortField("publicationDate_"), 
	            SortField.FIELD_SCORE
	    });
	    Searcher searcher = 
	        new MultiSearcher(new Searchable[] {
	                new CustomSearcher (index, 0),
	                new CustomSearcher (index, 2)});
	    // search and check hits
		matchHits(searcher, custSort);
	}


	// make sure the documents returned by the search match the expected list
	private void matchHits (Searcher searcher, Sort sort)
	throws IOException {
	    // make a query without sorting first
    ScoreDoc[] hitsByRank = searcher.search(query, null, 1000).scoreDocs;
		checkHits(hitsByRank, "Sort by rank: "); // check for duplicates
        Map resultMap = new TreeMap();
        // store hits in TreeMap - TreeMap does not allow duplicates; existing entries are silently overwritten
        for(int hitid=0;hitid<hitsByRank.length; ++hitid) {
            resultMap.put(
                    new Integer(hitsByRank[hitid].doc),  // Key:   Lucene Document ID
                    new Integer(hitid));				// Value: Hits-Objekt Index
        }
        
        // now make a query using the sort criteria
    ScoreDoc[] resultSort = searcher.search (query, null, 1000, sort).scoreDocs;
		checkHits(resultSort, "Sort by custom criteria: "); // check for duplicates
		
        String lf = System.getProperty("line.separator", "\n");
        // besides the sorting both sets of hits must be identical
        for(int hitid=0;hitid<resultSort.length; ++hitid) {
            Integer idHitDate = new Integer(resultSort[hitid].doc); // document ID from sorted search
            if(!resultMap.containsKey(idHitDate)) {
                log("ID "+idHitDate+" not found. Possibliy a duplicate.");
            }
            assertTrue(resultMap.containsKey(idHitDate)); // same ID must be in the Map from the rank-sorted search
            // every hit must appear once in both result sets --> remove it from the Map.
            // At the end the Map must be empty!
            resultMap.remove(idHitDate);
        }
        if(resultMap.size()==0) {
            // log("All hits matched");
        } else {
        log("Couldn't match "+resultMap.size()+" hits.");
        }
        assertEquals(resultMap.size(), 0);
	}

	/**
	 * Check the hits for duplicates.
	 * @param hits
	 */
    private void checkHits(ScoreDoc[] hits, String prefix) {
        if(hits!=null) {
            Map idMap = new TreeMap();
            for(int docnum=0;docnum<hits.length;++docnum) {
                Integer luceneId = null;

                luceneId = new Integer(hits[docnum].doc);
                if(idMap.containsKey(luceneId)) {
                    StringBuffer message = new StringBuffer(prefix);
                    message.append("Duplicate key for hit index = ");
                    message.append(docnum);
                    message.append(", previous index = ");
                    message.append(((Integer)idMap.get(luceneId)).toString());
                    message.append(", Lucene ID = ");
                    message.append(luceneId);
                    log(message.toString());
                } else { 
                    idMap.put(luceneId, new Integer(docnum));
                }
            }
        }
    }
    
    // Simply write to console - choosen to be independant of log4j etc 
    private void log(String message) {
        System.out.println(message);
    }
    
    public class CustomSearcher extends IndexSearcher {
        private int switcher;
        /**
         * @param directory
         * @throws IOException
         */
        public CustomSearcher(Directory directory, int switcher) throws IOException {
            super(directory);
            this.switcher = switcher;
        }
        /**
         * @param r
         */
        public CustomSearcher(IndexReader r, int switcher) {
            super(r);
            this.switcher = switcher;
        }
        /**
         * @param path
         * @throws IOException
         */
        public CustomSearcher(String path, int switcher) throws IOException {
            super(path);
            this.switcher = switcher;
        }
        /* (non-Javadoc)
         * @see org.apache.lucene.search.Searchable#search(org.apache.lucene.search.Query, org.apache.lucene.search.Filter, int, org.apache.lucene.search.Sort)
         */
        public TopFieldDocs search(Query query, Filter filter, int nDocs,
                Sort sort) throws IOException {
            BooleanQuery bq = new BooleanQuery();
            bq.add(query, BooleanClause.Occur.MUST);
            bq.add(new TermQuery(new Term("mandant", Integer.toString(switcher))), BooleanClause.Occur.MUST);
            return super.search(bq, filter, nDocs, sort);
        }
        /* (non-Javadoc)
         * @see org.apache.lucene.search.Searchable#search(org.apache.lucene.search.Query, org.apache.lucene.search.Filter, int)
         */
        public TopDocs search(Query query, Filter filter, int nDocs)
        throws IOException {
            BooleanQuery bq = new BooleanQuery();
            bq.add(query, BooleanClause.Occur.MUST);
            bq.add(new TermQuery(new Term("mandant", Integer.toString(switcher))), BooleanClause.Occur.MUST);
            return super.search(bq, filter, nDocs);
        }
    }
    private class RandomGen {
        private Random random = new Random(0); // to generate some arbitrary contents
	    private Calendar base = new GregorianCalendar(1980, 1, 1);

	    // Just to generate some different Lucene Date strings
        private String getLuceneDate() {
    	    return DateTools.timeToString(base.getTimeInMillis() + random.nextInt() - Integer.MIN_VALUE, DateTools.Resolution.DAY);
        }
    }
}
