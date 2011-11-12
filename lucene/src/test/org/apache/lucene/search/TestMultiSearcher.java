package org.apache.lucene.search;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SetBasedFieldSelector;

import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.store.Directory;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * Tests {@link MultiSearcher} class.
 */
public class TestMultiSearcher extends LuceneTestCase
{

	/**
	 * ReturnS a new instance of the concrete MultiSearcher class
	 * used in this test.
	 */
	protected MultiSearcher getMultiSearcherInstance(Searcher[] searchers) throws IOException {
		return new MultiSearcher(searchers);
	}

    public void testEmptyIndex() throws Exception {
        // creating two directories for indices
        Directory indexStoreA = newDirectory();
        Directory indexStoreB = newDirectory();

        // creating a document to store
        Document lDoc = new Document();
        lDoc.add(newField("fulltext", "Once upon a time.....", Field.Store.YES, Field.Index.ANALYZED));
        lDoc.add(newField("id", "doc1", Field.Store.YES, Field.Index.NOT_ANALYZED));
        lDoc.add(newField("handle", "1", Field.Store.YES, Field.Index.NOT_ANALYZED));

        // creating a document to store
        Document lDoc2 = new Document();
        lDoc2.add(newField("fulltext", "in a galaxy far far away.....",
            Field.Store.YES, Field.Index.ANALYZED));
        lDoc2.add(newField("id", "doc2", Field.Store.YES, Field.Index.NOT_ANALYZED));
        lDoc2.add(newField("handle", "1", Field.Store.YES, Field.Index.NOT_ANALYZED));

        // creating a document to store
        Document lDoc3 = new Document();
        lDoc3.add(newField("fulltext", "a bizarre bug manifested itself....",
            Field.Store.YES, Field.Index.ANALYZED));
        lDoc3.add(newField("id", "doc3", Field.Store.YES, Field.Index.NOT_ANALYZED));
        lDoc3.add(newField("handle", "1", Field.Store.YES, Field.Index.NOT_ANALYZED));

        // creating an index writer for the first index
        IndexWriter writerA = new IndexWriter(indexStoreA, newIndexWriterConfig(TEST_VERSION_CURRENT, new StandardAnalyzer(TEST_VERSION_CURRENT)));
        // creating an index writer for the second index, but writing nothing
        IndexWriter writerB = new IndexWriter(indexStoreB, newIndexWriterConfig(TEST_VERSION_CURRENT, new StandardAnalyzer(TEST_VERSION_CURRENT)));

        //--------------------------------------------------------------------
        // scenario 1
        //--------------------------------------------------------------------

        // writing the documents to the first index
        writerA.addDocument(lDoc);
        writerA.addDocument(lDoc2);
        writerA.addDocument(lDoc3);
        writerA.forceMerge(1);
        writerA.close();

        // closing the second index
        writerB.close();

        // creating the query
        QueryParser parser = new QueryParser(TEST_VERSION_CURRENT, "fulltext", new StandardAnalyzer(TEST_VERSION_CURRENT));
        Query query = parser.parse("handle:1");

        // building the searchables
        Searcher[] searchers = new Searcher[2];
        // VITAL STEP:adding the searcher for the empty index first, before the searcher for the populated index
        searchers[0] = new IndexSearcher(indexStoreB, true);
        searchers[1] = new IndexSearcher(indexStoreA, true);
        // creating the multiSearcher
        Searcher mSearcher = getMultiSearcherInstance(searchers);
        // performing the search
        ScoreDoc[] hits = mSearcher.search(query, null, 1000).scoreDocs;

        assertEquals(3, hits.length);

        // iterating over the hit documents
        for (int i = 0; i < hits.length; i++) {
          mSearcher.doc(hits[i].doc);
        }
        mSearcher.close();


        //--------------------------------------------------------------------
        // scenario 2
        //--------------------------------------------------------------------

        // adding one document to the empty index
        writerB = new IndexWriter(indexStoreB, newIndexWriterConfig(
            TEST_VERSION_CURRENT, 
                new StandardAnalyzer(TEST_VERSION_CURRENT))
                .setOpenMode(OpenMode.APPEND));
        writerB.addDocument(lDoc);
        writerB.forceMerge(1);
        writerB.close();

        // building the searchables
        Searcher[] searchers2 = new Searcher[2];
        // VITAL STEP:adding the searcher for the empty index first, before the searcher for the populated index
        searchers2[0] = new IndexSearcher(indexStoreB, true);
        searchers2[1] = new IndexSearcher(indexStoreA, true);
        // creating the mulitSearcher
        MultiSearcher mSearcher2 = getMultiSearcherInstance(searchers2);
        // performing the same search
        ScoreDoc[] hits2 = mSearcher2.search(query, null, 1000).scoreDocs;

        assertEquals(4, hits2.length);

        // iterating over the hit documents
        for (int i = 0; i < hits2.length; i++) {
          // no exception should happen at this point
          mSearcher2.doc(hits2[i].doc);
        }

        // test the subSearcher() method:
        Query subSearcherQuery = parser.parse("id:doc1");
        hits2 = mSearcher2.search(subSearcherQuery, null, 1000).scoreDocs;
        assertEquals(2, hits2.length);
        assertEquals(0, mSearcher2.subSearcher(hits2[0].doc));   // hit from searchers2[0]
        assertEquals(1, mSearcher2.subSearcher(hits2[1].doc));   // hit from searchers2[1]
        subSearcherQuery = parser.parse("id:doc2");
        hits2 = mSearcher2.search(subSearcherQuery, null, 1000).scoreDocs;
        assertEquals(1, hits2.length);
        assertEquals(1, mSearcher2.subSearcher(hits2[0].doc));   // hit from searchers2[1]
        mSearcher2.close();

        //--------------------------------------------------------------------
        // scenario 3
        //--------------------------------------------------------------------

        // deleting the document just added, this will cause a different exception to take place
        Term term = new Term("id", "doc1");
        IndexReader readerB = IndexReader.open(indexStoreB, false);
        readerB.deleteDocuments(term);
        readerB.close();

        // optimizing the index with the writer
        writerB = new IndexWriter(indexStoreB, new IndexWriterConfig(
            TEST_VERSION_CURRENT, 
                new StandardAnalyzer(TEST_VERSION_CURRENT))
                .setOpenMode(OpenMode.APPEND));
        writerB.forceMerge(1);
        writerB.close();

        // building the searchables
        Searcher[] searchers3 = new Searcher[2];

        searchers3[0] = new IndexSearcher(indexStoreB, true);
        searchers3[1] = new IndexSearcher(indexStoreA, true);
        // creating the mulitSearcher
        Searcher mSearcher3 = getMultiSearcherInstance(searchers3);
        // performing the same search
        ScoreDoc[] hits3 = mSearcher3.search(query, null, 1000).scoreDocs;

        assertEquals(3, hits3.length);

        // iterating over the hit documents
        for (int i = 0; i < hits3.length; i++) {
          mSearcher3.doc(hits3[i].doc);
        }
        mSearcher3.close();
        indexStoreA.close();
        indexStoreB.close();
    }
    
    private Document createDocument(String contents1, String contents2) {
        Document document=new Document();
        
        document.add(newField("contents", contents1, Field.Store.YES, Field.Index.NOT_ANALYZED));
      document.add(newField("other", "other contents", Field.Store.YES, Field.Index.NOT_ANALYZED));
        if (contents2!=null) {
            document.add(newField("contents", contents2, Field.Store.YES, Field.Index.NOT_ANALYZED));
        }
        
        return document;
    }
    
    private void initIndex(Random random, Directory directory, int nDocs, boolean create, String contents2) throws IOException {
        IndexWriter indexWriter=null;
        
        try {
          indexWriter = new IndexWriter(directory, LuceneTestCase.newIndexWriterConfig(random,
              TEST_VERSION_CURRENT, new KeywordAnalyzer()).setOpenMode(
                  create ? OpenMode.CREATE : OpenMode.APPEND));
            
            for (int i=0; i<nDocs; i++) {
                indexWriter.addDocument(createDocument("doc" + i, contents2));
            }
        } finally {
            if (indexWriter!=null) {
                indexWriter.close();
            }
        }
    }

  public void testFieldSelector() throws Exception {
    Directory ramDirectory1, ramDirectory2;
    IndexSearcher indexSearcher1, indexSearcher2;

    ramDirectory1 = newDirectory();
    ramDirectory2 = newDirectory();
    Query query = new TermQuery(new Term("contents", "doc0"));

    // Now put the documents in a different index
    initIndex(random, ramDirectory1, 10, true, null); // documents with a single token "doc0", "doc1", etc...
    initIndex(random, ramDirectory2, 10, true, "x"); // documents with two tokens "doc0" and "x", "doc1" and x, etc...

    indexSearcher1 = new IndexSearcher(ramDirectory1, true);
    indexSearcher2 = new IndexSearcher(ramDirectory2, true);

    MultiSearcher searcher = getMultiSearcherInstance(new Searcher[]{indexSearcher1, indexSearcher2});
    assertTrue("searcher is null and it shouldn't be", searcher != null);
    ScoreDoc[] hits = searcher.search(query, null, 1000).scoreDocs;
    assertTrue("hits is null and it shouldn't be", hits != null);
    assertTrue(hits.length + " does not equal: " + 2, hits.length == 2);
    Document document = searcher.doc(hits[0].doc);
    assertTrue("document is null and it shouldn't be", document != null);
    assertTrue("document.getFields() Size: " + document.getFields().size() + " is not: " + 2, document.getFields().size() == 2);
    //Should be one document from each directory
    //they both have two fields, contents and other
    Set<String> ftl = new HashSet<String>();
    ftl.add("other");
    SetBasedFieldSelector fs = new SetBasedFieldSelector(ftl, Collections. <String> emptySet());
    document = searcher.doc(hits[0].doc, fs);
    assertTrue("document is null and it shouldn't be", document != null);
    assertTrue("document.getFields() Size: " + document.getFields().size() + " is not: " + 1, document.getFields().size() == 1);
    String value = document.get("contents");
    assertTrue("value is not null and it should be", value == null);
    value = document.get("other");
    assertTrue("value is null and it shouldn't be", value != null);
    ftl.clear();
    ftl.add("contents");
    fs = new SetBasedFieldSelector(ftl, Collections. <String> emptySet());
    document = searcher.doc(hits[1].doc, fs);
    value = document.get("contents");
    assertTrue("value is null and it shouldn't be", value != null);    
    value = document.get("other");
    assertTrue("value is not null and it should be", value == null);
    indexSearcher1.close();
    indexSearcher2.close();
    ramDirectory1.close();
    ramDirectory2.close();
    searcher.close();
  }

  /* uncomment this when the highest score is always normalized to 1.0, even when it was < 1.0
 public void testNormalization1() throws IOException {
     testNormalization(1, "Using 1 document per index:");
 }
  */
    
    public void testNormalization10() throws IOException {
        testNormalization(10, "Using 10 documents per index:");
    }
    
    private void testNormalization(int nDocs, String message) throws IOException {
        Query query=new TermQuery(new Term("contents", "doc0"));
        
        Directory ramDirectory1;
        IndexSearcher indexSearcher1;
        ScoreDoc[] hits;
        
        ramDirectory1=newDirectory();
        
        // First put the documents in the same index
        initIndex(random, ramDirectory1, nDocs, true, null); // documents with a single token "doc0", "doc1", etc...
        initIndex(random, ramDirectory1, nDocs, false, "x"); // documents with two tokens "doc0" and "x", "doc1" and x, etc...
        
        indexSearcher1=new IndexSearcher(ramDirectory1, true);
        indexSearcher1.setDefaultFieldSortScoring(true, true);
        
        hits=indexSearcher1.search(query, null, 1000).scoreDocs;
        
        assertEquals(message, 2, hits.length);
        
        // Store the scores for use later
        float[] scores={ hits[0].score, hits[1].score };
        
        assertTrue(message, scores[0] > scores[1]);
        
        indexSearcher1.close();
        ramDirectory1.close();
        hits=null;
        
        
        
        Directory ramDirectory2;
        IndexSearcher indexSearcher2;
        
        ramDirectory1=newDirectory();
        ramDirectory2=newDirectory();
        
        // Now put the documents in a different index
        initIndex(random, ramDirectory1, nDocs, true, null); // documents with a single token "doc0", "doc1", etc...
        initIndex(random, ramDirectory2, nDocs, true, "x"); // documents with two tokens "doc0" and "x", "doc1" and x, etc...
        
        indexSearcher1=new IndexSearcher(ramDirectory1, true);
        indexSearcher1.setDefaultFieldSortScoring(true, true);
        indexSearcher2=new IndexSearcher(ramDirectory2, true);
        indexSearcher2.setDefaultFieldSortScoring(true, true);
        
        Searcher searcher=getMultiSearcherInstance(new Searcher[] { indexSearcher1, indexSearcher2 });
        
        hits=searcher.search(query, null, 1000).scoreDocs;
        
        assertEquals(message, 2, hits.length);
        
        // The scores should be the same (within reason)
        assertEquals(message, scores[0], hits[0].score, 1e-6); // This will a document from ramDirectory1
        assertEquals(message, scores[1], hits[1].score, 1e-6); // This will a document from ramDirectory2
        
        
        
        // Adding a Sort.RELEVANCE object should not change anything
        hits=searcher.search(query, null, 1000, Sort.RELEVANCE).scoreDocs;
        
        assertEquals(message, 2, hits.length);
        
        assertEquals(message, scores[0], hits[0].score, 1e-6); // This will a document from ramDirectory1
        assertEquals(message, scores[1], hits[1].score, 1e-6); // This will a document from ramDirectory2
        
        searcher.close();
        
        ramDirectory1.close();
        ramDirectory2.close();
    }
    
    /**
     * test that custom similarity is in effect when using MultiSearcher (LUCENE-789).
     * @throws IOException 
     */
    public void testCustomSimilarity () throws IOException {
        Directory dir = newDirectory();
        initIndex(random, dir, 10, true, "x"); // documents with two tokens "doc0" and "x", "doc1" and x, etc...
        IndexSearcher srchr = new IndexSearcher(dir, true);
        MultiSearcher msrchr = getMultiSearcherInstance(new Searcher[]{srchr});
        
        Similarity customSimilarity = new DefaultSimilarity() {
            // overide all
            @Override
            public float idf(int docFreq, int numDocs) { return 100.0f; }
            @Override
            public float coord(int overlap, int maxOverlap) { return 1.0f; }
            @Override
            public float computeNorm(String fieldName, FieldInvertState state) { return state.getBoost(); }
            @Override
            public float queryNorm(float sumOfSquaredWeights) { return 1.0f; }
            @Override
            public float sloppyFreq(int distance) { return 1.0f; }
            @Override
            public float tf(float freq) { return 1.0f; }
        };
        
        srchr.setSimilarity(customSimilarity);
        msrchr.setSimilarity(customSimilarity);
  
        Query query=new TermQuery(new Term("contents", "doc0"));
  
        // Get a score from IndexSearcher
        TopDocs topDocs = srchr.search(query, null, 1);
        float score1 = topDocs.getMaxScore();
        
        // Get the score from MultiSearcher
        topDocs = msrchr.search(query, null, 1);
        float scoreN = topDocs.getMaxScore();
        
        // The scores from the IndexSearcher and Multisearcher should be the same
        // if the same similarity is used.
        assertEquals("MultiSearcher score must be equal to single searcher score!", score1, scoreN, 1e-6);
        msrchr.close();
        srchr.close();
        dir.close();
    }
    
    public void testDocFreq() throws IOException{
      Directory dir1 = newDirectory();
      Directory dir2 = newDirectory();

      initIndex(random, dir1, 10, true, "x"); // documents with two tokens "doc0" and "x", "doc1" and x, etc...
      initIndex(random, dir2, 5, true, "x"); // documents with two tokens "doc0" and "x", "doc1" and x, etc...
      IndexSearcher searcher1 = new IndexSearcher(dir1, true);
      IndexSearcher searcher2 = new IndexSearcher(dir2, true);
      
      MultiSearcher multiSearcher = getMultiSearcherInstance(new Searcher[]{searcher1, searcher2});
      assertEquals(15, multiSearcher.docFreq(new Term("contents","x")));
      multiSearcher.close();
      searcher1.close();
      searcher2.close();
      dir1.close();
      dir2.close();
    }
    
    public void testCreateDocFrequencyMap() throws IOException{
      Directory dir1 = newDirectory();
      Directory dir2 = newDirectory();
      Term template = new Term("contents") ;
      String[] contents  = {"a", "b", "c"};
      HashSet<Term> termsSet = new HashSet<Term>();
      for (int i = 0; i < contents.length; i++) {
        initIndex(random, dir1, i+10, i==0, contents[i]); 
        initIndex(random, dir2, i+5, i==0, contents[i]);
        termsSet.add(template.createTerm(contents[i]));
      }
      IndexSearcher searcher1 = new IndexSearcher(dir1, true);
      IndexSearcher searcher2 = new IndexSearcher(dir2, true);
      MultiSearcher multiSearcher = getMultiSearcherInstance(new Searcher[]{searcher1, searcher2});
      Map<Term,Integer> docFrequencyMap = multiSearcher.createDocFrequencyMap(termsSet);
      assertEquals(3, docFrequencyMap.size());
      for (int i = 0; i < contents.length; i++) {
        assertEquals(Integer.valueOf((i*2) +15), docFrequencyMap.get(template.createTerm(contents[i])));
      }
      multiSearcher.close();
      searcher1.close();
      searcher2.close();
      dir1.close();
      dir2.close();
    }
}
