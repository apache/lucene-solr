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

import java.io.IOException;
import java.text.Collator;
import java.util.Locale;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.store.RAMDirectory;

/**
 * A basic 'positive' Unit test class for the RangeFilter class.
 *
 * <p>
 * NOTE: at the moment, this class only tests for 'positive' results,
 * it does not verify the results to ensure there are no 'false positives',
 * nor does it adequately test 'negative' results.  It also does not test
 * that garbage in results in an Exception.
 */
public class TestRangeFilter extends BaseTestRangeFilter {

    public TestRangeFilter(String name) {
	super(name);
    }
    public TestRangeFilter() {
        super();
    }

    public void testRangeFilterId() throws IOException {

        IndexReader reader = IndexReader.open(signedIndex.index);
	IndexSearcher search = new IndexSearcher(reader);

        int medId = ((maxId - minId) / 2);
        
        String minIP = pad(minId);
        String maxIP = pad(maxId);
        String medIP = pad(medId);
    
        int numDocs = reader.numDocs();
        
        assertEquals("num of docs", numDocs, 1+ maxId - minId);
        
  ScoreDoc[] result;
        Query q = new TermQuery(new Term("body","body"));

        // test id, bounded on both ends
        
  result = search.search(q,new RangeFilter("id",minIP,maxIP,T,T), numDocs).scoreDocs;
  assertEquals("find all", numDocs, result.length);

  result = search.search(q,new RangeFilter("id",minIP,maxIP,T,F), numDocs).scoreDocs;
  assertEquals("all but last", numDocs-1, result.length);

  result = search.search(q,new RangeFilter("id",minIP,maxIP,F,T), numDocs).scoreDocs;
  assertEquals("all but first", numDocs-1, result.length);
        
  result = search.search(q,new RangeFilter("id",minIP,maxIP,F,F), numDocs).scoreDocs;
        assertEquals("all but ends", numDocs-2, result.length);
    
        result = search.search(q,new RangeFilter("id",medIP,maxIP,T,T), numDocs).scoreDocs;
        assertEquals("med and up", 1+ maxId-medId, result.length);
        
        result = search.search(q,new RangeFilter("id",minIP,medIP,T,T), numDocs).scoreDocs;
        assertEquals("up to med", 1+ medId-minId, result.length);

        // unbounded id

  result = search.search(q,new RangeFilter("id",minIP,null,T,F), numDocs).scoreDocs;
  assertEquals("min and up", numDocs, result.length);

  result = search.search(q,new RangeFilter("id",null,maxIP,F,T), numDocs).scoreDocs;
  assertEquals("max and down", numDocs, result.length);

  result = search.search(q,new RangeFilter("id",minIP,null,F,F), numDocs).scoreDocs;
  assertEquals("not min, but up", numDocs-1, result.length);
        
  result = search.search(q,new RangeFilter("id",null,maxIP,F,F), numDocs).scoreDocs;
  assertEquals("not max, but down", numDocs-1, result.length);
        
        result = search.search(q,new RangeFilter("id",medIP,maxIP,T,F), numDocs).scoreDocs;
        assertEquals("med and up, not max", maxId-medId, result.length);
        
        result = search.search(q,new RangeFilter("id",minIP,medIP,F,T), numDocs).scoreDocs;
        assertEquals("not min, up to med", medId-minId, result.length);

        // very small sets

  result = search.search(q,new RangeFilter("id",minIP,minIP,F,F), numDocs).scoreDocs;
  assertEquals("min,min,F,F", 0, result.length);
  result = search.search(q,new RangeFilter("id",medIP,medIP,F,F), numDocs).scoreDocs;
  assertEquals("med,med,F,F", 0, result.length);
  result = search.search(q,new RangeFilter("id",maxIP,maxIP,F,F), numDocs).scoreDocs;
  assertEquals("max,max,F,F", 0, result.length);
                     
  result = search.search(q,new RangeFilter("id",minIP,minIP,T,T), numDocs).scoreDocs;
  assertEquals("min,min,T,T", 1, result.length);
  result = search.search(q,new RangeFilter("id",null,minIP,F,T), numDocs).scoreDocs;
  assertEquals("nul,min,F,T", 1, result.length);

  result = search.search(q,new RangeFilter("id",maxIP,maxIP,T,T), numDocs).scoreDocs;
  assertEquals("max,max,T,T", 1, result.length);
  result = search.search(q,new RangeFilter("id",maxIP,null,T,F), numDocs).scoreDocs;
  assertEquals("max,nul,T,T", 1, result.length);

  result = search.search(q,new RangeFilter("id",medIP,medIP,T,T), numDocs).scoreDocs;
  assertEquals("med,med,T,T", 1, result.length);
        
    }

    public void testRangeFilterIdCollating() throws IOException {

        IndexReader reader = IndexReader.open(signedIndex.index);
        IndexSearcher search = new IndexSearcher(reader);

        Collator c = Collator.getInstance(Locale.ENGLISH);

        int medId = ((maxId - minId) / 2);

        String minIP = pad(minId);
        String maxIP = pad(maxId);
        String medIP = pad(medId);

        int numDocs = reader.numDocs();

        assertEquals("num of docs", numDocs, 1+ maxId - minId);

        Hits result;
        Query q = new TermQuery(new Term("body","body"));

        // test id, bounded on both ends

        result = search.search(q,new RangeFilter("id",minIP,maxIP,T,T,c));
        assertEquals("find all", numDocs, result.length());

        result = search.search(q,new RangeFilter("id",minIP,maxIP,T,F,c));
        assertEquals("all but last", numDocs-1, result.length());

        result = search.search(q,new RangeFilter("id",minIP,maxIP,F,T,c));
        assertEquals("all but first", numDocs-1, result.length());

        result = search.search(q,new RangeFilter("id",minIP,maxIP,F,F,c));
        assertEquals("all but ends", numDocs-2, result.length());

        result = search.search(q,new RangeFilter("id",medIP,maxIP,T,T,c));
        assertEquals("med and up", 1+ maxId-medId, result.length());

        result = search.search(q,new RangeFilter("id",minIP,medIP,T,T,c));
        assertEquals("up to med", 1+ medId-minId, result.length());

        // unbounded id

        result = search.search(q,new RangeFilter("id",minIP,null,T,F,c));
        assertEquals("min and up", numDocs, result.length());

        result = search.search(q,new RangeFilter("id",null,maxIP,F,T,c));
        assertEquals("max and down", numDocs, result.length());

        result = search.search(q,new RangeFilter("id",minIP,null,F,F,c));
        assertEquals("not min, but up", numDocs-1, result.length());

        result = search.search(q,new RangeFilter("id",null,maxIP,F,F,c));
        assertEquals("not max, but down", numDocs-1, result.length());

        result = search.search(q,new RangeFilter("id",medIP,maxIP,T,F,c));
        assertEquals("med and up, not max", maxId-medId, result.length());

        result = search.search(q,new RangeFilter("id",minIP,medIP,F,T,c));
        assertEquals("not min, up to med", medId-minId, result.length());

        // very small sets

        result = search.search(q,new RangeFilter("id",minIP,minIP,F,F,c));
        assertEquals("min,min,F,F", 0, result.length());
        result = search.search(q,new RangeFilter("id",medIP,medIP,F,F,c));
        assertEquals("med,med,F,F", 0, result.length());
        result = search.search(q,new RangeFilter("id",maxIP,maxIP,F,F,c));
        assertEquals("max,max,F,F", 0, result.length());

        result = search.search(q,new RangeFilter("id",minIP,minIP,T,T,c));
        assertEquals("min,min,T,T", 1, result.length());
        result = search.search(q,new RangeFilter("id",null,minIP,F,T,c));
        assertEquals("nul,min,F,T", 1, result.length());

        result = search.search(q,new RangeFilter("id",maxIP,maxIP,T,T,c));
        assertEquals("max,max,T,T", 1, result.length());
        result = search.search(q,new RangeFilter("id",maxIP,null,T,F,c));
        assertEquals("max,nul,T,T", 1, result.length());

        result = search.search(q,new RangeFilter("id",medIP,medIP,T,T,c));
        assertEquals("med,med,T,T", 1, result.length());
    }

    public void testRangeFilterRand() throws IOException {

  IndexReader reader = IndexReader.open(signedIndex.index);
	IndexSearcher search = new IndexSearcher(reader);

        String minRP = pad(signedIndex.minR);
        String maxRP = pad(signedIndex.maxR);
    
        int numDocs = reader.numDocs();
        
        assertEquals("num of docs", numDocs, 1+ maxId - minId);
        
  ScoreDoc[] result;
        Query q = new TermQuery(new Term("body","body"));

        // test extremes, bounded on both ends
        
  result = search.search(q,new RangeFilter("rand",minRP,maxRP,T,T), numDocs).scoreDocs;
  assertEquals("find all", numDocs, result.length);

  result = search.search(q,new RangeFilter("rand",minRP,maxRP,T,F), numDocs).scoreDocs;
  assertEquals("all but biggest", numDocs-1, result.length);

  result = search.search(q,new RangeFilter("rand",minRP,maxRP,F,T), numDocs).scoreDocs;
  assertEquals("all but smallest", numDocs-1, result.length);
        
  result = search.search(q,new RangeFilter("rand",minRP,maxRP,F,F), numDocs).scoreDocs;
        assertEquals("all but extremes", numDocs-2, result.length);
    
        // unbounded

  result = search.search(q,new RangeFilter("rand",minRP,null,T,F), numDocs).scoreDocs;
  assertEquals("smallest and up", numDocs, result.length);

  result = search.search(q,new RangeFilter("rand",null,maxRP,F,T), numDocs).scoreDocs;
  assertEquals("biggest and down", numDocs, result.length);

  result = search.search(q,new RangeFilter("rand",minRP,null,F,F), numDocs).scoreDocs;
  assertEquals("not smallest, but up", numDocs-1, result.length);
        
  result = search.search(q,new RangeFilter("rand",null,maxRP,F,F), numDocs).scoreDocs;
  assertEquals("not biggest, but down", numDocs-1, result.length);
        
        // very small sets

  result = search.search(q,new RangeFilter("rand",minRP,minRP,F,F), numDocs).scoreDocs;
  assertEquals("min,min,F,F", 0, result.length);
  result = search.search(q,new RangeFilter("rand",maxRP,maxRP,F,F), numDocs).scoreDocs;
  assertEquals("max,max,F,F", 0, result.length);
                     
  result = search.search(q,new RangeFilter("rand",minRP,minRP,T,T), numDocs).scoreDocs;
  assertEquals("min,min,T,T", 1, result.length);
  result = search.search(q,new RangeFilter("rand",null,minRP,F,T), numDocs).scoreDocs;
  assertEquals("nul,min,F,T", 1, result.length);

  result = search.search(q,new RangeFilter("rand",maxRP,maxRP,T,T), numDocs).scoreDocs;
  assertEquals("max,max,T,T", 1, result.length);
  result = search.search(q,new RangeFilter("rand",maxRP,null,T,F), numDocs).scoreDocs;
  assertEquals("max,nul,T,T", 1, result.length);
        
    }

    public void testRangeFilterRandCollating() throws IOException {

        // using the unsigned index because collation seems to ignore hyphens
        IndexReader reader = IndexReader.open(unsignedIndex.index);
        IndexSearcher search = new IndexSearcher(reader);

        Collator c = Collator.getInstance(Locale.ENGLISH);

        String minRP = pad(unsignedIndex.minR);
        String maxRP = pad(unsignedIndex.maxR);

        int numDocs = reader.numDocs();

        assertEquals("num of docs", numDocs, 1+ maxId - minId);

        Hits result;
        Query q = new TermQuery(new Term("body","body"));

        // test extremes, bounded on both ends

        result = search.search(q,new RangeFilter("rand",minRP,maxRP,T,T,c));
        assertEquals("find all", numDocs, result.length());

        result = search.search(q,new RangeFilter("rand",minRP,maxRP,T,F,c));
        assertEquals("all but biggest", numDocs-1, result.length());

        result = search.search(q,new RangeFilter("rand",minRP,maxRP,F,T,c));
        assertEquals("all but smallest", numDocs-1, result.length());

        result = search.search(q,new RangeFilter("rand",minRP,maxRP,F,F,c));
        assertEquals("all but extremes", numDocs-2, result.length());

        // unbounded

        result = search.search(q,new RangeFilter("rand",minRP,null,T,F,c));
        assertEquals("smallest and up", numDocs, result.length());

        result = search.search(q,new RangeFilter("rand",null,maxRP,F,T,c));
        assertEquals("biggest and down", numDocs, result.length());

        result = search.search(q,new RangeFilter("rand",minRP,null,F,F,c));
        assertEquals("not smallest, but up", numDocs-1, result.length());

        result = search.search(q,new RangeFilter("rand",null,maxRP,F,F,c));
        assertEquals("not biggest, but down", numDocs-1, result.length());

        // very small sets

        result = search.search(q,new RangeFilter("rand",minRP,minRP,F,F,c));
        assertEquals("min,min,F,F", 0, result.length());
        result = search.search(q,new RangeFilter("rand",maxRP,maxRP,F,F,c));
        assertEquals("max,max,F,F", 0, result.length());

        result = search.search(q,new RangeFilter("rand",minRP,minRP,T,T,c));
        assertEquals("min,min,T,T", 1, result.length());
        result = search.search(q,new RangeFilter("rand",null,minRP,F,T,c));
        assertEquals("nul,min,F,T", 1, result.length());

        result = search.search(q,new RangeFilter("rand",maxRP,maxRP,T,T,c));
        assertEquals("max,max,T,T", 1, result.length());
        result = search.search(q,new RangeFilter("rand",maxRP,null,T,F,c));
        assertEquals("max,nul,T,T", 1, result.length());
    }
    
    public void testFarsi() throws Exception {
            
        /* build an index */
        RAMDirectory farsiIndex = new RAMDirectory();
        IndexWriter writer = new IndexWriter(farsiIndex, new SimpleAnalyzer(), T, 
                                             IndexWriter.MaxFieldLength.LIMITED);
        Document doc = new Document();
        doc.add(new Field("content","\u0633\u0627\u0628", 
                          Field.Store.YES, Field.Index.UN_TOKENIZED));
        doc.add(new Field("body", "body",
                          Field.Store.YES, Field.Index.UN_TOKENIZED));
        writer.addDocument(doc);
            
        writer.optimize();
        writer.close();

        IndexReader reader = IndexReader.open(farsiIndex);
        IndexSearcher search = new IndexSearcher(reader);
        Query q = new TermQuery(new Term("body","body"));

        // Neither Java 1.4.2 nor 1.5.0 has Farsi Locale collation available in
        // RuleBasedCollator.  However, the Arabic Locale seems to order the Farsi
        // characters properly.
        Collator collator = Collator.getInstance(new Locale("ar"));
        
        // Unicode order would include U+0633 in [ U+062F - U+0698 ], but Farsi
        // orders the U+0698 character before the U+0633 character, so the single
        // index Term below should NOT be returned by a RangeFilter with a Farsi
        // Collator (or an Arabic one for the case when Farsi is not supported).
        Hits result = search.search
            (q, new RangeFilter("content", "\u062F", "\u0698", T, T, collator));
        assertEquals("The index Term should not be included.", 0, result.length());

        result = search.search
            (q, new RangeFilter("content", "\u0633", "\u0638", T, T, collator));
        assertEquals("The index Term should be included.", 1, result.length());
        search.close();
    }
}
