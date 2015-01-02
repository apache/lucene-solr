package org.apache.lucene.search;

/*
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

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldTypes;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.junit.Test;

/**
 * A basic 'positive' Unit test class for the DocValues range filters.
 *
 * <p>
 * NOTE: at the moment, this class only tests for 'positive' results,
 * it does not verify the results to ensure there are no 'false positives',
 * nor does it adequately test 'negative' results.  It also does not test
 * that garbage in results in an Exception.
 */
public class TestDocValuesRangeFilter extends BaseTestRangeFilter {

  @Test
  public void testRangeFilterId() throws IOException {

    IndexReader reader = signedIndexReader;
    IndexSearcher search = newSearcher(reader);

    int medId = ((maxId - minId) / 2);
        
    String minIP = pad(minId);
    String maxIP = pad(maxId);
    String medIP = pad(medId);
    
    int numDocs = reader.numDocs();
        
    assertEquals("num of docs", numDocs, 1+ maxId - minId);
        
    ScoreDoc[] result;
    Query q = new TermQuery(new Term("body","body"));
    FieldTypes fieldTypes = search.getFieldTypes();

    // test id, bounded on both ends
    result = search.search(q, fieldTypes.newStringDocValuesRangeFilter("id",minIP,T,maxIP,T), numDocs).scoreDocs;
    assertEquals("find all", numDocs, result.length);

    result = search.search(q, fieldTypes.newStringDocValuesRangeFilter("id",minIP,T,maxIP,F), numDocs).scoreDocs;
    assertEquals("all but last", numDocs-1, result.length);

    result = search.search(q, fieldTypes.newStringDocValuesRangeFilter("id",minIP,F,maxIP,T), numDocs).scoreDocs;
    assertEquals("all but first", numDocs-1, result.length);
        
    result = search.search(q, fieldTypes.newStringDocValuesRangeFilter("id",minIP,F,maxIP,F), numDocs).scoreDocs;
    assertEquals("all but ends", numDocs-2, result.length);
    
    result = search.search(q, fieldTypes.newStringDocValuesRangeFilter("id",medIP,T,maxIP,T), numDocs).scoreDocs;
    assertEquals("med and up", 1+ maxId-medId, result.length);
        
    result = search.search(q, fieldTypes.newStringDocValuesRangeFilter("id",minIP,T,medIP,T), numDocs).scoreDocs;
    assertEquals("up to med", 1+ medId-minId, result.length);

    // unbounded id

    result = search.search(q, fieldTypes.newStringDocValuesRangeFilter("id",(String)null,T,null,T), numDocs).scoreDocs;
    assertEquals("find all", numDocs, result.length);

    result = search.search(q, fieldTypes.newStringDocValuesRangeFilter("id",minIP,T,null,F), numDocs).scoreDocs;
    assertEquals("min and up", numDocs, result.length);

    result = search.search(q, fieldTypes.newStringDocValuesRangeFilter("id",null,F,maxIP,T), numDocs).scoreDocs;
    assertEquals("max and down", numDocs, result.length);

    result = search.search(q, fieldTypes.newStringDocValuesRangeFilter("id",minIP,F,null,F), numDocs).scoreDocs;
    assertEquals("not min, but up", numDocs-1, result.length);
        
    result = search.search(q, fieldTypes.newStringDocValuesRangeFilter("id",null,F,maxIP,F), numDocs).scoreDocs;
    assertEquals("not max, but down", numDocs-1, result.length);
        
    result = search.search(q, fieldTypes.newStringDocValuesRangeFilter("id",medIP,T,maxIP,F), numDocs).scoreDocs;
    assertEquals("med and up, not max", maxId-medId, result.length);
        
    result = search.search(q, fieldTypes.newStringDocValuesRangeFilter("id",minIP,F,medIP,T), numDocs).scoreDocs;
    assertEquals("not min, up to med", medId-minId, result.length);

    // very small sets

    result = search.search(q, fieldTypes.newStringDocValuesRangeFilter("id",minIP,F,minIP,F), numDocs).scoreDocs;
    assertEquals("min,min,F,F", 0, result.length);
    result = search.search(q, fieldTypes.newStringDocValuesRangeFilter("id",medIP,F,medIP,F), numDocs).scoreDocs;
    assertEquals("med,med,F,F", 0, result.length);
    result = search.search(q, fieldTypes.newStringDocValuesRangeFilter("id",maxIP,F,maxIP,F), numDocs).scoreDocs;
    assertEquals("max,max,F,F", 0, result.length);
                     
    result = search.search(q, fieldTypes.newStringDocValuesRangeFilter("id",minIP,T,minIP,T), numDocs).scoreDocs;
    assertEquals("min,min,T,T", 1, result.length);
    result = search.search(q, fieldTypes.newStringDocValuesRangeFilter("id",null,F,minIP,T), numDocs).scoreDocs;
    assertEquals("nul,min,F,T", 1, result.length);

    result = search.search(q, fieldTypes.newStringDocValuesRangeFilter("id",maxIP,T,maxIP,T), numDocs).scoreDocs;
    assertEquals("max,max,T,T", 1, result.length);
    result = search.search(q, fieldTypes.newStringDocValuesRangeFilter("id",maxIP,T,null,F), numDocs).scoreDocs;
    assertEquals("max,nul,T,T", 1, result.length);

    result = search.search(q, fieldTypes.newStringDocValuesRangeFilter("id",medIP,T,medIP,T), numDocs).scoreDocs;
    assertEquals("med,med,T,T", 1, result.length);
  }

  @Test
  public void testFieldCacheRangeFilterRand() throws IOException {

    IndexReader reader = signedIndexReader;
    IndexSearcher search = newSearcher(reader);

    String minRP = pad(signedIndexDir.minR);
    String maxRP = pad(signedIndexDir.maxR);
    
    int numDocs = reader.numDocs();
        
    assertEquals("num of docs", numDocs, 1+ maxId - minId);
        
    ScoreDoc[] result;
    Query q = new TermQuery(new Term("body","body"));

    // test extremes, bounded on both ends
        
    FieldTypes fieldTypes = search.getFieldTypes();
    result = search.search(q, fieldTypes.newStringDocValuesRangeFilter("rand",minRP,T,maxRP,T), numDocs).scoreDocs;
    assertEquals("find all", numDocs, result.length);

    result = search.search(q, fieldTypes.newStringDocValuesRangeFilter("rand",minRP,T,maxRP,F), numDocs).scoreDocs;
    assertEquals("all but biggest", numDocs-1, result.length);

    result = search.search(q, fieldTypes.newStringDocValuesRangeFilter("rand",minRP,F,maxRP,T), numDocs).scoreDocs;
    assertEquals("all but smallest", numDocs-1, result.length);
        
    result = search.search(q, fieldTypes.newStringDocValuesRangeFilter("rand",minRP,F,maxRP,F), numDocs).scoreDocs;
    assertEquals("all but extremes", numDocs-2, result.length);
    
    // unbounded

    result = search.search(q, fieldTypes.newStringDocValuesRangeFilter("rand",minRP,T,null,F), numDocs).scoreDocs;
    assertEquals("smallest and up", numDocs, result.length);

    result = search.search(q, fieldTypes.newStringDocValuesRangeFilter("rand",null,F,maxRP,T), numDocs).scoreDocs;
    assertEquals("biggest and down", numDocs, result.length);

    result = search.search(q, fieldTypes.newStringDocValuesRangeFilter("rand",minRP,F,null,F), numDocs).scoreDocs;
    assertEquals("not smallest, but up", numDocs-1, result.length);
        
    result = search.search(q, fieldTypes.newStringDocValuesRangeFilter("rand",null,F,maxRP,F), numDocs).scoreDocs;
    assertEquals("not biggest, but down", numDocs-1, result.length);
        
    // very small sets

    result = search.search(q, fieldTypes.newStringDocValuesRangeFilter("rand",minRP,F,minRP,F), numDocs).scoreDocs;
    assertEquals("min,min,F,F", 0, result.length);
    result = search.search(q, fieldTypes.newStringDocValuesRangeFilter("rand",maxRP,F,maxRP,F), numDocs).scoreDocs;
    assertEquals("max,max,F,F", 0, result.length);
                     
    result = search.search(q, fieldTypes.newStringDocValuesRangeFilter("rand",minRP,T,minRP,T), numDocs).scoreDocs;
    assertEquals("min,min,T,T", 1, result.length);
    result = search.search(q, fieldTypes.newStringDocValuesRangeFilter("rand",null,F,minRP,T), numDocs).scoreDocs;
    assertEquals("nul,min,F,T", 1, result.length);

    result = search.search(q, fieldTypes.newStringDocValuesRangeFilter("rand",maxRP,T,maxRP,T), numDocs).scoreDocs;
    assertEquals("max,max,T,T", 1, result.length);
    result = search.search(q, fieldTypes.newStringDocValuesRangeFilter("rand",maxRP,T,null,F), numDocs).scoreDocs;
    assertEquals("max,nul,T,T", 1, result.length);
  }
  
  @Test
  public void testFieldCacheRangeFilterInts() throws IOException {

    IndexReader reader = signedIndexReader;
    IndexSearcher search = newSearcher(reader);

    int numDocs = reader.numDocs();
    int medId = ((maxId - minId) / 2);
    Integer minIdO = Integer.valueOf(minId);
    Integer maxIdO = Integer.valueOf(maxId);
    Integer medIdO = Integer.valueOf(medId);
        
    assertEquals("num of docs", numDocs, 1+ maxId - minId);
        
    ScoreDoc[] result;
    Query q = new TermQuery(new Term("body","body"));

    // test id, bounded on both ends
        
    FieldTypes fieldTypes = search.getFieldTypes();
    result = search.search(q,fieldTypes.newIntDocValuesRangeFilter("id_int",minIdO,T,maxIdO,T), numDocs).scoreDocs;
    assertEquals("find all", numDocs, result.length);

    result = search.search(q,fieldTypes.newIntDocValuesRangeFilter("id_int",minIdO,T,maxIdO,F), numDocs).scoreDocs;
    assertEquals("all but last", numDocs-1, result.length);

    result = search.search(q,fieldTypes.newIntDocValuesRangeFilter("id_int",minIdO,F,maxIdO,T), numDocs).scoreDocs;
    assertEquals("all but first", numDocs-1, result.length);
        
    result = search.search(q,fieldTypes.newIntDocValuesRangeFilter("id_int",minIdO,F,maxIdO,F), numDocs).scoreDocs;
    assertEquals("all but ends", numDocs-2, result.length);
    
    result = search.search(q,fieldTypes.newIntDocValuesRangeFilter("id_int",medIdO,T,maxIdO,T), numDocs).scoreDocs;
    assertEquals("med and up", 1+ maxId-medId, result.length);
        
    result = search.search(q,fieldTypes.newIntDocValuesRangeFilter("id_int",minIdO,T,medIdO,T), numDocs).scoreDocs;
    assertEquals("up to med", 1+ medId-minId, result.length);
    
    // unbounded id

    result = search.search(q,fieldTypes.newIntDocValuesRangeFilter("id_int",(Integer) null,T,null,T), numDocs).scoreDocs;
    assertEquals("find all", numDocs, result.length);

    result = search.search(q,fieldTypes.newIntDocValuesRangeFilter("id_int",minIdO,T,null,F), numDocs).scoreDocs;
    assertEquals("min and up", numDocs, result.length);

    result = search.search(q,fieldTypes.newIntDocValuesRangeFilter("id_int",null,F,maxIdO,T), numDocs).scoreDocs;
    assertEquals("max and down", numDocs, result.length);

    result = search.search(q,fieldTypes.newIntDocValuesRangeFilter("id_int",minIdO,F,null,F), numDocs).scoreDocs;
    assertEquals("not min, but up", numDocs-1, result.length);
        
    result = search.search(q,fieldTypes.newIntDocValuesRangeFilter("id_int",null,F,maxIdO,F), numDocs).scoreDocs;
    assertEquals("not max, but down", numDocs-1, result.length);
        
    result = search.search(q,fieldTypes.newIntDocValuesRangeFilter("id_int",medIdO,T,maxIdO,F), numDocs).scoreDocs;
    assertEquals("med and up, not max", maxId-medId, result.length);
        
    result = search.search(q,fieldTypes.newIntDocValuesRangeFilter("id_int",minIdO,F,medIdO,T), numDocs).scoreDocs;
    assertEquals("not min, up to med", medId-minId, result.length);

    // very small sets

    result = search.search(q,fieldTypes.newIntDocValuesRangeFilter("id_int",minIdO,F,minIdO,F), numDocs).scoreDocs;
    assertEquals("min,min,F,F", 0, result.length);
    result = search.search(q,fieldTypes.newIntDocValuesRangeFilter("id_int",medIdO,F,medIdO,F), numDocs).scoreDocs;
    assertEquals("med,med,F,F", 0, result.length);
    result = search.search(q,fieldTypes.newIntDocValuesRangeFilter("id_int",maxIdO,F,maxIdO,F), numDocs).scoreDocs;
    assertEquals("max,max,F,F", 0, result.length);
                     
    result = search.search(q,fieldTypes.newIntDocValuesRangeFilter("id_int",minIdO,T,minIdO,T), numDocs).scoreDocs;
    assertEquals("min,min,T,T", 1, result.length);
    result = search.search(q,fieldTypes.newIntDocValuesRangeFilter("id_int",null,F,minIdO,T), numDocs).scoreDocs;
    assertEquals("nul,min,F,T", 1, result.length);

    result = search.search(q,fieldTypes.newIntDocValuesRangeFilter("id_int",maxIdO,T,maxIdO,T), numDocs).scoreDocs;
    assertEquals("max,max,T,T", 1, result.length);
    result = search.search(q,fieldTypes.newIntDocValuesRangeFilter("id_int",maxIdO,T,null,F), numDocs).scoreDocs;
    assertEquals("max,nul,T,T", 1, result.length);

    result = search.search(q,fieldTypes.newIntDocValuesRangeFilter("id_int",medIdO,T,medIdO,T), numDocs).scoreDocs;
    assertEquals("med,med,T,T", 1, result.length);
    
    // special cases
    result = search.search(q,fieldTypes.newIntDocValuesRangeFilter("id_int",Integer.valueOf(Integer.MAX_VALUE),F,null,F), numDocs).scoreDocs;
    assertEquals("overflow special case", 0, result.length);
    result = search.search(q,fieldTypes.newIntDocValuesRangeFilter("id_int",null,F,Integer.valueOf(Integer.MIN_VALUE),F), numDocs).scoreDocs;
    assertEquals("overflow special case", 0, result.length);
    result = search.search(q,fieldTypes.newIntDocValuesRangeFilter("id_int",maxIdO,T,minIdO,T), numDocs).scoreDocs;
    assertEquals("inverse range", 0, result.length);
  }
  
  @Test
  public void testFieldCacheRangeFilterLongs() throws IOException {

    IndexReader reader = signedIndexReader;
    IndexSearcher search = newSearcher(reader);

    int numDocs = reader.numDocs();
    int medId = ((maxId - minId) / 2);
    Long minIdO = Long.valueOf(minId);
    Long maxIdO = Long.valueOf(maxId);
    Long medIdO = Long.valueOf(medId);
        
    assertEquals("num of docs", numDocs, 1+ maxId - minId);
        
    ScoreDoc[] result;
    Query q = new TermQuery(new Term("body","body"));

    // test id, bounded on both ends
        
    FieldTypes fieldTypes = search.getFieldTypes();
    result = search.search(q,fieldTypes.newLongDocValuesRangeFilter("id_long",minIdO,T,maxIdO,T), numDocs).scoreDocs;
    assertEquals("find all", numDocs, result.length);

    result = search.search(q,fieldTypes.newLongDocValuesRangeFilter("id_long",minIdO,T,maxIdO,F), numDocs).scoreDocs;
    assertEquals("all but last", numDocs-1, result.length);

    result = search.search(q,fieldTypes.newLongDocValuesRangeFilter("id_long",minIdO,F,maxIdO,T), numDocs).scoreDocs;
    assertEquals("all but first", numDocs-1, result.length);
        
    result = search.search(q,fieldTypes.newLongDocValuesRangeFilter("id_long",minIdO,F,maxIdO,F), numDocs).scoreDocs;
    assertEquals("all but ends", numDocs-2, result.length);
    
    result = search.search(q,fieldTypes.newLongDocValuesRangeFilter("id_long",medIdO,T,maxIdO,T), numDocs).scoreDocs;
    assertEquals("med and up", 1+ maxId-medId, result.length);
        
    result = search.search(q,fieldTypes.newLongDocValuesRangeFilter("id_long",minIdO,T,medIdO,T), numDocs).scoreDocs;
    assertEquals("up to med", 1+ medId-minId, result.length);
    
    // unbounded id

    result = search.search(q,fieldTypes.newLongDocValuesRangeFilter("id_long",(Long) null,T,null,T), numDocs).scoreDocs;
    assertEquals("find all", numDocs, result.length);

    result = search.search(q,fieldTypes.newLongDocValuesRangeFilter("id_long",minIdO,T,null,F), numDocs).scoreDocs;
    assertEquals("min and up", numDocs, result.length);

    result = search.search(q,fieldTypes.newLongDocValuesRangeFilter("id_long",null,F,maxIdO,T), numDocs).scoreDocs;
    assertEquals("max and down", numDocs, result.length);

    result = search.search(q,fieldTypes.newLongDocValuesRangeFilter("id_long",minIdO,F,null,F), numDocs).scoreDocs;
    assertEquals("not min, but up", numDocs-1, result.length);
        
    result = search.search(q,fieldTypes.newLongDocValuesRangeFilter("id_long",null,F,maxIdO,F), numDocs).scoreDocs;
    assertEquals("not max, but down", numDocs-1, result.length);
        
    result = search.search(q,fieldTypes.newLongDocValuesRangeFilter("id_long",medIdO,T,maxIdO,F), numDocs).scoreDocs;
    assertEquals("med and up, not max", maxId-medId, result.length);
        
    result = search.search(q,fieldTypes.newLongDocValuesRangeFilter("id_long",minIdO,F,medIdO,T), numDocs).scoreDocs;
    assertEquals("not min, up to med", medId-minId, result.length);

    // very small sets

    result = search.search(q,fieldTypes.newLongDocValuesRangeFilter("id_long",minIdO,F,minIdO,F), numDocs).scoreDocs;
    assertEquals("min,min,F,F", 0, result.length);
    result = search.search(q,fieldTypes.newLongDocValuesRangeFilter("id_long",medIdO,F,medIdO,F), numDocs).scoreDocs;
    assertEquals("med,med,F,F", 0, result.length);
    result = search.search(q,fieldTypes.newLongDocValuesRangeFilter("id_long",maxIdO,F,maxIdO,F), numDocs).scoreDocs;
    assertEquals("max,max,F,F", 0, result.length);
                     
    result = search.search(q,fieldTypes.newLongDocValuesRangeFilter("id_long",minIdO,T,minIdO,T), numDocs).scoreDocs;
    assertEquals("min,min,T,T", 1, result.length);
    result = search.search(q,fieldTypes.newLongDocValuesRangeFilter("id_long",null,F,minIdO,T), numDocs).scoreDocs;
    assertEquals("nul,min,F,T", 1, result.length);

    result = search.search(q,fieldTypes.newLongDocValuesRangeFilter("id_long",maxIdO,T,maxIdO,T), numDocs).scoreDocs;
    assertEquals("max,max,T,T", 1, result.length);
    result = search.search(q,fieldTypes.newLongDocValuesRangeFilter("id_long",maxIdO,T,null,F), numDocs).scoreDocs;
    assertEquals("max,nul,T,T", 1, result.length);

    result = search.search(q,fieldTypes.newLongDocValuesRangeFilter("id_long",medIdO,T,medIdO,T), numDocs).scoreDocs;
    assertEquals("med,med,T,T", 1, result.length);
    
    // special cases
    result = search.search(q,fieldTypes.newLongDocValuesRangeFilter("id_long",Long.valueOf(Long.MAX_VALUE),F,null,F), numDocs).scoreDocs;
    assertEquals("overflow special case", 0, result.length);
    result = search.search(q,fieldTypes.newLongDocValuesRangeFilter("id_long",null,F,Long.valueOf(Long.MIN_VALUE),F), numDocs).scoreDocs;
    assertEquals("overflow special case", 0, result.length);
    result = search.search(q,fieldTypes.newLongDocValuesRangeFilter("id_long",maxIdO,T,minIdO,T), numDocs).scoreDocs;
    assertEquals("inverse range", 0, result.length);
  }
  
  // float and double tests are a bit minimalistic, but its complicated, because missing precision
  
  @Test
  public void testFieldCacheRangeFilterFloats() throws IOException {

    IndexReader reader = signedIndexReader;
    IndexSearcher search = newSearcher(reader);

    int numDocs = reader.numDocs();
    Float minIdO = Float.valueOf(minId + .5f);
    Float medIdO = Float.valueOf(minIdO.floatValue() + ((maxId-minId))/2.0f);
        
    ScoreDoc[] result;
    Query q = new TermQuery(new Term("body","body"));

    FieldTypes fieldTypes = search.getFieldTypes();
    result = search.search(q,fieldTypes.newFloatDocValuesRangeFilter("id_float",minIdO,T,medIdO,T), numDocs).scoreDocs;
    assertEquals("find all", numDocs/2, result.length);
    int count = 0;
    result = search.search(q,fieldTypes.newFloatDocValuesRangeFilter("id_float",null,F,medIdO,T), numDocs).scoreDocs;
    count += result.length;
    result = search.search(q,fieldTypes.newFloatDocValuesRangeFilter("id_float",medIdO,F,null,F), numDocs).scoreDocs;
    count += result.length;
    assertEquals("sum of two concenatted ranges", numDocs, count);
    result = search.search(q,fieldTypes.newFloatDocValuesRangeFilter("id_float",(Float) null,T,null,T), numDocs).scoreDocs;
    assertEquals("find all", numDocs, result.length);
    result = search.search(q,fieldTypes.newFloatDocValuesRangeFilter("id_float",Float.valueOf(Float.POSITIVE_INFINITY),F,null,F), numDocs).scoreDocs;
    assertEquals("infinity special case", 0, result.length);
    result = search.search(q,fieldTypes.newFloatDocValuesRangeFilter("id_float",null,F,Float.valueOf(Float.NEGATIVE_INFINITY),F), numDocs).scoreDocs;
    assertEquals("infinity special case", 0, result.length);
  }
  
  @Test
  public void testFieldCacheRangeFilterDoubles() throws IOException {

    IndexReader reader = signedIndexReader;
    IndexSearcher search = newSearcher(reader);

    int numDocs = reader.numDocs();
    Double minIdO = Double.valueOf(minId + .5);
    Double medIdO = Double.valueOf(minIdO.floatValue() + ((maxId-minId))/2.0);
        
    ScoreDoc[] result;
    Query q = new TermQuery(new Term("body","body"));

    FieldTypes fieldTypes = search.getFieldTypes();
    result = search.search(q,fieldTypes.newDoubleDocValuesRangeFilter("id_double",minIdO,T,medIdO,T), numDocs).scoreDocs;
    assertEquals("find all", numDocs/2, result.length);
    int count = 0;
    result = search.search(q,fieldTypes.newDoubleDocValuesRangeFilter("id_double",null,F,medIdO,T), numDocs).scoreDocs;
    count += result.length;
    result = search.search(q,fieldTypes.newDoubleDocValuesRangeFilter("id_double",medIdO,F,null,F), numDocs).scoreDocs;
    count += result.length;
    assertEquals("sum of two concenatted ranges", numDocs, count);
    result = search.search(q,fieldTypes.newDoubleDocValuesRangeFilter("id_double",(Double) null,T,null,T), numDocs).scoreDocs;
    assertEquals("find all", numDocs, result.length);
    result = search.search(q,fieldTypes.newDoubleDocValuesRangeFilter("id_double",Double.valueOf(Double.POSITIVE_INFINITY),F,null,F), numDocs).scoreDocs;
    assertEquals("infinity special case", 0, result.length);
    result = search.search(q,fieldTypes.newDoubleDocValuesRangeFilter("id_double",null,F,Double.valueOf(Double.NEGATIVE_INFINITY),F), numDocs).scoreDocs;
    assertEquals("infinity special case", 0, result.length);
  }
  
  // test using a sparse index (with deleted docs).
  @Test
  public void testSparseIndex() throws IOException {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
    FieldTypes fieldTypes = writer.getFieldTypes();

    for (int d = -20; d <= 20; d++) {
      Document doc = writer.newDocument();
      doc.addInt("id_int", d);
      doc.addAtom("body", "body");
      writer.addDocument(doc);
    }
    
    writer.forceMerge(1);
    writer.deleteDocuments(fieldTypes.newIntTerm("id_int", 0));
    writer.close();

    IndexReader reader = DirectoryReader.open(dir);
    IndexSearcher search = newSearcher(reader);
    assertTrue(reader.hasDeletions());

    ScoreDoc[] result;
    Query q = new TermQuery(new Term("body","body"));

    result = search.search(q,fieldTypes.newIntDocValuesRangeFilter("id_int",-20,T,20,T), 100).scoreDocs;
    assertEquals("find all", 40, result.length);

    result = search.search(q,fieldTypes.newIntDocValuesRangeFilter("id_int",0,T,20,T), 100).scoreDocs;
    assertEquals("find all", 20, result.length);

    result = search.search(q,fieldTypes.newIntDocValuesRangeFilter("id_int",-20,T,0,T), 100).scoreDocs;
    assertEquals("find all", 20, result.length);

    result = search.search(q,fieldTypes.newIntDocValuesRangeFilter("id_int",10,T,20,T), 100).scoreDocs;
    assertEquals("find all", 11, result.length);

    result = search.search(q,fieldTypes.newIntDocValuesRangeFilter("id_int",-20,T,-10,T), 100).scoreDocs;
    assertEquals("find all", 11, result.length);
    reader.close();
    dir.close();
  }
  
}
