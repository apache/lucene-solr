package org.apache.lucene.queries;

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

import java.util.HashSet;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.SlowCompositeReaderWrapper;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Filter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LuceneTestCase;

public class TermsFilterTest extends LuceneTestCase {

  public void testCachability() throws Exception {
    TermsFilter a = new TermsFilter();
    a.addTerm(new Term("field1", "a"));
    a.addTerm(new Term("field1", "b"));
    HashSet<Filter> cachedFilters = new HashSet<Filter>();
    cachedFilters.add(a);
    TermsFilter b = new TermsFilter();
    b.addTerm(new Term("field1", "a"));
    b.addTerm(new Term("field1", "b"));

    assertTrue("Must be cached", cachedFilters.contains(b));
    b.addTerm(new Term("field1", "a")); //duplicate term
    assertTrue("Must be cached", cachedFilters.contains(b));
    b.addTerm(new Term("field1", "c"));
    assertFalse("Must not be cached", cachedFilters.contains(b));
  }

  public void testMissingTerms() throws Exception {
    String fieldName = "field1";
    Directory rd = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), rd);
    for (int i = 0; i < 100; i++) {
      Document doc = new Document();
      int term = i * 10; //terms are units of 10;
      doc.add(newStringField(fieldName, "" + term, Field.Store.YES));
      w.addDocument(doc);
    }
    IndexReader reader = new SlowCompositeReaderWrapper(w.getReader());
    assertTrue(reader.getContext() instanceof AtomicReaderContext);
    AtomicReaderContext context = (AtomicReaderContext) reader.getContext();
    w.close();

    TermsFilter tf = new TermsFilter();
    tf.addTerm(new Term(fieldName, "19"));
    FixedBitSet bits = (FixedBitSet) tf.getDocIdSet(context, context.reader().getLiveDocs());
    assertEquals("Must match nothing", 0, bits.cardinality());

    tf.addTerm(new Term(fieldName, "20"));
    bits = (FixedBitSet) tf.getDocIdSet(context, context.reader().getLiveDocs());
    assertEquals("Must match 1", 1, bits.cardinality());

    tf.addTerm(new Term(fieldName, "10"));
    bits = (FixedBitSet) tf.getDocIdSet(context, context.reader().getLiveDocs());
    assertEquals("Must match 2", 2, bits.cardinality());

    tf.addTerm(new Term(fieldName, "00"));
    bits = (FixedBitSet) tf.getDocIdSet(context, context.reader().getLiveDocs());
    assertEquals("Must match 2", 2, bits.cardinality());

    reader.close();
    rd.close();
  }
  
  public void testMissingField() throws Exception {
    String fieldName = "field1";
    Directory rd1 = newDirectory();
    RandomIndexWriter w1 = new RandomIndexWriter(random(), rd1);
    Document doc = new Document();
    doc.add(newStringField(fieldName, "content1", Field.Store.YES));
    w1.addDocument(doc);
    IndexReader reader1 = w1.getReader();
    w1.close();
    
    fieldName = "field2";
    Directory rd2 = newDirectory();
    RandomIndexWriter w2 = new RandomIndexWriter(random(), rd2);
    doc = new Document();
    doc.add(newStringField(fieldName, "content2", Field.Store.YES));
    w2.addDocument(doc);
    IndexReader reader2 = w2.getReader();
    w2.close();
    
    TermsFilter tf = new TermsFilter();
    tf.addTerm(new Term(fieldName, "content1"));
    
    MultiReader multi = new MultiReader(reader1, reader2);
    for (AtomicReaderContext context : multi.leaves()) {
      FixedBitSet bits = (FixedBitSet) tf.getDocIdSet(context, context.reader().getLiveDocs());
      assertTrue("Must be >= 0", bits.cardinality() >= 0);      
    }
    multi.close();
    reader1.close();
    reader2.close();
    rd1.close();
    rd2.close();
  }

}
