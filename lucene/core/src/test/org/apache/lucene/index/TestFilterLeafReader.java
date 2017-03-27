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
package org.apache.lucene.index;



import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.BaseDirectoryWrapper;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;

public class TestFilterLeafReader extends LuceneTestCase {

  private static class TestReader extends FilterLeafReader {

    /** Filter that only permits terms containing 'e'.*/
    private static class TestFields extends FilterFields {
      TestFields(Fields in) {
        super(in);
      }

      @Override
      public Terms terms(String field) throws IOException {
        return new TestTerms(super.terms(field));
      }
    }

    private static class TestTerms extends FilterTerms {
      TestTerms(Terms in) {
        super(in);
      }

      @Override
      public TermsEnum iterator() throws IOException {
        return new TestTermsEnum(super.iterator());
      }
    }

    private static class TestTermsEnum extends FilterTermsEnum {
      public TestTermsEnum(TermsEnum in) {
        super(in);
      }

      /** Scan for terms containing the letter 'e'.*/
      @Override
      public BytesRef next() throws IOException {
        BytesRef text;
        while ((text = in.next()) != null) {
          if (text.utf8ToString().indexOf('e') != -1)
            return text;
        }
        return null;
      }

      @Override
      public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
        return new TestPositions(super.postings(reuse == null ? null : ((FilterPostingsEnum) reuse).in, flags));
      }
    }

    /** Filter that only returns odd numbered documents. */
    private static class TestPositions extends FilterPostingsEnum {
      public TestPositions(PostingsEnum in) {
        super(in);
      }

      /** Scan for odd numbered documents. */
      @Override
      public int nextDoc() throws IOException {
        int doc;
        while ((doc = in.nextDoc()) != NO_MORE_DOCS) {
          if ((doc % 2) == 1)
            return doc;
        }
        return NO_MORE_DOCS;
      }
    }
    
    public TestReader(LeafReader reader) throws IOException {
      super(reader);
    }

    @Override
    public Fields fields() throws IOException {
      return new TestFields(super.fields());
    }

    @Override
    public CacheHelper getCoreCacheHelper() {
      return null;
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
      return null;
    }
  }
    
  /**
   * Tests the IndexReader.getFieldNames implementation
   * @throws Exception on error
   */
  public void testFilterIndexReader() throws Exception {
    Directory directory = newDirectory();

    IndexWriter writer = new IndexWriter(directory, newIndexWriterConfig(new MockAnalyzer(random())));

    Document d1 = new Document();
    d1.add(newTextField("default", "one two", Field.Store.YES));
    writer.addDocument(d1);

    Document d2 = new Document();
    d2.add(newTextField("default", "one three", Field.Store.YES));
    writer.addDocument(d2);

    Document d3 = new Document();
    d3.add(newTextField("default", "two four", Field.Store.YES));
    writer.addDocument(d3);
    writer.forceMerge(1);
    writer.close();

    Directory target = newDirectory();

    // We mess with the postings so this can fail:
    ((BaseDirectoryWrapper) target).setCrossCheckTermVectorsOnClose(false);

    writer = new IndexWriter(target, newIndexWriterConfig(new MockAnalyzer(random())));
    try (LeafReader reader = new TestReader(getOnlyLeafReader(DirectoryReader.open(directory)))) {
      writer.addIndexes(SlowCodecReaderWrapper.wrap(reader));
    }
    writer.close();
    IndexReader reader = DirectoryReader.open(target);
    
    TermsEnum terms = MultiFields.getTerms(reader, "default").iterator();
    while (terms.next() != null) {
      assertTrue(terms.term().utf8ToString().indexOf('e') != -1);
    }
    
    assertEquals(TermsEnum.SeekStatus.FOUND, terms.seekCeil(new BytesRef("one")));
    
    PostingsEnum positions = terms.postings(null, PostingsEnum.ALL);
    while (positions.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
      assertTrue((positions.docID() % 2) == 1);
    }

    reader.close();
    directory.close();
    target.close();
  }

  private static void checkOverrideMethods(Class<?> clazz) throws NoSuchMethodException, SecurityException {
    final Class<?> superClazz = clazz.getSuperclass();
    for (Method m : superClazz.getMethods()) {
      final int mods = m.getModifiers();
      if (Modifier.isStatic(mods) || Modifier.isAbstract(mods) || Modifier.isFinal(mods) || m.isSynthetic()
          || m.getName().equals("attributes") || m.getName().equals("getStats")) {
        continue;
      }
      // The point of these checks is to ensure that methods that have a default
      // impl through other methods are not overridden. This makes the number of
      // methods to override to have a working impl minimal and prevents from some
      // traps: for example, think about having getCoreCacheKey delegate to the
      // filtered impl by default
      final Method subM = clazz.getMethod(m.getName(), m.getParameterTypes());
      if (subM.getDeclaringClass() == clazz
          && m.getDeclaringClass() != Object.class
          && m.getDeclaringClass() != subM.getDeclaringClass()) {
        fail(clazz + " overrides " + m + " although it has a default impl");
      }
    }
  }

  public void testOverrideMethods() throws Exception {
    checkOverrideMethods(FilterLeafReader.class);
    checkOverrideMethods(FilterLeafReader.FilterFields.class);
    checkOverrideMethods(FilterLeafReader.FilterTerms.class);
    checkOverrideMethods(FilterLeafReader.FilterTermsEnum.class);
    checkOverrideMethods(FilterLeafReader.FilterPostingsEnum.class);
  }

  public void testUnwrap() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    w.addDocument(new Document());
    DirectoryReader dr = w.getReader();
    LeafReader r = dr.leaves().get(0).reader();
    FilterLeafReader r2 = new FilterLeafReader(r) {
      @Override
      public CacheHelper getCoreCacheHelper() {
        return in.getCoreCacheHelper();
      }
      @Override
      public CacheHelper getReaderCacheHelper() {
        return in.getReaderCacheHelper();
      }
    };
    assertEquals(r, r2.getDelegate());
    assertEquals(r, FilterLeafReader.unwrap(r2));
    w.close();
    dr.close();
    dir.close();
  }
}
