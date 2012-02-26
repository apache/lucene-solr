package org.apache.lucene.index;

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
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashSet;

import junit.framework.TestSuite;
import junit.textui.TestRunner;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.ReaderUtil;

public class TestFilterIndexReader extends LuceneTestCase {

  private static class TestReader extends FilterIndexReader {

     /** Filter that only permits terms containing 'e'.*/
    private static class TestTermEnum extends FilterTermEnum {
      public TestTermEnum(TermEnum termEnum) {
        super(termEnum);
      }

      /** Scan for terms containing the letter 'e'.*/
      @Override
      public boolean next() throws IOException {
        while (in.next()) {
          if (in.term().text().indexOf('e') != -1)
            return true;
        }
        return false;
      }
    }
    
    /** Filter that only returns odd numbered documents. */
    private static class TestTermPositions extends FilterTermPositions {
      public TestTermPositions(TermPositions in) {
        super(in);
      }

      /** Scan for odd numbered documents. */
      @Override
      public boolean next() throws IOException {
        while (in.next()) {
          if ((in.doc() % 2) == 1)
            return true;
        }
        return false;
      }
    }
    
    public TestReader(IndexReader reader) {
      super(reader);
    }

    /** Filter terms with TestTermEnum. */
    @Override
    public TermEnum terms() throws IOException {
      return new TestTermEnum(in.terms());
    }

    /** Filter positions with TestTermPositions. */
    @Override
    public TermPositions termPositions() throws IOException {
      return new TestTermPositions(in.termPositions());
    }

    @Override
    public FieldInfos getFieldInfos() {
      return ReaderUtil.getMergedFieldInfos(in);
    }
  }


  /** Main for running test case by itself. */
  public static void main(String args[]) {
    TestRunner.run (new TestSuite(TestIndexReader.class));
  }
    
  /**
   * Tests the IndexReader.getFieldNames implementation
   * @throws Exception on error
   */
  public void testFilterIndexReader() throws Exception {
    Directory directory = newDirectory();
    IndexWriter writer = new IndexWriter(directory, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)));

    Document d1 = new Document();
    d1.add(newField("default","one two", Field.Store.YES, Field.Index.ANALYZED));
    writer.addDocument(d1);

    Document d2 = new Document();
    d2.add(newField("default","one three", Field.Store.YES, Field.Index.ANALYZED));
    writer.addDocument(d2);

    Document d3 = new Document();
    d3.add(newField("default","two four", Field.Store.YES, Field.Index.ANALYZED));
    writer.addDocument(d3);

    writer.close();

    IndexReader reader = new TestReader(IndexReader.open(directory, true));
    TermEnum terms = reader.terms();
    while (terms.next()) {
      assertTrue(terms.term().text().indexOf('e') != -1);
    }
    terms.close();
    
    TermPositions positions = reader.termPositions(new Term("default", "one"));
    while (positions.next()) {
      assertTrue((positions.doc() % 2) == 1);
    }

    int NUM_DOCS = 3;

    TermDocs td = reader.termDocs(null);
    for(int i=0;i<NUM_DOCS;i++) {
      assertTrue(td.next());
      assertEquals(i, td.doc());
      assertEquals(1, td.freq());
    }
    td.close();
    reader.close();
    directory.close();
  }

  private void checkOverrideMethods(Class<?> clazz) throws Exception {
    boolean fail = false;
    for (Method m : clazz.getMethods()) {
      int mods = m.getModifiers();
      if (Modifier.isStatic(mods) || Modifier.isFinal(mods) || m.isSynthetic()) {
        continue;
      }
      Class<?> declaringClass = m.getDeclaringClass();
      if (declaringClass != clazz && declaringClass != Object.class) {
        System.err.println("method is not overridden by "+clazz.getName()+": " + m.toGenericString());
        fail = true;
      }
    }
    assertFalse(clazz.getName()+" does not override some methods; see log above", fail);
  }

  public void testOverrideMethods() throws Exception {
    HashSet<String> methodsThatShouldNotBeOverridden = new HashSet<String>();
    methodsThatShouldNotBeOverridden.add("reopen");
    methodsThatShouldNotBeOverridden.add("doOpenIfChanged");
    methodsThatShouldNotBeOverridden.add("clone");
    boolean fail = false;
    for (Method m : FilterIndexReader.class.getMethods()) {
      int mods = m.getModifiers();
      if (Modifier.isStatic(mods) || Modifier.isFinal(mods) || m.isSynthetic()) {
        continue;
      }
      Class<?> declaringClass = m.getDeclaringClass();
      String name = m.getName();
      if (declaringClass != FilterIndexReader.class && declaringClass != Object.class && !methodsThatShouldNotBeOverridden.contains(name)) {
        System.err.println("method is not overridden by FilterIndexReader: " + name);
        fail = true;
      } else if (declaringClass == FilterIndexReader.class && methodsThatShouldNotBeOverridden.contains(name)) {
        System.err.println("method should not be overridden by FilterIndexReader: " + name);
        fail = true;
      }
    }
    assertFalse("FilterIndexReader overrides (or not) some problematic methods; see log above", fail);
    
    // some more inner classes:
    checkOverrideMethods(FilterIndexReader.FilterTermEnum.class);
    checkOverrideMethods(FilterIndexReader.FilterTermDocs.class);
    // TODO: FilterTermPositions should extend correctly, this is borken,
    // but for backwards compatibility we let it be:
    // checkOverrideMethods(FilterIndexReader.FilterTermPositions.class);
  }

}
