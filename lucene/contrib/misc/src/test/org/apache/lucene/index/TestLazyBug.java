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

import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.document.FieldSelectorResult;
import org.apache.lucene.document.FieldSelectorVisitor;
import org.apache.lucene.document.TextField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;


/**
 * Test demonstrating EOF bug on the last field of the last doc
 * if other docs have allready been accessed.
 */
public class TestLazyBug extends LuceneTestCase {

  public static int NUM_DOCS = TEST_NIGHTLY ? 500 : 50;
  public static int NUM_FIELDS = TEST_NIGHTLY ? 100 : 10;

  private static String[] data = new String[] {
    "now",
    "is the time",
    "for all good men",
    "to come to the aid",
    "of their country!",
    "this string contains big chars:{\u0111 \u0222 \u0333 \u1111 \u2222 \u3333}",
    "this string is a bigger string, mary had a little lamb, little lamb, little lamb!"
  };

  private static Set<String> dataset = asSet(data);

  private static String MAGIC_FIELD = "f"+(NUM_FIELDS/3);
  
  private static Directory directory;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    directory = makeIndex();
  }
  
  @AfterClass
  public static void afterClass() throws Exception {
    directory.close();
    directory = null;
  }

  private static FieldSelector SELECTOR = new FieldSelector() {
      public FieldSelectorResult accept(String f) {
        if (f.equals(MAGIC_FIELD)) {
          return FieldSelectorResult.LOAD;
        }
        return FieldSelectorResult.LAZY_LOAD;
      }
    };

  private static Directory makeIndex() throws Exception {
    Directory dir = newDirectory();
    try {
      IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(
                                                                     TEST_VERSION_CURRENT, new MockAnalyzer(random)).setMergePolicy(newLogMergePolicy()));
      LogMergePolicy lmp = (LogMergePolicy) writer.getConfig().getMergePolicy();
      lmp.setUseCompoundFile(false);
      for (int d = 1; d <= NUM_DOCS; d++) {
        Document doc = new Document();
        for (int f = 1; f <= NUM_FIELDS; f++ ) {
          doc.add(newField("f"+f,
                            data[f % data.length]
                            + '#' + data[random.nextInt(data.length)],
                            TextField.TYPE_UNSTORED));
        }
        writer.addDocument(doc);
      }
      writer.close();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return dir;
  }

  public void doTest(int[] docs) throws Exception {
    IndexReader reader = IndexReader.open(directory, true);
    for (int i = 0; i < docs.length; i++) {
      final FieldSelectorVisitor visitor = new FieldSelectorVisitor(SELECTOR);
      reader.document(docs[i], visitor);
      Document d = visitor.getDocument();
      d.get(MAGIC_FIELD);

      List<IndexableField> fields = d.getFields();
      for (Iterator<IndexableField> fi = fields.iterator(); fi.hasNext(); ) {
        IndexableField f=null;
        try {
          f =  fi.next();
          String fname = f.name();
          String fval = f.stringValue();
          assertNotNull(docs[i]+" FIELD: "+fname, fval);
          String[] vals = fval.split("#");
          if (!dataset.contains(vals[0]) || !dataset.contains(vals[1])) {
            fail("FIELD:"+fname+",VAL:"+fval);
          }
        } catch (Exception e) {
          throw new Exception(docs[i]+" WTF: "+f.name(), e);
        }
      }
    }
    reader.close();
  }

  public void testLazyWorks() throws Exception {
    doTest(new int[] { NUM_DOCS-1 });
  }

  public void testLazyAlsoWorks() throws Exception {
    doTest(new int[] { NUM_DOCS-1, NUM_DOCS/2 });
  }

  public void testLazyBroken() throws Exception {
    doTest(new int[] { NUM_DOCS/2, NUM_DOCS-1 });
  }
}
