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

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;

import java.util.*;
import java.lang.reflect.Array;


/**
 * Test demonstrating EOF bug on the last field of the last doc 
 * if other docs have allready been accessed.
 */
public class TestLazyBug extends LuceneTestCase {

  public static int BASE_SEED = 13;

  public static int NUM_DOCS = 500;
  public static int NUM_FIELDS = 100;

  private static String[] data = new String[] {
    "now",
    "is the time",
    "for all good men",
    "to come to the aid",
    "of their country!",
    "this string contains big chars:{\u0111 \u0222 \u0333 \u1111 \u2222 \u3333}",
    "this string is a bigger string, mary had a little lamb, little lamb, little lamb!"
  };

  private static Set dataset = new HashSet(Arrays.asList(data));
  
  private static String MAGIC_FIELD = "f"+(NUM_FIELDS/3);
  
  private static FieldSelector SELECTOR = new FieldSelector() {
      public FieldSelectorResult accept(String f) {
        if (f.equals(MAGIC_FIELD)) {
          return FieldSelectorResult.LOAD;
        }
        return FieldSelectorResult.LAZY_LOAD;
      }
    };
  
  private static Directory makeIndex() throws RuntimeException { 
    Directory dir = new RAMDirectory();
    try {
      Random r = new Random(BASE_SEED + 42) ; 
      Analyzer analyzer = new SimpleAnalyzer();
      IndexWriter writer = new IndexWriter(dir, analyzer, true, IndexWriter.MaxFieldLength.LIMITED);
      
      writer.setUseCompoundFile(false);
      
      for (int d = 1; d <= NUM_DOCS; d++) {
        Document doc = new Document();
        for (int f = 1; f <= NUM_FIELDS; f++ ) {
          doc.add(new Field("f"+f, 
                            data[f % data.length] 
                            + '#' + data[r.nextInt(data.length)], 
                            Field.Store.YES, 
                            Field.Index.ANALYZED));
        }
        writer.addDocument(doc);
      }
      writer.close();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return dir;
  }
  
  public static void doTest(int[] docs) throws Exception {
    Directory dir = makeIndex();
    IndexReader reader = IndexReader.open(dir);
    for (int i = 0; i < docs.length; i++) {
      Document d = reader.document(docs[i], SELECTOR);
      String trash = d.get(MAGIC_FIELD);
      
      List fields = d.getFields();
      for (Iterator fi = fields.iterator(); fi.hasNext(); ) {
        Fieldable f=null;
        try {
          f = (Fieldable) fi.next();
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
    doTest(new int[] { 399 });
  }
  
  public void testLazyAlsoWorks() throws Exception {
    doTest(new int[] { 399, 150 });
  }

  public void testLazyBroken() throws Exception {
    doTest(new int[] { 150, 399 });
  }

}
