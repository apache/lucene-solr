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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

public class TestRandomStoredFields extends LuceneTestCase {

  public void testRandomStoredFields() throws IOException {
    Directory dir = newDirectory();
    Random rand = random();
    RandomIndexWriter w = new RandomIndexWriter(rand, dir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())).setMaxBufferedDocs(_TestUtil.nextInt(rand, 5, 20)));
    //w.w.setUseCompoundFile(false);
    final int docCount = atLeast(200);
    final int fieldCount = _TestUtil.nextInt(rand, 1, 5);

    final List<Integer> fieldIDs = new ArrayList<Integer>();

    FieldType customType = new FieldType(TextField.TYPE_STORED);
    customType.setTokenized(false);
    Field idField = newField("id", "", customType);

    for(int i=0;i<fieldCount;i++) {
      fieldIDs.add(i);
    }

    final Map<String,Document> docs = new HashMap<String,Document>();

    if (VERBOSE) {
      System.out.println("TEST: build index docCount=" + docCount);
    }

    FieldType customType2 = new FieldType();
    customType2.setStored(true);
    for(int i=0;i<docCount;i++) {
      Document doc = new Document();
      doc.add(idField);
      final String id = ""+i;
      idField.setStringValue(id);
      docs.put(id, doc);
      if (VERBOSE) {
        System.out.println("TEST: add doc id=" + id);
      }

      for(int field: fieldIDs) {
        final String s;
        if (rand.nextInt(4) != 3) {
          s = _TestUtil.randomUnicodeString(rand, 1000);
          doc.add(newField("f"+field, s, customType2));
        } else {
          s = null;
        }
      }
      w.addDocument(doc);
      if (rand.nextInt(50) == 17) {
        // mixup binding of field name -> Number every so often
        Collections.shuffle(fieldIDs);
      }
      if (rand.nextInt(5) == 3 && i > 0) {
        final String delID = ""+rand.nextInt(i);
        if (VERBOSE) {
          System.out.println("TEST: delete doc id=" + delID);
        }
        w.deleteDocuments(new Term("id", delID));
        docs.remove(delID);
      }
    }

    if (VERBOSE) {
      System.out.println("TEST: " + docs.size() + " docs in index; now load fields");
    }
    if (docs.size() > 0) {
      String[] idsList = docs.keySet().toArray(new String[docs.size()]);

      for(int x=0;x<2;x++) {
        IndexReader r = w.getReader();
        IndexSearcher s = newSearcher(r);

        if (VERBOSE) {
          System.out.println("TEST: cycle x=" + x + " r=" + r);
        }

        int num = atLeast(1000);
        for(int iter=0;iter<num;iter++) {
          String testID = idsList[rand.nextInt(idsList.length)];
          if (VERBOSE) {
            System.out.println("TEST: test id=" + testID);
          }
          TopDocs hits = s.search(new TermQuery(new Term("id", testID)), 1);
          assertEquals(1, hits.totalHits);
          Document doc = r.document(hits.scoreDocs[0].doc);
          Document docExp = docs.get(testID);
          for(int i=0;i<fieldCount;i++) {
            assertEquals("doc " + testID + ", field f" + fieldCount + " is wrong", docExp.get("f"+i),  doc.get("f"+i));
          }
        }
        r.close();
        w.forceMerge(1);
      }
    }
    w.close();
    dir.close();
  }
}
