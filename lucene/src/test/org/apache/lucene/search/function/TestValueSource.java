package org.apache.lucene.search.function;

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

import org.apache.lucene.util.*;
import org.apache.lucene.store.*;
import org.apache.lucene.search.*;
import org.apache.lucene.search.function.*;
import org.apache.lucene.analysis.*;
import org.apache.lucene.index.*;
import org.apache.lucene.document.*;

public class TestValueSource extends LuceneTestCase {

  public void testMultiValueSource() throws Exception {
    Directory dir = new MockRAMDirectory();
    IndexWriter w = new IndexWriter(dir, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.UNLIMITED);
    Document doc = new Document();
    Field f = new Field("field", "", Field.Store.NO, Field.Index.NOT_ANALYZED);
    doc.add(f);

    for(int i=0;i<17;i++) {
      f.setValue(""+i);
      w.addDocument(doc);
      w.commit();
    }

    IndexReader r = w.getReader();
    w.close();

    assertTrue(r.getSequentialSubReaders().length > 1);

    ValueSource s1 = new IntFieldSource("field");
    DocValues v1 = s1.getValues(r);
    DocValues v2 = new MultiValueSource(s1).getValues(r);

    for(int i=0;i<r.maxDoc();i++) {
      assertEquals(v1.intVal(i), i);
      assertEquals(v2.intVal(i), i);
    }

    FieldCache.DEFAULT.purgeAllCaches();

    r.close();
    dir.close();
  }

}

