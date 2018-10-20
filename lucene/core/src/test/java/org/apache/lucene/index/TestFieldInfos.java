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


import java.util.Iterator;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

public class TestFieldInfos extends LuceneTestCase {

  public void testFieldInfos() throws Exception{
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random()))
        .setMergePolicy(NoMergePolicy.INSTANCE));

    Document d1 = new Document();
    for (int i = 0; i < 15; i++) {
      d1.add(new StringField("f" + i, "v" + i, Field.Store.YES));
    }
    writer.addDocument(d1);
    writer.commit();

    Document d2 = new Document();
    d2.add(new StringField("f0", "v0", Field.Store.YES));
    d2.add(new StringField("f15", "v15", Field.Store.YES));
    d2.add(new StringField("f16", "v16", Field.Store.YES));
    writer.addDocument(d2);
    writer.commit();

    Document d3 = new Document();
    writer.addDocument(d3);
    writer.close();

    SegmentInfos sis = SegmentInfos.readLatestCommit(dir);
    assertEquals(3, sis.size());

    FieldInfos fis1 = IndexWriter.readFieldInfos(sis.info(0));
    FieldInfos fis2 = IndexWriter.readFieldInfos(sis.info(1));
    FieldInfos fis3 = IndexWriter.readFieldInfos(sis.info(2));

    // testing dense FieldInfos
    Iterator<FieldInfo>  it = fis1.iterator();
    int i = 0;
    while(it.hasNext()) {
      FieldInfo fi = it.next();
      assertEquals(i, fi.number);
      assertEquals("f" + i , fi.name);
      assertEquals("f" + i, fis1.fieldInfo(i).name); //lookup by number
      assertEquals("f" + i, fis1.fieldInfo("f" + i).name); //lookup by name
      i++;
    }

    // testing sparse FieldInfos
    assertEquals("f0", fis2.fieldInfo(0).name); //lookup by number
    assertEquals("f0", fis2.fieldInfo("f0").name); //lookup by name
    assertNull(fis2.fieldInfo(1));
    assertNull(fis2.fieldInfo("f1"));
    assertEquals("f15", fis2.fieldInfo(15).name);
    assertEquals("f15", fis2.fieldInfo("f15").name);
    assertEquals("f16", fis2.fieldInfo(16).name);
    assertEquals("f16", fis2.fieldInfo("f16").name);

    // testing empty FieldInfos
    assertNull(fis3.fieldInfo(0)); //lookup by number
    assertNull(fis3.fieldInfo("f0")); //lookup by name
    assertEquals(0, fis3.size());
    Iterator<FieldInfo> it3 = fis3.iterator();
    assertFalse(it3.hasNext());
    dir.close();
  }

}
