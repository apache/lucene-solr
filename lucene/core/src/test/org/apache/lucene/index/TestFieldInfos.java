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
import java.util.Iterator;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.sameInstance;

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

  public void testFieldAttributes() throws Exception{
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random()))
        .setMergePolicy(NoMergePolicy.INSTANCE));

    FieldType type1 = new FieldType();
    type1.setStored(true);
    type1.putAttribute("testKey1", "testValue1");

    Document d1 = new Document();
    d1.add(new Field("f1", "v1", type1));
    FieldType type2 = new FieldType(type1);
    //changing the value after copying shouldn't impact the original type1
    type2.putAttribute("testKey1", "testValue2");
    writer.addDocument(d1);
    writer.commit();

    Document d2 = new Document();
    type1.putAttribute("testKey1", "testValueX");
    type1.putAttribute("testKey2", "testValue2");
    d2.add(new Field("f1", "v2", type1));
    d2.add(new Field("f2", "v2", type2));
    writer.addDocument(d2);
    writer.commit();
    writer.forceMerge(1);

    IndexReader reader = writer.getReader();
    FieldInfos fis = FieldInfos.getMergedFieldInfos(reader);
    assertEquals(fis.size(), 2);
    Iterator<FieldInfo>  it = fis.iterator();
    while(it.hasNext()) {
      FieldInfo fi = it.next();
      switch (fi.name) {
        case "f1":
          // testKey1 can point to either testValue1 or testValueX based on the order
          // of merge, but we see textValueX winning here since segment_2 is merged on segment_1.
          assertEquals("testValueX", fi.getAttribute("testKey1"));
          assertEquals("testValue2", fi.getAttribute("testKey2"));
          break;
        case "f2":
          assertEquals("testValue2", fi.getAttribute("testKey1"));
          break;
        default:
          assertFalse("Unknown field", true);
      }
    }
    reader.close();
    writer.close();
    dir.close();
  }

  public void testMergedFieldInfos_empty() throws IOException {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));

    IndexReader reader = writer.getReader();
    FieldInfos actual = FieldInfos.getMergedFieldInfos(reader);
    FieldInfos expected = FieldInfos.EMPTY;

    assertThat(actual, sameInstance(expected));

    reader.close();
    writer.close();
    dir.close();
  }

  public void testMergedFieldInfos_singleLeaf() throws IOException {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));

    Document d1 = new Document();
    d1.add(new StringField("f1", "v1", Field.Store.YES));
    writer.addDocument(d1);
    writer.commit();

    Document d2 = new Document();
    d2.add(new StringField("f2", "v2", Field.Store.YES));
    writer.addDocument(d2);
    writer.commit();

    writer.forceMerge(1);

    IndexReader reader = writer.getReader();
    FieldInfos actual = FieldInfos.getMergedFieldInfos(reader);
    FieldInfos expected = reader.leaves().get(0).reader().getFieldInfos();

    assertThat(reader.leaves().size(), equalTo(1));
    assertThat(actual, sameInstance(expected));

    reader.close();
    writer.close();
    dir.close();
  }

  public void testFieldNumbersAutoIncrement() {
    FieldInfos.FieldNumbers fieldNumbers = new FieldInfos.FieldNumbers("softDeletes");
    for (int i = 0; i < 10; i++) {
      fieldNumbers.addOrGet("field" + i, -1, IndexOptions.NONE, DocValuesType.NONE,
          0, 0, 0, false);
    }
    int idx = fieldNumbers.addOrGet("EleventhField", -1, IndexOptions.NONE, DocValuesType.NONE,
        0, 0, 0, false);
    assertEquals("Field numbers 0 through 9 were allocated", 10, idx);

    fieldNumbers.clear();
    idx = fieldNumbers.addOrGet("PostClearField", -1, IndexOptions.NONE, DocValuesType.NONE,
        0, 0, 0, false);
    assertEquals("Field numbers should reset after clear()", 0, idx);
  }
}
