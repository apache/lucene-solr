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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

public class TestOmitNorms extends LuceneTestCase {

  // Tests that merging of docs with different omitNorms throws error
  public void testMixedMergeThrowsError() throws Exception {
    Directory ram = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random());
    IndexWriter writer =
        new IndexWriter(
            ram,
            newIndexWriterConfig(analyzer)
                .setMaxBufferedDocs(3)
                .setMergePolicy(newLogMergePolicy(2)));
    Document d = new Document();

    // this field will have norms
    FieldType fieldType1 = new FieldType(TextField.TYPE_NOT_STORED);
    fieldType1.setOmitNorms(false);
    fieldType1.setStoreTermVectors(false);
    Field f1 = new Field("f1", "This field has norms", fieldType1);
    d.add(f1);

    // this field will NOT have norms
    FieldType fieldType2 = new FieldType(TextField.TYPE_NOT_STORED);
    fieldType2.setOmitNorms(true);
    fieldType2.setStoreTermVectors(false);
    Field f2 = new Field("f2", "This field has NO norms in all docs", fieldType2);
    d.add(f2);

    for (int i = 0; i < 30; i++) {
      writer.addDocument(d);
    }

    // reverse omitNorms options for f1 and f2
    Document d2 = new Document();
    d2.add(new Field("f1", "This field has NO norms", fieldType2));
    d2.add(new Field("f2", "This field has norms", fieldType1));

    IllegalArgumentException exception =
        expectThrows(IllegalArgumentException.class, () -> writer.addDocument(d2));
    assertEquals("cannot change field \"f1\" from omitNorms=false to inconsistent omitNorms=true", exception.getMessage());

    writer.forceMerge(1);
    writer.close();

    LeafReader reader = getOnlyLeafReader(DirectoryReader.open(ram));
    FieldInfos fi = reader.getFieldInfos();
    // assert original omitNorms
    assertTrue("OmitNorms field bit must not be set.", fi.fieldInfo("f1").omitsNorms() == false);
    assertTrue("OmitNorms field bit must be set.", fi.fieldInfo("f2").omitsNorms());

    reader.close();
    ram.close();
  }

  // Make sure first adding docs that do not omitNorms for
  // field X, then adding docs that do omitNorms for that same
  // field,
  public void testMixedRAM() throws Exception {
    Directory ram = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random());
    IndexWriter writer =
        new IndexWriter(
            ram,
            newIndexWriterConfig(analyzer)
                .setMaxBufferedDocs(10)
                .setMergePolicy(newLogMergePolicy(2)));
    Document d = new Document();

    // this field will have norms
    Field f1 = newTextField("f1", "This field has norms", Field.Store.NO);
    d.add(f1);

    // this field will NOT have norms

    FieldType customType = new FieldType(TextField.TYPE_NOT_STORED);
    customType.setOmitNorms(true);
    Field f2 = newField("f2", "This field has NO norms in all docs", customType);
    d.add(f2);

    for (int i = 0; i < 5; i++) {
      writer.addDocument(d);
    }

    for (int i = 0; i < 20; i++) {
      writer.addDocument(d);
    }

    // force merge
    writer.forceMerge(1);

    // flush
    writer.close();

    LeafReader reader = getOnlyLeafReader(DirectoryReader.open(ram));
    FieldInfos fi = reader.getFieldInfos();
    assertTrue("OmitNorms field bit should not be set.", !fi.fieldInfo("f1").omitsNorms());
    assertTrue("OmitNorms field bit should be set.", fi.fieldInfo("f2").omitsNorms());

    reader.close();
    ram.close();
  }

  private void assertNoNrm(Directory dir) throws Throwable {
    final String[] files = dir.listAll();
    for (int i = 0; i < files.length; i++) {
      // TODO: this relies upon filenames
      assertFalse(files[i].endsWith(".nrm") || files[i].endsWith(".len"));
    }
  }

  // Verifies no *.nrm exists when all fields omit norms:
  public void testNoNrmFile() throws Throwable {
    Directory ram = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random());
    IndexWriter writer =
        new IndexWriter(
            ram,
            newIndexWriterConfig(analyzer)
                .setMaxBufferedDocs(3)
                .setMergePolicy(newLogMergePolicy()));
    LogMergePolicy lmp = (LogMergePolicy) writer.getConfig().getMergePolicy();
    lmp.setMergeFactor(2);
    lmp.setNoCFSRatio(0.0);
    Document d = new Document();

    FieldType customType = new FieldType(TextField.TYPE_NOT_STORED);
    customType.setOmitNorms(true);
    Field f1 = newField("f1", "This field has no norms", customType);
    d.add(f1);

    for (int i = 0; i < 30; i++) {
      writer.addDocument(d);
    }

    writer.commit();

    assertNoNrm(ram);

    // force merge
    writer.forceMerge(1);
    // flush
    writer.close();

    assertNoNrm(ram);
    ram.close();
  }

}
