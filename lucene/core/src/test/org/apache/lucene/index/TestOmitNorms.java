package org.apache.lucene.index;

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

import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldTypes;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

public class TestOmitNorms extends LuceneTestCase {
  // Tests whether the DocumentWriter correctly enable the
  // omitNorms bit in the FieldInfo
  public void testOmitNorms() throws Exception {
    Directory ram = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random());
    IndexWriter writer = new IndexWriter(ram, newIndexWriterConfig(analyzer));
    FieldTypes fieldTypes = writer.getFieldTypes();

    Document d = writer.newDocument();
        
    // this field will have norms
    d.addLargeText("f1", "This field has norms");
       
    // this field will NOT have norms
    fieldTypes.disableNorms("f2");
    d.addLargeText("f2", "This field has NO norms in all docs");
        
    writer.addDocument(d);
    writer.forceMerge(1);

    // now we add another document which has term freq for field f2 and not for f1 and verify if the SegmentMerger
    // keep things constant
    d = writer.newDocument();
        
    // Reverse
    fieldTypes.disableNorms("f1");
    d.addLargeText("f1", "This field has norms");
    d.addLargeText("f2", "This field has NO norms in all docs");
        
    writer.addDocument(d);

    // force merge
    writer.forceMerge(1);
    // flush
    writer.close();

    SegmentReader reader = getOnlySegmentReader(DirectoryReader.open(ram));
    FieldInfos fi = reader.getFieldInfos();
    assertTrue("OmitNorms field bit should be set.", fi.fieldInfo("f1").omitsNorms());
    assertTrue("OmitNorms field bit should be set.", fi.fieldInfo("f2").omitsNorms());
        
    reader.close();
    ram.close();
  }
 
  // Tests whether merging of docs that have different
  // omitNorms for the same field works
  public void testMixedMerge() throws Exception {
    Directory ram = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random());
    IndexWriter writer = new IndexWriter(
        ram,
        newIndexWriterConfig(analyzer)
           .setMaxBufferedDocs(3)
           .setMergePolicy(newLogMergePolicy(2))
    );
    FieldTypes fieldTypes = writer.getFieldTypes();

    Document d = writer.newDocument();
        
    // this field will have norms
    d.addLargeText("f1", "This field has norms");
       
    // this field will NOT have norms
    fieldTypes.disableNorms("f2");
    d.addLargeText("f2", "This field has NO norms in all docs");

    for (int i = 0; i < 30; i++) {
      writer.addDocument(d);
    }
        
    // now we add another document which has norms for field f2 and not for f1 and verify if the SegmentMerger
    // keep things constant
    d = writer.newDocument();
        
    // Reverese
    fieldTypes.disableNorms("f1");
    d.addLargeText("f1", "This field has norms");
    d.addLargeText("f2", "This field has NO norms in all docs");
        
    for (int i = 0; i < 30; i++) {
      writer.addDocument(d);
    }
        
    // force merge
    writer.forceMerge(1);
    // flush
    writer.close();

    SegmentReader reader = getOnlySegmentReader(DirectoryReader.open(ram));
    FieldInfos fi = reader.getFieldInfos();
    assertTrue("OmitNorms field bit should be set.", fi.fieldInfo("f1").omitsNorms());
    assertTrue("OmitNorms field bit should be set.", fi.fieldInfo("f2").omitsNorms());
        
    reader.close();
    ram.close();
  }

  // Make sure first adding docs that do not omitNorms for
  // field X, then adding docs that do omitNorms for that same
  // field, 
  public void testMixedRAM() throws Exception {
    Directory ram = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random());
    IndexWriter writer = new IndexWriter(
        ram,
        newIndexWriterConfig(analyzer)
            .setMaxBufferedDocs(10)
            .setMergePolicy(newLogMergePolicy(2))
    );
    FieldTypes fieldTypes = writer.getFieldTypes();

    Document d = writer.newDocument();
        
    // this field will have norms
    d.addLargeText("f1", "This field has norms");
       
    // this field will NOT have norms
    fieldTypes.disableNorms("f2");
    d.addLargeText("f2", "This field has NO norms in all docs");

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

    SegmentReader reader = getOnlySegmentReader(DirectoryReader.open(ram));
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
    IndexWriter writer = new IndexWriter(ram, newIndexWriterConfig(analyzer)
                                                .setMaxBufferedDocs(3)
                                                .setMergePolicy(newLogMergePolicy()));
    FieldTypes fieldTypes = writer.getFieldTypes();

    LogMergePolicy lmp = (LogMergePolicy) writer.getConfig().getMergePolicy();
    lmp.setMergeFactor(2);
    lmp.setNoCFSRatio(0.0);

    Document d = writer.newDocument();

    fieldTypes.disableNorms("f1");
    d.addLargeText("f1", "This field has no norms");
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
