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

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FieldInfosReader;
import org.apache.lucene.codecs.FieldInfosWriter;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;

import java.io.IOException;
import java.util.Arrays;

//import org.cnlp.utils.properties.ResourceBundleHelper;

public class TestFieldInfos extends LuceneTestCase {

  private Document testDoc = new Document();

  @Override
  public void setUp() throws Exception {
    super.setUp();
    DocHelper.setupDoc(testDoc);
  }

  public FieldInfos createAndWriteFieldInfos(Directory dir, String filename) throws IOException{
  //Positive test of FieldInfos
    assertTrue(testDoc != null);
    FieldInfos.Builder builder = new FieldInfos.Builder();
    for (IndexableField field : testDoc) {
      builder.addOrUpdate(field.name(), field.fieldType());
    }
    FieldInfos fieldInfos = builder.finish();
    //Since the complement is stored as well in the fields map
    assertTrue(fieldInfos.size() == DocHelper.all.size()); //this is all b/c we are using the no-arg constructor
    
    
    IndexOutput output = dir.createOutput(filename, newIOContext(random()));
    assertTrue(output != null);
    //Use a RAMOutputStream
  
    FieldInfosWriter writer = Codec.getDefault().fieldInfosFormat().getFieldInfosWriter();
    writer.write(dir, filename, fieldInfos, IOContext.DEFAULT);
    output.close();
    return fieldInfos;
  }
  
  public FieldInfos readFieldInfos(Directory dir, String filename) throws IOException {
    FieldInfosReader reader = Codec.getDefault().fieldInfosFormat().getFieldInfosReader();
    return reader.read(dir, filename, IOContext.DEFAULT);
  }
  
  public void test() throws IOException {
    String name = "testFile";
    Directory dir = newDirectory();
    FieldInfos fieldInfos = createAndWriteFieldInfos(dir, name);

    FieldInfos readIn = readFieldInfos(dir, name);
    assertTrue(fieldInfos.size() == readIn.size());
    FieldInfo info = readIn.fieldInfo("textField1");
    assertTrue(info != null);
    assertTrue(info.hasVectors() == false);
    assertTrue(info.omitsNorms() == false);

    info = readIn.fieldInfo("textField2");
    assertTrue(info != null);
    assertTrue(info.omitsNorms() == false);

    info = readIn.fieldInfo("textField3");
    assertTrue(info != null);
    assertTrue(info.hasVectors() == false);
    assertTrue(info.omitsNorms() == true);

    info = readIn.fieldInfo("omitNorms");
    assertTrue(info != null);
    assertTrue(info.hasVectors() == false);
    assertTrue(info.omitsNorms() == true);

    dir.close();
  }
  
  public void testReadOnly() throws IOException {
    String name = "testFile";
    Directory dir = newDirectory();
    FieldInfos fieldInfos = createAndWriteFieldInfos(dir, name);
    FieldInfos readOnly = readFieldInfos(dir, name);
    assertReadOnly(readOnly, fieldInfos);
    dir.close();
  }
  
  private void assertReadOnly(FieldInfos readOnly, FieldInfos modifiable) {    
    assertEquals(modifiable.size(), readOnly.size());
    // assert we can iterate
    for (FieldInfo fi : readOnly) {
      assertEquals(fi.name, modifiable.fieldInfo(fi.number).name);
    }
  }
}
