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
package org.apache.lucene.store;


import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.function.Supplier;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.util.English;
import org.junit.Test;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

public class TestByteBuffersDirectory extends BaseDirectoryTestCase {
  private Supplier<ByteBuffersDirectory> implSupplier;

  public TestByteBuffersDirectory(Supplier<ByteBuffersDirectory> implSupplier, String name) {
    this.implSupplier = implSupplier;
  }
  
  @Override
  protected Directory getDirectory(Path path) throws IOException {
    return implSupplier.get();
  }

  @Test
  public void testBuildIndex() throws IOException {
    try (Directory dir = getDirectory(null);
         IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(
            new MockAnalyzer(random())).setOpenMode(OpenMode.CREATE))) {
      int docs = RandomizedTest.randomIntBetween(0, 10);
      for (int i = docs; i > 0; i--) {
        Document doc = new Document();
        doc.add(newStringField("content", English.intToEnglish(i).trim(), Field.Store.YES));
        writer.addDocument(doc);
      }
      writer.commit();
      assertEquals(docs, writer.getDocStats().numDocs);
    }
  }
  
  @ParametersFactory(argumentFormatting = "impl=%2$s")
  public static Iterable<Object[]> parametersWithCustomName() {
    return Arrays.asList(new Object [][] {
      {(Supplier<ByteBuffersDirectory>) () -> new ByteBuffersDirectory(
          new SingleInstanceLockFactory(), 
          ByteBuffersDataOutput::new,
          ByteBuffersDirectory.OUTPUT_AS_MANY_BUFFERS), "many buffers (heap)"},
      {(Supplier<ByteBuffersDirectory>) () -> new ByteBuffersDirectory(
          new SingleInstanceLockFactory(), 
          ByteBuffersDataOutput::new,
          ByteBuffersDirectory.OUTPUT_AS_ONE_BUFFER), "one buffer (heap)"},
      {(Supplier<ByteBuffersDirectory>) () -> new ByteBuffersDirectory(
          new SingleInstanceLockFactory(), 
          ByteBuffersDataOutput::new,
          ByteBuffersDirectory.OUTPUT_AS_MANY_BUFFERS_LUCENE), "lucene's buffers (heap)"},
      {(Supplier<ByteBuffersDirectory>) () -> new ByteBuffersDirectory(
          new SingleInstanceLockFactory(), 
          ByteBuffersDataOutput::new,
          ByteBuffersDirectory.OUTPUT_AS_BYTE_ARRAY), "byte array (heap)"},
    });
  }
}
