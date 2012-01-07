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
import java.util.Random;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.search.similarities.DefaultSimilarityProvider;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.search.similarities.SimilarityProvider;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

/**
 * Test that norms info is preserved during index life - including
 * separate norms, addDocument, addIndexes, forceMerge.
 */
public class TestNorms extends LuceneTestCase {
  
  class CustomNormEncodingSimilarity extends DefaultSimilarity {
    @Override
    public byte encodeNormValue(float f) {
      return (byte) f;
    }
    
    @Override
    public float decodeNormValue(byte b) {
      return (float) b;
    }

    @Override
    public byte computeNorm(FieldInvertState state) {
      return encodeNormValue((float) state.getLength());
    }
  }
  
  // LUCENE-1260
  public void testCustomEncoder() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig config = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random));
    config.setSimilarityProvider(new DefaultSimilarityProvider() {
      @Override
      public Similarity get(String field) {
        return new CustomNormEncodingSimilarity();
      }
    });
    RandomIndexWriter writer = new RandomIndexWriter(random, dir, config);
    Document doc = new Document();
    Field foo = newField("foo", "", TextField.TYPE_UNSTORED);
    Field bar = newField("bar", "", TextField.TYPE_UNSTORED);
    doc.add(foo);
    doc.add(bar);
    
    for (int i = 0; i < 100; i++) {
      bar.setValue("singleton");
      writer.addDocument(doc);
    }
    
    IndexReader reader = writer.getReader();
    writer.close();
    
    byte fooNorms[] = (byte[]) MultiDocValues.getNormDocValues(reader, "foo").getSource().getArray();
    for (int i = 0; i < reader.maxDoc(); i++)
      assertEquals(0, fooNorms[i]);
    
    byte barNorms[] = (byte[]) MultiDocValues.getNormDocValues(reader, "bar").getSource().getArray();
    for (int i = 0; i < reader.maxDoc(); i++)
      assertEquals(1, barNorms[i]);
    
    reader.close();
    dir.close();
  }
}
