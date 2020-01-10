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
package org.apache.lucene.document;

import java.io.IOException;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

public class TestFeatureDoubleValues extends LuceneTestCase {

  public void testFeature() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig config = newIndexWriterConfig().setMergePolicy(newLogMergePolicy(random().nextBoolean()));
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, config);
    Document doc = new Document();
    doc.add(new FeatureField("field", "name", 30F));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new FeatureField("field", "name", 1F));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new FeatureField("field", "name", 4F));
    writer.addDocument(doc);
    writer.forceMerge(1);
    IndexReader ir = writer.getReader();
    writer.close();

    assertEquals(1, ir.leaves().size());
    LeafReaderContext context = ir.leaves().get(0);
    DoubleValuesSource valuesSource = FeatureField.newDoubleValues("field", "name");
    DoubleValues values = valuesSource.getValues(context, null);

    assertTrue(values.advanceExact(0));
    assertEquals(30, values.doubleValue(), 0f);
    assertTrue(values.advanceExact(1));
    assertEquals(1, values.doubleValue(), 0f);
    assertTrue(values.advanceExact(2));
    assertEquals(4, values.doubleValue(), 0f);
    assertFalse(values.advanceExact(3));

    ir.close();
    dir.close();
  }

  public void testFeatureMissing() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig config = newIndexWriterConfig().setMergePolicy(newLogMergePolicy(random().nextBoolean()));
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, config);
    Document doc = new Document();
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new FeatureField("field", "name", 1F));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new FeatureField("field", "name", 4F));
    writer.addDocument(doc);
    writer.forceMerge(1);
    IndexReader ir = writer.getReader();
    writer.close();

    assertEquals(1, ir.leaves().size());
    LeafReaderContext context = ir.leaves().get(0);
    DoubleValuesSource valuesSource = FeatureField.newDoubleValues("field", "name");
    DoubleValues values = valuesSource.getValues(context, null);

    assertFalse(values.advanceExact(0));
    assertTrue(values.advanceExact(1));
    assertEquals(1, values.doubleValue(), 0f);
    assertTrue(values.advanceExact(2));
    assertEquals(4, values.doubleValue(), 0f);
    assertFalse(values.advanceExact(3));

    ir.close();
    dir.close();
  }

  public void testFeatureMissingFieldInSegment() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig config = newIndexWriterConfig().setMergePolicy(newLogMergePolicy(random().nextBoolean()));
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, config);
    Document doc = new Document();
    writer.addDocument(doc);
    writer.commit();
    IndexReader ir = writer.getReader();
    writer.close();
    
    assertEquals(1, ir.leaves().size());
    LeafReaderContext context = ir.leaves().get(0);
    DoubleValuesSource valuesSource = FeatureField.newDoubleValues("field", "name");
    DoubleValues values = valuesSource.getValues(context, null);

    assertFalse(values.advanceExact(0));
    assertFalse(values.advanceExact(1));

    ir.close();
    dir.close();
  }

  public void testFeatureMissingFeatureNameInSegment() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig config = newIndexWriterConfig().setMergePolicy(newLogMergePolicy(random().nextBoolean()));
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, config);
    Document doc = new Document();
    doc.add(new FeatureField("field", "different_name", 0.5F));
    writer.addDocument(doc);
    writer.commit();
    IndexReader ir = writer.getReader();
    writer.close();
    
    assertEquals(1, ir.leaves().size());
    LeafReaderContext context = ir.leaves().get(0);
    DoubleValuesSource valuesSource = FeatureField.newDoubleValues("field", "name");
    DoubleValues values = valuesSource.getValues(context, null);

    assertFalse(values.advanceExact(0));
    assertFalse(values.advanceExact(1));

    ir.close();
    dir.close();
  }

  public void testFeatureMultipleMissing() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig config = newIndexWriterConfig().setMergePolicy(newLogMergePolicy(random().nextBoolean()));
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, config);
    Document doc = new Document();
    writer.addDocument(doc);
    doc = new Document();
    writer.addDocument(doc);
    doc = new Document();
    writer.addDocument(doc);
    doc = new Document();
    writer.addDocument(doc);
    doc = new Document();
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new FeatureField("field", "name", 1F));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new FeatureField("field", "name", 4F));
    writer.addDocument(doc);
    writer.forceMerge(1);
    IndexReader ir = writer.getReader();
    writer.close();

    assertEquals(1, ir.leaves().size());
    LeafReaderContext context = ir.leaves().get(0);
    DoubleValuesSource valuesSource = FeatureField.newDoubleValues("field", "name");
    DoubleValues values = valuesSource.getValues(context, null);

    assertFalse(values.advanceExact(0));
    assertFalse(values.advanceExact(1));
    assertFalse(values.advanceExact(2));
    assertFalse(values.advanceExact(3));
    assertFalse(values.advanceExact(4));
    assertTrue(values.advanceExact(5));
    assertEquals(1, values.doubleValue(), 0f);
    assertTrue(values.advanceExact(6));
    assertEquals(4, values.doubleValue(), 0f);
    assertFalse(values.advanceExact(7));

    ir.close();
    dir.close();
  }
  
  public void testHashCodeAndEquals() {
    FeatureDoubleValuesSource valuesSource = new FeatureDoubleValuesSource("test_field", "test_feature");
    FeatureDoubleValuesSource equal = new FeatureDoubleValuesSource("test_field", "test_feature");

    FeatureDoubleValuesSource differentField = new FeatureDoubleValuesSource("other field", "test_feature");
    FeatureDoubleValuesSource differentFeature = new FeatureDoubleValuesSource("test_field", "other_feature");
    DoubleValuesSource otherImpl = new DoubleValuesSource() {
      
      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        return false;
      }
      
      @Override
      public String toString() {
        return null;
      }
      
      @Override
      public DoubleValuesSource rewrite(IndexSearcher reader) throws IOException {
        return null;
      }
      
      @Override
      public boolean needsScores() {
        return false;
      }
      
      @Override
      public int hashCode() {
        return 0;
      }
      
      @Override
      public DoubleValues getValues(LeafReaderContext ctx, DoubleValues scores) throws IOException {
        return null;
      }
      
      @Override
      public boolean equals(Object obj) {
        return false;
      }
    };
    
    assertTrue(valuesSource.equals(equal));
    assertEquals(valuesSource.hashCode(), equal.hashCode());
    assertFalse(valuesSource.equals(null));
    assertFalse(valuesSource.equals(otherImpl));
    assertNotEquals(valuesSource.hashCode(), otherImpl.hashCode());
    assertFalse(valuesSource.equals(differentField));
    assertNotEquals(valuesSource.hashCode(), differentField.hashCode());
    assertFalse(valuesSource.equals(differentFeature));
    assertNotEquals(valuesSource.hashCode(), differentFeature.hashCode());
  }
}
