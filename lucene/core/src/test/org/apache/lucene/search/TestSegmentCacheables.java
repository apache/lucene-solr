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

package org.apache.lucene.search;

import java.io.IOException;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

public class TestSegmentCacheables extends LuceneTestCase {

  private static boolean isCacheable(LeafReaderContext ctx, SegmentCacheable... ss) {
    for (SegmentCacheable s : ss) {
      if (s.isCacheable(ctx) == false)
        return false;
    }
    return true;
  }

  public void testMultipleDocValuesDelegates() throws IOException {

    SegmentCacheable seg = (ctx) -> true;
    SegmentCacheable non = (ctx) -> false;
    SegmentCacheable dv1 = (ctx) -> DocValues.isCacheable(ctx, "field1");
    SegmentCacheable dv2 = (ctx) -> DocValues.isCacheable(ctx, "field2");
    SegmentCacheable dv3 = (ctx) -> DocValues.isCacheable(ctx, "field3");
    SegmentCacheable dv34 = (ctx) -> DocValues.isCacheable(ctx, "field3", "field4");
    SegmentCacheable dv12 = (ctx) -> DocValues.isCacheable(ctx, "field1", "field2");

    SegmentCacheable seg_dv1 = (ctx) -> isCacheable(ctx, seg, dv1);
    SegmentCacheable dv2_dv34 = (ctx) -> isCacheable(ctx, dv2, dv34);
    SegmentCacheable dv2_non = (ctx) -> isCacheable(ctx, dv2, non);

    SegmentCacheable seg_dv1_dv2_dv34 = (ctx) -> isCacheable(ctx, seg_dv1, dv2_dv34);

    SegmentCacheable dv1_dv3 = (ctx) -> isCacheable(ctx, dv1, dv3);
    SegmentCacheable dv12_dv1_dv3 = (ctx) -> isCacheable(ctx, dv12, dv1_dv3);


    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE);
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new NumericDocValuesField("field3", 1));
    doc.add(newTextField("text", "text", Field.Store.NO));
    w.addDocument(doc);
    w.commit();
    DirectoryReader reader = DirectoryReader.open(w);

    LeafReaderContext ctx = reader.leaves().get(0);

    assertTrue(seg_dv1.isCacheable(ctx));
    assertTrue(dv2_dv34.isCacheable(ctx));
    assertTrue(seg_dv1_dv2_dv34.isCacheable(ctx));
    assertFalse(dv2_non.isCacheable(ctx));

    w.updateNumericDocValue(new Term("text", "text"), "field3", 2l);
    w.commit();
    reader.close();
    reader = DirectoryReader.open(dir);

    // after field3 is updated, all composites referring to it should be uncacheable

    ctx = reader.leaves().get(0);
    assertTrue(seg_dv1.isCacheable(ctx));
    assertFalse(dv34.isCacheable(ctx));
    assertFalse(dv2_dv34.isCacheable(ctx));
    assertFalse(dv1_dv3.isCacheable(ctx));
    assertFalse(seg_dv1_dv2_dv34.isCacheable(ctx));
    assertFalse(dv12_dv1_dv3.isCacheable(ctx));

    reader.close();
    w.close();
    dir.close();

  }

}
