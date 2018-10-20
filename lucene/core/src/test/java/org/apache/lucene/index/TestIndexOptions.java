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
import java.util.Collections;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.TextField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;

public class TestIndexOptions extends LuceneTestCase {

  public void testChangeIndexOptionsViaAddDocument() throws IOException {
    for (IndexOptions from : IndexOptions.values()) {
      for (IndexOptions to : IndexOptions.values()) {
        for (boolean preExisting : new boolean[] { false, true }) {
          for (boolean onNewSegment : new boolean[] { false, true }) {
            doTestChangeIndexOptionsViaAddDocument(preExisting, onNewSegment, from, to);
          }
        }
      }
    }
  }

  private void doTestChangeIndexOptionsViaAddDocument(boolean preExistingField, boolean onNewSegment, IndexOptions from, IndexOptions to) throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    if (preExistingField) {
      w.addDocument(Collections.singleton(new IntPoint("foo", 1)));
      if (onNewSegment) {
        DirectoryReader.open(w).close();
      }
    }
    FieldType ft1 = new FieldType(TextField.TYPE_STORED);
    ft1.setIndexOptions(from);
    w.addDocument(Collections.singleton(new Field("foo", "bar", ft1)));
    if (onNewSegment) {
      DirectoryReader.open(w).close();
    }
    FieldType ft2 = new FieldType(TextField.TYPE_STORED);
    ft2.setIndexOptions(to);
    if (from == IndexOptions.NONE || to == IndexOptions.NONE || from == to) {
      w.addDocument(Collections.singleton(new Field("foo", "bar", ft2))); // no exception
      w.forceMerge(1);
      try (LeafReader r = getOnlyLeafReader(DirectoryReader.open(w))) {
        IndexOptions expected = from == IndexOptions.NONE ? to : from;
        assertEquals(expected, r.getFieldInfos().fieldInfo("foo").getIndexOptions());
      }
    } else {
      IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
          () -> w.addDocument(Collections.singleton(new Field("foo", "bar", ft2))));
      assertEquals("cannot change field \"foo\" from index options=" + from +
          " to inconsistent index options=" + to, e.getMessage());
    }
    w.close();
    dir.close();
  }

  public void testChangeIndexOptionsViaAddIndexesCodecReader() throws IOException {
    for (IndexOptions from : IndexOptions.values()) {
      for (IndexOptions to : IndexOptions.values()) {
        doTestChangeIndexOptionsAddIndexesCodecReader(from, to);
      }
    }
  }

  private void doTestChangeIndexOptionsAddIndexesCodecReader(IndexOptions from, IndexOptions to) throws IOException {
    Directory dir1 = newDirectory();
    IndexWriter w1 = new IndexWriter(dir1, newIndexWriterConfig());
    FieldType ft1 = new FieldType(TextField.TYPE_STORED);
    ft1.setIndexOptions(from);
    w1.addDocument(Collections.singleton(new Field("foo", "bar", ft1)));

    Directory dir2 = newDirectory();
    IndexWriter w2 = new IndexWriter(dir2, newIndexWriterConfig());
    FieldType ft2 = new FieldType(TextField.TYPE_STORED);
    ft2.setIndexOptions(to);
    w2.addDocument(Collections.singleton(new Field("foo", "bar", ft2)));

    try (CodecReader cr = (CodecReader) getOnlyLeafReader(DirectoryReader.open(w2))) {
      if (from == IndexOptions.NONE || to == IndexOptions.NONE || from == to) {
        w1.addIndexes(cr); // no exception
        w1.forceMerge(1);
        try (LeafReader r = getOnlyLeafReader(DirectoryReader.open(w1))) {
          IndexOptions expected = from == IndexOptions.NONE ? to : from;
          assertEquals(expected, r.getFieldInfos().fieldInfo("foo").getIndexOptions());
        }
      } else {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> w1.addIndexes(cr));
        assertEquals("cannot change field \"foo\" from index options=" + from +
            " to inconsistent index options=" + to, e.getMessage());
      }
    }

    IOUtils.close(w1, w2, dir1, dir2);
  }

  public void testChangeIndexOptionsViaAddIndexesDirectory() throws IOException {
    for (IndexOptions from : IndexOptions.values()) {
      for (IndexOptions to : IndexOptions.values()) {
        doTestChangeIndexOptionsAddIndexesDirectory(from, to);
      }
    }
  }

  private void doTestChangeIndexOptionsAddIndexesDirectory(IndexOptions from, IndexOptions to) throws IOException {
    Directory dir1 = newDirectory();
    IndexWriter w1 = new IndexWriter(dir1, newIndexWriterConfig());
    FieldType ft1 = new FieldType(TextField.TYPE_STORED);
    ft1.setIndexOptions(from);
    w1.addDocument(Collections.singleton(new Field("foo", "bar", ft1)));

    Directory dir2 = newDirectory();
    IndexWriter w2 = new IndexWriter(dir2, newIndexWriterConfig());
    FieldType ft2 = new FieldType(TextField.TYPE_STORED);
    ft2.setIndexOptions(to);
    w2.addDocument(Collections.singleton(new Field("foo", "bar", ft2)));
    w2.close();

    if (from == IndexOptions.NONE || to == IndexOptions.NONE || from == to) {
      w1.addIndexes(dir2); // no exception
      w1.forceMerge(1);
      try (LeafReader r = getOnlyLeafReader(DirectoryReader.open(w1))) {
        IndexOptions expected = from == IndexOptions.NONE ? to : from;
        assertEquals(expected, r.getFieldInfos().fieldInfo("foo").getIndexOptions());
      }
    } else {
      IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
          () -> w1.addIndexes(dir2));
      assertEquals("cannot change field \"foo\" from index options=" + from +
          " to inconsistent index options=" + to, e.getMessage());
    }

    IOUtils.close(w1, dir1, dir2);
  }
}
