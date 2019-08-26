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
package org.apache.lucene.search.join;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public class TestCheckJoinIndex extends LuceneTestCase {

  public void testNoParent() throws IOException {
    final Directory dir = newDirectory();
    final RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    final int numDocs = TestUtil.nextInt(random(), 1, 3);
    for (int i = 0; i < numDocs; ++i) {
      w.addDocument(new Document());
    }
    final IndexReader reader = w.getReader();
    w.close();
    BitSetProducer parentsFilter = new QueryBitSetProducer(new MatchNoDocsQuery());
    try {
      expectThrows(IllegalStateException.class, () -> CheckJoinIndex.check(reader, parentsFilter));
    } finally {
      reader.close();
      dir.close();
    }
  }

  public void testOrphans() throws IOException {
    final Directory dir = newDirectory();
    final RandomIndexWriter w = new RandomIndexWriter(random(), dir);

    {
      // Add a first valid block
      List<Document> block = new ArrayList<>();
      final int numChildren = TestUtil.nextInt(random(), 0, 3);
      for (int i = 0; i < numChildren; ++i) {
        block.add(new Document());
      }
      Document parent = new Document();
      parent.add(new StringField("parent", "true", Store.NO));
      block.add(parent);
      w.addDocuments(block);
    }

    {
      // Then a block with no parent
      List<Document> block = new ArrayList<>();
      final int numChildren = TestUtil.nextInt(random(), 1, 3);
      for (int i = 0; i < numChildren; ++i) {
        block.add(new Document());
      }
      w.addDocuments(block);
    }

    final IndexReader reader = w.getReader();
    w.close();
    BitSetProducer parentsFilter = new QueryBitSetProducer(new TermQuery(new Term("parent", "true")));
    try {
      expectThrows(IllegalStateException.class, () -> CheckJoinIndex.check(reader, parentsFilter));
    } finally {
      reader.close();
      dir.close();
    }
  }

  public void testInconsistentDeletes() throws IOException {
    final Directory dir = newDirectory();
    final IndexWriterConfig iwc = newIndexWriterConfig();
    iwc.setMergePolicy(NoMergePolicy.INSTANCE); // so that deletions don't trigger merges
    final RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);

    List<Document> block = new ArrayList<>();
    final int numChildren = TestUtil.nextInt(random(), 1, 3);
    for (int i = 0; i < numChildren; ++i) {
      Document doc = new Document();
      doc.add(new StringField("child", Integer.toString(i), Store.NO));
      block.add(doc);
    }
    Document parent = new Document();
    parent.add(new StringField("parent", "true", Store.NO));
    block.add(parent);
    w.addDocuments(block);

    if (random().nextBoolean()) {
      w.deleteDocuments(new Term("parent", "true"));
    } else {
      // delete any of the children
      w.deleteDocuments(new Term("child", Integer.toString(random().nextInt(numChildren))));
    }

    final IndexReader reader = w.getReader();
    w.close();

    BitSetProducer parentsFilter = new QueryBitSetProducer(new TermQuery(new Term("parent", "true")));
    try {
      expectThrows(IllegalStateException.class, () -> CheckJoinIndex.check(reader, parentsFilter));
    } finally {
      reader.close();
      dir.close();
    }
  }

}
