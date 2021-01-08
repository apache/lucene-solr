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
package org.apache.lucene.search.matchhighlight;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import java.io.IOException;
import java.util.ArrayList;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;

/**
 * Utility class for building an ephemeral document index and running a block of code on its reader.
 */
class IndexBuilder {
  public static final String FLD_ID = "id";
  public static final String FLD_SORT_ORDER = "id_order";

  private final BiFunction<String, String, IndexableField> toField;
  private final ArrayList<Document> documents = new ArrayList<>();
  private int seq;

  class DocFields {
    final Document document;

    public DocFields(Document doc) {
      this.document = doc;
    }

    public void add(String field, String... values) {
      assert values.length > 0 : "At least one field value is required.";
      for (String value : values) {
        document.add(toField.apply(field, value));
      }
    }
  }

  IndexBuilder(BiFunction<String, String, IndexableField> valueToField) {
    this.toField = valueToField;
  }

  public IndexBuilder doc(String field, String... values) {
    return doc(
        fields -> {
          fields.add(field, values);
        });
  }

  public IndexBuilder doc(Consumer<DocFields> fields) {
    Document doc = new Document();
    doc.add(new NumericDocValuesField(FLD_SORT_ORDER, seq));
    doc.add(new StringField(FLD_ID, Integer.toString(seq++), Field.Store.YES));
    fields.accept(new DocFields(doc));
    documents.add(doc);
    return this;
  }

  public IndexBuilder build(Analyzer analyzer, IOUtils.IOConsumer<DirectoryReader> block)
      throws IOException {
    IndexWriterConfig config = new IndexWriterConfig(analyzer);
    config.setIndexSort(new Sort(new SortField(FLD_SORT_ORDER, SortField.Type.LONG)));
    try (Directory directory = new ByteBuffersDirectory()) {
      IndexWriter iw = new IndexWriter(directory, config);
      for (Document doc : documents) {
        iw.addDocument(doc);
      }
      if (RandomizedTest.randomBoolean()) {
        iw.commit();
      }
      iw.flush();

      try (DirectoryReader reader = DirectoryReader.open(iw)) {
        block.accept(reader);
      }
    }
    return this;
  }
}
