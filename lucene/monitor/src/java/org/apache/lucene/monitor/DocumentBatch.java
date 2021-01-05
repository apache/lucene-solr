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

package org.apache.lucene.monitor;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.function.Supplier;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;

abstract class DocumentBatch implements Closeable, Supplier<LeafReader> {

  /**
   * Create a DocumentBatch containing a single InputDocument
   *
   * @param doc the document to add
   * @return the batch containing the input document
   */
  public static DocumentBatch of(Analyzer analyzer, Document doc) {
    return new SingletonDocumentBatch(analyzer, doc);
  }

  /**
   * Create a DocumentBatch containing a set of InputDocuments
   *
   * @param docs Collection of documents to add. There must be at least one document in the
   *     collection.
   * @return the batch containing the input documents
   */
  public static DocumentBatch of(Analyzer analyzer, Document... docs) {
    if (docs.length == 0) {
      throw new IllegalArgumentException("A DocumentBatch must contain at least one document");
    } else if (docs.length == 1) {
      return new SingletonDocumentBatch(analyzer, docs[0]);
    } else {
      return new MultiDocumentBatch(analyzer, docs);
    }
  }

  // Implementation of DocumentBatch for collections of documents
  private static class MultiDocumentBatch extends DocumentBatch {

    private final Directory directory = new ByteBuffersDirectory();
    private final LeafReader reader;

    MultiDocumentBatch(Analyzer analyzer, Document... docs) {
      assert (docs.length > 0);
      IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
      try (IndexWriter writer = new IndexWriter(directory, iwc)) {
        this.reader = build(writer, docs);
      } catch (IOException e) {
        throw new RuntimeException(e); // This is a RAMDirectory, so should never happen...
      }
    }

    @Override
    public LeafReader get() {
      return reader;
    }

    private LeafReader build(IndexWriter writer, Document... docs) throws IOException {
      writer.addDocuments(Arrays.asList(docs));
      writer.commit();
      writer.forceMerge(1);
      LeafReader reader = DirectoryReader.open(directory).leaves().get(0).reader();
      assert reader != null;
      return reader;
    }

    @Override
    public void close() throws IOException {
      IOUtils.close(reader, directory);
    }
  }

  // Specialized class for batches containing a single object - MemoryIndex benchmarks as
  // better performing than RAMDirectory for this case
  private static class SingletonDocumentBatch extends DocumentBatch {

    private final LeafReader reader;

    private SingletonDocumentBatch(Analyzer analyzer, Document doc) {
      MemoryIndex memoryindex = new MemoryIndex(true, true);
      for (IndexableField field : doc) {
        memoryindex.addField(field, analyzer);
      }
      memoryindex.freeze();
      reader = (LeafReader) memoryindex.createSearcher().getIndexReader();
    }

    @Override
    public LeafReader get() {
      return reader;
    }

    @Override
    public void close() throws IOException {
      reader.close();
    }
  }
}
