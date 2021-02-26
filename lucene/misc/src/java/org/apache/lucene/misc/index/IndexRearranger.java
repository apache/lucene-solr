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

package org.apache.lucene.misc.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterCodecReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.NamedThreadFactory;

/**
 * Copy and rearrange index according to document selectors, from input dir to output dir. Length of
 * documentSelectors determines how many segments there will be
 *
 * <p>TODO: another possible (faster) approach to do this is to manipulate FlushPolicy and
 * MergePolicy at indexing time to create small desired segments first and merge them accordingly
 * for details please see: https://markmail.org/message/lbtdntclpnocmfuf
 */
public class IndexRearranger {
  protected final Directory input, output;
  protected final IndexWriterConfig config;
  protected final List<DocumentSelector> documentSelectors;

  public IndexRearranger(
      Directory input,
      Directory output,
      IndexWriterConfig config,
      List<DocumentSelector> documentSelectors) {
    this.input = input;
    this.output = output;
    this.config = config;
    this.documentSelectors = documentSelectors;
  }

  public void execute() throws Exception {
    config.setMergePolicy(
        NoMergePolicy.INSTANCE); // do not merge since one addIndexes call create one segment
    try (IndexWriter writer = new IndexWriter(output, config);
        IndexReader reader = DirectoryReader.open(input)) {
      ExecutorService executor =
          Executors.newFixedThreadPool(
              Math.min(Runtime.getRuntime().availableProcessors(), documentSelectors.size()),
              new NamedThreadFactory("rearranger"));
      ArrayList<Future<Void>> futures = new ArrayList<>();
      for (DocumentSelector record : documentSelectors) {
        Callable<Void> addSegment =
            () -> {
              addOneSegment(writer, reader, record);
              return null;
            };
        futures.add(executor.submit(addSegment));
      }
      for (Future<Void> future : futures) {
        future.get();
      }
      executor.shutdown();
    }
  }

  private static void addOneSegment(
      IndexWriter writer, IndexReader reader, DocumentSelector selector) throws IOException {
    CodecReader[] readers = new CodecReader[reader.leaves().size()];
    for (LeafReaderContext context : reader.leaves()) {
      readers[context.ord] =
          new DocSelectorFilteredCodecReader((CodecReader) context.reader(), selector);
    }
    writer.addIndexes(readers);
  }

  private static class DocSelectorFilteredCodecReader extends FilterCodecReader {

    BitSet filteredLiveDocs;
    int numDocs;

    public DocSelectorFilteredCodecReader(CodecReader in, DocumentSelector selector)
        throws IOException {
      super(in);
      filteredLiveDocs = selector.getFilteredLiveDocs(in);
      numDocs = filteredLiveDocs.cardinality();
    }

    @Override
    public int numDocs() {
      return numDocs;
    }

    @Override
    public Bits getLiveDocs() {
      return filteredLiveDocs;
    }

    @Override
    public CacheHelper getCoreCacheHelper() {
      return in.getCoreCacheHelper();
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
      return null;
    }
  }

  /** Select document within a CodecReader */
  public interface DocumentSelector {
    BitSet getFilteredLiveDocs(CodecReader reader) throws IOException;
  }
}
