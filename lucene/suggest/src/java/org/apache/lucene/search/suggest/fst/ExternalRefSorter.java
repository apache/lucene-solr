package org.apache.lucene.search.suggest.fst;

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

import java.io.Closeable;
import java.io.IOException;
import java.util.Comparator;

import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.BytesRefIterator;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.OfflineSorter;

/**
 * Builds and iterates over sequences stored on disk.
 * @lucene.experimental
 * @lucene.internal
 */
public class ExternalRefSorter implements BytesRefSorter, Closeable {
  private final OfflineSorter sorter;
  private OfflineSorter.ByteSequencesWriter writer;
  private IndexOutput input;
  private String sortedFileName;
  
  /**
   * Will buffer all sequences to a temporary file and then sort (all on-disk).
   */
  public ExternalRefSorter(OfflineSorter sorter) throws IOException {
    this.sorter = sorter;
    this.input = sorter.getDirectory().createTempOutput(sorter.getTempFileNamePrefix(), "RefSorterRaw", IOContext.DEFAULT);
    this.writer = new OfflineSorter.ByteSequencesWriter(this.input);
  }
  
  @Override
  public void add(BytesRef utf8) throws IOException {
    if (writer == null) {
      throw new IllegalStateException();
    }
    writer.write(utf8);
  }
  
  @Override
  public BytesRefIterator iterator() throws IOException {
    if (sortedFileName == null) {
      closeWriter();
      
      boolean success = false;
      try {
        sortedFileName = sorter.sort(input.getName());
        success = true;
      } finally {
        if (success) {
          sorter.getDirectory().deleteFile(input.getName());
        } else {
          IOUtils.deleteFilesIgnoringExceptions(sorter.getDirectory(), input.getName());
        }
      }
      
      input = null;
    }
    
    return new ByteSequenceIterator(new OfflineSorter.ByteSequencesReader(sorter.getDirectory().openInput(sortedFileName, IOContext.READONCE)));
  }
  
  private void closeWriter() throws IOException {
    if (writer != null) {
      writer.close();
      writer = null;
    }
  }
  
  /**
   * Removes any written temporary files.
   */
  @Override
  public void close() throws IOException {
    try {
      closeWriter();
    } finally {
      IOUtils.deleteFilesIgnoringExceptions(sorter.getDirectory(),
                                            input == null ? null : input.getName(),
                                            sortedFileName);
    }
  }
  
  /**
   * Iterate over byte refs in a file.
   */
  class ByteSequenceIterator implements BytesRefIterator {
    private final OfflineSorter.ByteSequencesReader reader;
    private BytesRefBuilder scratch = new BytesRefBuilder();
    
    public ByteSequenceIterator(OfflineSorter.ByteSequencesReader reader) {
      this.reader = reader;
    }
    
    @Override
    public BytesRef next() throws IOException {
      if (scratch == null) {
        return null;
      }
      boolean success = false;
      try {
        if (reader.read(scratch) == false) {
          IOUtils.close(reader);
          scratch = null;
        }
        success = true;
        if (scratch == null) {
          return null;
        }
        return scratch.get();
      } finally {
        if (!success) {
          IOUtils.closeWhileHandlingException(reader);
        }
      }
    }
  }

  @Override
  public Comparator<BytesRef> getComparator() {
    return sorter.getComparator();
  }
}
