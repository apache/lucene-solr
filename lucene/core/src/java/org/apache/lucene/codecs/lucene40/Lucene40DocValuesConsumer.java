package org.apache.lucene.codecs.lucene40;

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

import java.io.IOException;

import org.apache.lucene.codecs.lucene40.values.DocValuesWriterBase;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.PerDocWriteState;
import org.apache.lucene.store.CompoundFileDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;

/**
 * Lucene 4.0 PerDocConsumer implementation that uses compound file.
 * 
 * @see Lucene40DocValuesFormat
 * @lucene.experimental
 */
public class Lucene40DocValuesConsumer extends DocValuesWriterBase {
  private final Directory mainDirectory;
  private Directory directory;
  private final String segmentSuffix;

  /** Segment suffix used when writing doc values index files. */
  public final static String DOC_VALUES_SEGMENT_SUFFIX = "dv";

  /** Sole constructor. */
  public Lucene40DocValuesConsumer(PerDocWriteState state, String segmentSuffix) {
    super(state);
    this.segmentSuffix = segmentSuffix;
    mainDirectory = state.directory;
    //TODO maybe we should enable a global CFS that all codecs can pull on demand to further reduce the number of files?
  }
  
  @Override
  protected Directory getDirectory() throws IOException {
    // lazy init
    if (directory == null) {
      directory = new CompoundFileDirectory(mainDirectory,
                                            IndexFileNames.segmentFileName(segmentName, segmentSuffix,
                                                                           IndexFileNames.COMPOUND_FILE_EXTENSION), context, true);
    }
    return directory;
  }

  @Override
  public void close() throws IOException {
    if (directory != null) {
      directory.close();
    }
  }

  @Override
  public void abort() {
    try {
      close();
    } catch (Throwable t) {
      // ignore
    } finally {
      // TODO: why the inconsistency here? we do this, but not SimpleText (which says IFD
      // will do it).
      // TODO: check that IFD really does this always, even if codec abort() throws a 
      // RuntimeException (e.g. ThreadInterruptedException)
      IOUtils.deleteFilesIgnoringExceptions(mainDirectory, IndexFileNames.segmentFileName(
        segmentName, segmentSuffix, IndexFileNames.COMPOUND_FILE_EXTENSION),
        IndexFileNames.segmentFileName(segmentName, segmentSuffix,
            IndexFileNames.COMPOUND_FILE_ENTRIES_EXTENSION));
    }
  }
}
