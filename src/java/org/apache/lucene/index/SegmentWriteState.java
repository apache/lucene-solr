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

import java.util.HashSet;
import java.util.Collection;

import org.apache.lucene.store.Directory;

class SegmentWriteState {
  DocumentsWriter docWriter;
  Directory directory;
  String segmentName;
  String docStoreSegmentName;
  int numDocs;
  int termIndexInterval;
  int numDocsInStore;
  Collection flushedFiles;

  public SegmentWriteState(DocumentsWriter docWriter, Directory directory, String segmentName, String docStoreSegmentName, int numDocs,
                           int numDocsInStore, int termIndexInterval) {
    this.docWriter = docWriter;
    this.directory = directory;
    this.segmentName = segmentName;
    this.docStoreSegmentName = docStoreSegmentName;
    this.numDocs = numDocs;
    this.numDocsInStore = numDocsInStore;
    this.termIndexInterval = termIndexInterval;
    flushedFiles = new HashSet();
  }

  public String segmentFileName(String ext) {
    return segmentName + "." + ext;
  }
}
