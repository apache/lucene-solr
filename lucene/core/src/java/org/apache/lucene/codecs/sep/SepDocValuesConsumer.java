package org.apache.lucene.codecs.sep;

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

import org.apache.lucene.codecs.lucene40.values.DocValuesWriterBase;
import org.apache.lucene.index.PerDocWriteState;
import org.apache.lucene.store.Directory;

/**
 * Implementation of PerDocConsumer that uses separate files.
 * @lucene.experimental
 */

public class SepDocValuesConsumer extends DocValuesWriterBase {
  private final Directory directory;

  public SepDocValuesConsumer(PerDocWriteState state) {
    super(state);
    this.directory = state.directory;
  }
  
  @Override
  protected Directory getDirectory() {
    return directory;
  }

  @Override
  public void abort() {
    // We don't have to remove files here: IndexFileDeleter
    // will do so
  }
}
