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

package org.apache.lucene.codecs.encrypting;

import java.io.IOException;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.SegmentEncryptingDirectory;
import org.apache.lucene.util.TestUtil;

public class EncryptingDocValuesFormat extends DocValuesFormat {

  private final DocValuesFormat delegate = TestUtil.getDefaultDocValuesFormat();

  public EncryptingDocValuesFormat() {
    super("Encrypting");
  }

  @Override
  public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    Directory encryptingDirectory = new SegmentEncryptingDirectory(state.directory, EncryptingFormatUtil::getEncryptionKey, state.segmentInfo);
    SegmentWriteState encryptingState = new SegmentWriteState(state.infoStream, encryptingDirectory, state.segmentInfo, state.fieldInfos, state.segUpdates, state.context, state.segmentSuffix);
    return delegate.fieldsConsumer(encryptingState);
  }

  @Override
  public DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
    Directory encryptingDirectory = new SegmentEncryptingDirectory(state.directory, EncryptingFormatUtil::getEncryptionKey, state.segmentInfo);
    SegmentReadState decryptingState = new SegmentReadState(encryptingDirectory, state.segmentInfo, state.fieldInfos, state.context, state.segmentSuffix);
    return delegate.fieldsProducer(decryptingState);
  }
}
