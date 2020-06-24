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
import java.util.Arrays;
import java.util.Random;

import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.CipherPool;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.EncryptingDirectory;
import org.apache.lucene.util.TestUtil;

public class EncryptingPostingsFormat extends PostingsFormat {

  private final PostingsFormat delegate = TestUtil.getDefaultPostingsFormat();
  private final CipherPool cipherPool = new CipherPool();

  public EncryptingPostingsFormat() {
    super("Encrypting");
  }
  
  @Override
  public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    Directory encryptingDirectory = new EncryptingDirectory(state.directory, this::getEncryptionKey, cipherPool, state.segmentInfo);
    SegmentWriteState encryptingState = new SegmentWriteState(state.infoStream, encryptingDirectory, state.segmentInfo, state.fieldInfos, state.segUpdates, state.context, state.segmentSuffix);
    return delegate.fieldsConsumer(encryptingState);
  }

  @Override
  public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
    Directory encryptingDirectory = new EncryptingDirectory(state.directory, this::getEncryptionKey, cipherPool, state.segmentInfo);
    SegmentReadState decryptingState = new SegmentReadState(encryptingDirectory, state.segmentInfo, state.fieldInfos, state.context, state.segmentSuffix);
    return delegate.fieldsProducer(decryptingState);
  }

  private byte[] getEncryptionKey(SegmentInfo segmentInfo, String fileName) {
    byte[] key = new byte[32];
    new Random(Arrays.hashCode(segmentInfo.getId())).nextBytes(key);
    return key;
  }
}
