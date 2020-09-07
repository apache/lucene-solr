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

import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.util.TestUtil;

/**
 * Test {@link org.apache.lucene.codecs.Codec} to demonstrate how encryption keys can be provided to an
 * {@link org.apache.lucene.store.EncryptingDirectory} to encrypt/decrypt {@link PostingsFormat} or
 * {@link DocValuesFormat} files.
 * <p>If the keys do not depend on the segment info, see also {@link SimpleEncryptingDirectory} example.</p>
 * <p>Run tests with -Dtests.codec=Encrypting</p>
 */
public class EncryptingCodec extends FilterCodec {

  private final PostingsFormat postingsFormat = new EncryptingPostingsFormat();
  private final DocValuesFormat docValuesFormat = new EncryptingDocValuesFormat();

  public EncryptingCodec() {
    super("Encrypting", TestUtil.getDefaultCodec());
  }

  @Override
  public PostingsFormat postingsFormat() {
    return postingsFormat;
  }

  @Override
  public DocValuesFormat docValuesFormat() {
    return docValuesFormat;
  }
}
