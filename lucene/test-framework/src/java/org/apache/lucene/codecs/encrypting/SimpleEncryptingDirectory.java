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

import java.util.Arrays;

import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.FileEncryptingDirectory;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.SingleInstanceLockFactory;

/**
 * Test {@link FileEncryptingDirectory} to demonstrate how encryption keys can be provided.
 * <p>This is obviously too simple. A real {@link FileEncryptingDirectory} could get the key from a property or a secured
 * database.</p>
 * <p>If the key depends on the segment info, see also {@link EncryptingCodec} example.</p>
 * <p>Run tests with -Dtests.directory=org.apache.lucene.codecs.encrypting.SimpleEncryptingDirectory</p>
 */
public class SimpleEncryptingDirectory extends FileEncryptingDirectory {

  private static final byte[] KEY;

  static {
    // AES key length must be either 16, 24 or 32 bytes.
    KEY = new byte[32];
    Arrays.fill(KEY, (byte) 31);
  }

  public SimpleEncryptingDirectory() {
    this(new SingleInstanceLockFactory());
  }

  // This constructor is called by LuceneTestCase.newDirectoryImpl() with special LockFactory.
  // It is required for some tests to pass.
  public SimpleEncryptingDirectory(LockFactory lockFactory) {
    super(new ByteBuffersDirectory(lockFactory), SimpleEncryptingDirectory::getEncryptionKey);
  }

  private static byte[] getEncryptionKey(String fileName) {
    return KEY;
  }
}
