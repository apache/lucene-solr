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
import org.apache.lucene.store.CipherPool;
import org.apache.lucene.store.EncryptingDirectory;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.SingleInstanceLockFactory;

public class SimpleEncryptingDirectory extends EncryptingDirectory {

  private static final byte[] KEY;
  static {
    KEY = new byte[32];
    Arrays.fill(KEY, (byte) 31);
  }

  public SimpleEncryptingDirectory() {
    this(new SingleInstanceLockFactory());
  }

  public SimpleEncryptingDirectory(LockFactory lockFactory) {
    super(new ByteBuffersDirectory(lockFactory), SimpleEncryptingDirectory::getEncryptionKey, new CipherPool());
  }

  private static byte[] getEncryptionKey(String fileName) {
    return KEY;
  }
}
