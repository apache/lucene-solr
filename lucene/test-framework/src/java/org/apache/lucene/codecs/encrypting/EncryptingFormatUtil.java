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
import java.util.Random;

import org.apache.lucene.index.SegmentInfo;

/**
 * Methods for encrypting formats.
 */
class EncryptingFormatUtil {

  static byte[] getEncryptionKey(SegmentInfo segmentInfo, String fileName) {
    // AES key length must be either 16, 24 or 32 bytes.
    byte[] key = new byte[32];
    // The key must be the same each time the same SegmentInfo ID is passed.
    // In real production this would be replaced by a call to a secure key vault in which keys are identified by the
    // segment ID.
    new Random(Arrays.hashCode(segmentInfo.getId())).nextBytes(key);
    return key;
  }
}
