package org.apache.lucene.index;

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

import java.util.EnumSet;
import java.util.Set;

import org.apache.lucene.codecs.Codec;

/**
 * Tests with the default randomized codec. Not really redundant with
 * other specific instantiations since we want to test some test-only impls
 * like Asserting, as well as make it easy to write a codec and pass -Dtests.codec
 */
public class TestTermVectorsFormat extends BaseTermVectorsFormatTestCase {

  @Override
  protected Codec getCodec() {
    return Codec.getDefault();
  }

  @Override
  protected Set<Options> validOptions() {
    if (PREFLEX_IMPERSONATION_IS_ACTIVE) {
      // payloads are not supported on vectors in 3.x indexes
      return EnumSet.range(Options.NONE, Options.POSITIONS_AND_OFFSETS);
    } else {
      return super.validOptions();
    }
  }
}
