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
package org.apache.lucene.codecs.perfield;


import java.util.Collections;
import java.util.Random;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.BasePostingsFormatTestCase;
import org.apache.lucene.index.RandomCodec;

/**
 * Basic tests of PerFieldPostingsFormat
 */
public class TestPerFieldPostingsFormat extends BasePostingsFormatTestCase {

  @Override
  protected Codec getCodec() {
    return new RandomCodec(new Random(random().nextLong()), Collections.<String>emptySet());
  }

  @Override
  public void testMergeStability() throws Exception {
    assumeTrue("The MockRandom PF randomizes content on the fly, so we can't check it", false);
  }

  @Override
  public void testPostingsEnumReuse() throws Exception {
    assumeTrue("The MockRandom PF randomizes content on the fly, so we can't check it", false);
  }
}
