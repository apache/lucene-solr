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

package org.apache.lucene.codecs.uniformsplit;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.BasePostingsFormatTestCase;
import org.apache.lucene.util.TestUtil;
import org.junit.After;
import org.junit.Before;

/**
 *  Tests {@link UniformSplitPostingsFormat} with block encoding using ROT13 cypher.
 */
public class TestUniformSplitPostingFormat extends BasePostingsFormatTestCase {

  private final Codec codec = TestUtil.alwaysPostingsFormat(new UniformSplitRot13PostingsFormat());

  @Override
  protected Codec getCodec() {
    return codec;
  }

  @Before
  public void initialize() {
    UniformSplitRot13PostingsFormat.blockEncodingCalled = false;
  }

  @After
  public void checkEncodingCalled() {
    assertTrue(UniformSplitRot13PostingsFormat.blockEncodingCalled);
    // We do not check that the decoding has been called because some tests
    // break the flow with mocked exceptions. Anyway, if encoding is called
    // then the appropriate decoding must be run otherwise tests don't pass.
  }
}
