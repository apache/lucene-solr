package org.apache.lucene.codecs.blockterms;

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

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.lucene41ords.Lucene41WithOrds;
import org.apache.lucene.index.BasePostingsFormatTestCase;
import org.apache.lucene.util._TestUtil;

/**
 * Basic tests of a PF using FixedGap terms dictionary
 */
// TODO: we should add an instantiation for VarGap too to TestFramework, and a test in this package
// TODO: ensure both of these are also in rotation in RandomCodec
public class TestFixedGapPostingsFormat extends BasePostingsFormatTestCase {
  private final Codec codec = _TestUtil.alwaysPostingsFormat(new Lucene41WithOrds());

  @Override
  protected Codec getCodec() {
    return codec;
  }
}
