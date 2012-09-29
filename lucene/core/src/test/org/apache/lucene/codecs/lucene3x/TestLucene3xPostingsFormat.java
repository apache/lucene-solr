package org.apache.lucene.codecs.lucene3x;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.BasePostingsFormatTestCase;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.BeforeClass;

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

/**
 * Tests Lucene3x postings format
 */
public class TestLucene3xPostingsFormat extends BasePostingsFormatTestCase {
  private final Codec codec = new PreFlexRWCodec();

  /** we will manually instantiate preflex-rw here */
  @BeforeClass
  public static void beforeClass3xPostingsFormat() {
    LuceneTestCase.PREFLEX_IMPERSONATION_IS_ACTIVE = true;
  }

  @Override
  protected Codec getCodec() {
    return codec;
  }
}
