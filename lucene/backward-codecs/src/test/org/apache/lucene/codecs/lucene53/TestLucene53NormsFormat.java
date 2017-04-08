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
package org.apache.lucene.codecs.lucene53;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.lucene62.Lucene62RWCodec;
import org.apache.lucene.index.BaseNormsFormatTestCase;
import org.apache.lucene.util.Version;

/**
 * Tests Lucene53NormsFormat
 */
public class TestLucene53NormsFormat extends BaseNormsFormatTestCase {
  private final Codec codec = new Lucene62RWCodec();

  @Override
  protected int getCreatedVersionMajor() {
    return Version.LUCENE_6_2_0.major;
  }

  @Override
  protected Codec getCodec() {
    return codec;
  }

  @Override
  protected boolean codecSupportsSparsity() {
    return false;
  }
}