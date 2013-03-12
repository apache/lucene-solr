package org.apache.lucene.analysis.in;

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

import java.io.Reader;

import org.apache.lucene.analysis.util.CharTokenizer;
import org.apache.lucene.analysis.standard.StandardTokenizer; // javadocs
import org.apache.lucene.util.Version;

/**
 * Simple Tokenizer for text in Indian Languages.
 * @deprecated (3.6) Use {@link StandardTokenizer} instead.
 */
@Deprecated
public final class IndicTokenizer extends CharTokenizer {
 
  public IndicTokenizer(Version matchVersion, AttributeFactory factory, Reader input) {
    super(matchVersion, factory, input);
  }

  public IndicTokenizer(Version matchVersion, Reader input) {
    super(matchVersion, input);
  }

  @Override
  protected boolean isTokenChar(int c) {
    return Character.isLetter(c)
    || Character.getType(c) == Character.NON_SPACING_MARK
    || Character.getType(c) == Character.FORMAT
    || Character.getType(c) == Character.COMBINING_SPACING_MARK;
  }
}
