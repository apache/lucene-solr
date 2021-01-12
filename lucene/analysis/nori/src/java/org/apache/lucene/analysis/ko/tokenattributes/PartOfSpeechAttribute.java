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
package org.apache.lucene.analysis.ko.tokenattributes;

import org.apache.lucene.analysis.ko.POS.Tag;
import org.apache.lucene.analysis.ko.POS.Type;
import org.apache.lucene.analysis.ko.Token;
import org.apache.lucene.analysis.ko.dict.Dictionary.Morpheme;
import org.apache.lucene.util.Attribute;

/**
 * Part of Speech attributes for Korean.
 *
 * @lucene.experimental
 */
public interface PartOfSpeechAttribute extends Attribute {
  /** Get the {@link Type} of the token. */
  Type getPOSType();

  /** Get the left part of speech of the token. */
  Tag getLeftPOS();

  /** Get the right part of speech of the token. */
  Tag getRightPOS();

  /** Get the {@link Morpheme} decomposition of the token. */
  Morpheme[] getMorphemes();

  /** Set the current token. */
  void setToken(Token token);
}
