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

import org.apache.lucene.analysis.ko.POS.Type;
import org.apache.lucene.analysis.ko.POS.Tag;
import org.apache.lucene.analysis.ko.Token;
import org.apache.lucene.analysis.ko.dict.Dictionary.Morpheme;
import org.apache.lucene.util.AttributeImpl;
import org.apache.lucene.util.AttributeReflector;

/**
 * Part of Speech attributes for Korean.
 * @lucene.experimental
 */
public class PartOfSpeechAttributeImpl extends AttributeImpl implements PartOfSpeechAttribute, Cloneable {
  private Token token;

  @Override
  public Type getPOSType() {
    return token == null ? null : token.getPOSType();
  }

  @Override
  public Tag getLeftPOS() {
    return token == null ? null : token.getLeftPOS();
  }

  @Override
  public Tag getRightPOS() {
    return token == null ? null : token.getRightPOS();
  }

  @Override
  public Morpheme[] getMorphemes() {
    return token == null ? null : token.getMorphemes();
  }

  @Override
  public void setToken(Token token) {
    this.token = token;
  }

  @Override
  public void clear() {
    token = null;
  }

  @Override
  public void reflectWith(AttributeReflector reflector) {
    reflector.reflect(PartOfSpeechAttribute.class, "posType", getPOSType().name());
    Tag leftPOS = getLeftPOS();
    reflector.reflect(PartOfSpeechAttribute.class, "leftPOS", leftPOS.name() + "(" + leftPOS.description() + ")");
    Tag rightPOS = getRightPOS();
    reflector.reflect(PartOfSpeechAttribute.class, "rightPOS", rightPOS.name() + "(" + rightPOS.description() + ")");
    reflector.reflect(PartOfSpeechAttribute.class, "morphemes", displayMorphemes(getMorphemes()));
  }

  private String displayMorphemes(Morpheme[] morphemes) {
    if (morphemes == null) {
      return null;
    }
    StringBuilder builder = new StringBuilder();
    for (Morpheme morpheme : morphemes) {
      if (builder.length() > 0) {
        builder.append("+");
      }
      builder.append(morpheme.surfaceForm + "/" + morpheme.posTag.name() + "(" + morpheme.posTag.description() + ")");
    }
    return builder.toString();
  }

  @Override
  public void copyTo(AttributeImpl target) {
    PartOfSpeechAttribute t = (PartOfSpeechAttribute) target;
    t.setToken(token);
  }
}
