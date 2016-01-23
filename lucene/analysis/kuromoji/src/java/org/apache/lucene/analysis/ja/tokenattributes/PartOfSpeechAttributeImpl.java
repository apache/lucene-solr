package org.apache.lucene.analysis.ja.tokenattributes;

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

import org.apache.lucene.analysis.ja.Token;
import org.apache.lucene.analysis.ja.util.ToStringUtil;
import org.apache.lucene.util.AttributeImpl;
import org.apache.lucene.util.AttributeReflector;

/**
 * Attribute for {@link Token#getPartOfSpeech()}.
 */
public class PartOfSpeechAttributeImpl extends AttributeImpl implements PartOfSpeechAttribute, Cloneable {
  private Token token;
  
  @Override
  public String getPartOfSpeech() {
    return token == null ? null : token.getPartOfSpeech();
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
  public void copyTo(AttributeImpl target) {
    PartOfSpeechAttribute t = (PartOfSpeechAttribute) target;
    t.setToken(token);
  }
  
  @Override
  public void reflectWith(AttributeReflector reflector) {
    String partOfSpeech = getPartOfSpeech();
    String partOfSpeechEN = partOfSpeech == null ? null : ToStringUtil.getPOSTranslation(partOfSpeech);
    reflector.reflect(PartOfSpeechAttribute.class, "partOfSpeech", partOfSpeech);
    reflector.reflect(PartOfSpeechAttribute.class, "partOfSpeech (en)", partOfSpeechEN);
  }
}
