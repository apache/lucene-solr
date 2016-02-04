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
package org.apache.lucene.analysis.ja.tokenattributes;


import org.apache.lucene.analysis.ja.Token;
import org.apache.lucene.analysis.ja.util.ToStringUtil;
import org.apache.lucene.util.AttributeImpl;
import org.apache.lucene.util.AttributeReflector;

/**
 * Attribute for Kuromoji reading data
 */
public class ReadingAttributeImpl extends AttributeImpl implements ReadingAttribute, Cloneable {
  private Token token;
  
  @Override
  public String getReading() {
    return token == null ? null : token.getReading();
  }
  
  @Override
  public String getPronunciation() {
    return token == null ? null : token.getPronunciation();
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
    ReadingAttribute t = (ReadingAttribute) target;
    t.setToken(token);
  }
  
  @Override
  public void reflectWith(AttributeReflector reflector) {
    String reading = getReading();
    String readingEN = reading == null ? null : ToStringUtil.getRomanization(reading);
    String pronunciation = getPronunciation();
    String pronunciationEN = pronunciation == null ? null : ToStringUtil.getRomanization(pronunciation);
    reflector.reflect(ReadingAttribute.class, "reading", reading);
    reflector.reflect(ReadingAttribute.class, "reading (en)", readingEN);
    reflector.reflect(ReadingAttribute.class, "pronunciation", pronunciation);
    reflector.reflect(ReadingAttribute.class, "pronunciation (en)", pronunciationEN);
  }
}
