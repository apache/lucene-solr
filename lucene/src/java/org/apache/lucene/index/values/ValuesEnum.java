package org.apache.lucene.index.values;

/**
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
import java.io.IOException;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Attribute;
import org.apache.lucene.util.AttributeSource;

public abstract class ValuesEnum extends DocIdSetIterator{
  private AttributeSource source;
  protected final ValuesAttribute attr;

 
  protected ValuesEnum(Values enumType) {
     this(null, enumType);
  }

  protected ValuesEnum(AttributeSource source, Values enumType) {
    this.source = source;
    boolean setType = !hasAttribute(ValuesAttribute.class);
    attr = addAttribute(ValuesAttribute.class);
    if (setType)
      attr.setType(enumType);
  }

  public AttributeSource attributes() {
    if (source == null)
      source = new AttributeSource();
    return source;
  }

  public <T extends Attribute> T addAttribute(Class<T> attr) {
    return attributes().addAttribute(attr);
  }

  public <T extends Attribute> T getAttribute(Class<T> attr) {
    return attributes().getAttribute(attr);
  }

  public <T extends Attribute> boolean hasAttribute(Class<T> attr) {
    return attributes().hasAttribute(attr);
  }

  public abstract void close() throws IOException;

}
