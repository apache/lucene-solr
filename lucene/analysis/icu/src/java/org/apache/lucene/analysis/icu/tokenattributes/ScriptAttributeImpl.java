package org.apache.lucene.analysis.icu.tokenattributes;

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

import org.apache.lucene.util.AttributeImpl;
import org.apache.lucene.util.AttributeReflector;

import com.ibm.icu.lang.UScript;

/**
 * Implementation of {@link ScriptAttribute} that stores the script
 * as an integer.
 * @lucene.experimental
 */
public class ScriptAttributeImpl extends AttributeImpl implements ScriptAttribute, Cloneable {
  private int code = UScript.COMMON;
  
  /** Initializes this attribute with <code>UScript.COMMON</code> */
  public ScriptAttributeImpl() {}
  
  @Override
  public int getCode() {
    return code;
  }
  
  @Override
  public void setCode(int code) {
    this.code = code;
  }

  @Override
  public String getName() {
    return UScript.getName(code);
  }

  @Override
  public String getShortName() {
    return UScript.getShortName(code);
  }
  
  @Override
  public void clear() {
    code = UScript.COMMON;
  }

  @Override
  public void copyTo(AttributeImpl target) {
    ScriptAttribute t = (ScriptAttribute) target;
    t.setCode(code);
  }
  
  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    
    if (other instanceof ScriptAttributeImpl) {
      return ((ScriptAttributeImpl) other).code == code;
    }
    
    return false;
  }

  @Override
  public int hashCode() {
    return code;
  }

  @Override
  public void reflectWith(AttributeReflector reflector) {
    reflector.reflect(ScriptAttribute.class, "script", getName());
  }
}
