package org.apache.lucene.queryParser.standard.config;

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

import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.document.DateTools;
import org.apache.lucene.document.DateTools.Resolution;
import org.apache.lucene.queryParser.core.config.AbstractQueryConfig;
import org.apache.lucene.queryParser.core.config.ConfigAttribute;
import org.apache.lucene.queryParser.standard.config.StandardQueryConfigHandler.ConfigurationKeys;
import org.apache.lucene.util.AttributeImpl;

/**
 * This attribute enables the user to define a default DateResolution per field.
 * it's used by {@link FieldDateResolutionFCListener#buildFieldConfig(org.apache.lucene.queryParser.core.config.FieldConfig)}
 *
 * @see FieldDateResolutionMapAttribute
 * 
 * @deprecated
 * 
 */
@Deprecated
public class FieldDateResolutionMapAttributeImpl extends AttributeImpl 
				implements FieldDateResolutionMapAttribute, ConfigAttribute {

  private static final long serialVersionUID = -2104763012523049527L;
  
  private AbstractQueryConfig config;

  { enableBackwards = false; }
  
  public FieldDateResolutionMapAttributeImpl() {
    // empty constructor
  }

  public void setFieldDateResolutionMap(Map<CharSequence, DateTools.Resolution> dateRes) {
    config.set(ConfigurationKeys.FIELD_DATE_RESOLUTION_MAP, dateRes);
  }
  
  public Map<CharSequence, Resolution> getFieldDateResolutionMap() {
    return config.get(ConfigurationKeys.FIELD_DATE_RESOLUTION_MAP);
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void copyTo(AttributeImpl target) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean equals(Object other) {

    if (other instanceof FieldDateResolutionMapAttributeImpl
        && ((FieldDateResolutionMapAttributeImpl) other)
            .getFieldDateResolutionMap().equals(getFieldDateResolutionMap())) {

      return true;

    }

    return false;

  }

  @Override
  public int hashCode() {
    final int prime = 97;
    Map<CharSequence, DateTools.Resolution> dateRes = getFieldDateResolutionMap();
    if (dateRes != null) 
      return dateRes.hashCode() * prime;
    else 
      return Float.valueOf(prime).hashCode();
  }

  @Override
  public String toString() {
    return "<fieldDateResolutionMapAttribute map=" + getFieldDateResolutionMap() + "/>";
  }
  
  public void setQueryConfigHandler(AbstractQueryConfig config) {
    this.config = config;
    
    if (!config.has(ConfigurationKeys.FIELD_DATE_RESOLUTION_MAP)) {
      setFieldDateResolutionMap(new HashMap<CharSequence, DateTools.Resolution>());
    }
    
  }

}
