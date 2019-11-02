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

package org.apache.solr.util;

import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.AnnotationIntrospector;
import com.fasterxml.jackson.databind.PropertyName;
import com.fasterxml.jackson.databind.introspect.Annotated;
import com.fasterxml.jackson.databind.introspect.AnnotatedField;
import com.fasterxml.jackson.databind.introspect.AnnotatedMember;
import com.fasterxml.jackson.databind.introspect.AnnotatedMethod;
import com.fasterxml.jackson.databind.util.BeanUtil;
import org.apache.solr.common.annotation.Property;
import org.apache.solr.search.SolrCache;
//this class provides a maping between jackson's JsonProperty Annotation to Solr's Property annotation
// see SOLR-13841 for more details
public class SolrPropertyAnnotationInspector extends AnnotationIntrospector {
  public static final SolrPropertyAnnotationInspector INSTANCE = new SolrPropertyAnnotationInspector();

  @Override
  public Version version() {
    return Version.unknownVersion();
  }

  @Override
  public PropertyName findNameForSerialization(Annotated a) {
    if (a instanceof AnnotatedMethod) {
      AnnotatedMethod am = (AnnotatedMethod) a;
      Property prop = am.getAnnotation(Property.class);
      if (prop == null) return null;
      if (prop.value().isEmpty()) {
        return new PropertyName(BeanUtil.okNameForGetter(am, true));
      } else {
        return new PropertyName(prop.value());
      }

    }
    if (a instanceof AnnotatedField) {
      AnnotatedField af = (AnnotatedField) a;
      Property prop = af.getAnnotation(Property.class);
      if (prop == null) return null;
      return prop.value().isEmpty() ?
          new PropertyName(af.getName()) :
          new PropertyName(prop.value());
    }
    return null;
  }

  @Override
  public Boolean hasRequiredMarker(AnnotatedMember m) {
    Property prop = m.getAnnotation(Property.class);
    if (prop == null) return Boolean.FALSE;
    return prop.required();
  }

  @Override
  public String findPropertyDefaultValue(Annotated m) {
    Property prop = m.getAnnotation(Property.class);
    if (prop == null) return "";
    return prop.defaultValue();
  }

  @Override
  public PropertyName findNameForDeserialization(Annotated a) {
    return findNameForSerialization(a);
  }

}
