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
package org.apache.solr.handler.extraction;

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.beans.PropertyEditor;
import java.beans.PropertyEditorManager;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.util.SafeXMLParsing;
import org.apache.tika.parser.ParseContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class ParseContextConfig {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  private final Map<Class<?>, Object> entries = new HashMap<>();

  /** Creates an empty Config without any settings (used as placeholder). */
  public ParseContextConfig() {
  }

  /** Creates a {@code ParseContextConfig} from the given XML DOM element. */
  public ParseContextConfig(SolrResourceLoader resourceLoader, Element element) throws Exception {
    extract(element, resourceLoader);
  }

  /** Creates a {@code ParseContextConfig} from the given XML file, loaded from the given {@link SolrResourceLoader}. */
  public ParseContextConfig(SolrResourceLoader resourceLoader, String parseContextConfigLoc) throws Exception {
    this(resourceLoader, loadConfigFile(resourceLoader, parseContextConfigLoc).getDocumentElement());
  }
  
  private static Document loadConfigFile(SolrResourceLoader resourceLoader, String parseContextConfigLoc) throws Exception {
    return SafeXMLParsing.parseConfigXML(log, resourceLoader, parseContextConfigLoc);
  }

  private void extract(Element element, SolrResourceLoader loader) throws Exception {
    final NodeList xmlEntries = element.getElementsByTagName("entry");
    for (int i = 0, c1 = xmlEntries.getLength(); i < c1; i++) {
      final NamedNodeMap xmlEntryAttributes = xmlEntries.item(i).getAttributes();
      final String className = xmlEntryAttributes.getNamedItem("class").getNodeValue();
      final String implementationName = xmlEntryAttributes.getNamedItem("impl").getNodeValue();

      final NodeList xmlProperties = ((Element)xmlEntries.item(i)).getElementsByTagName("property");

      final Class<?> interfaceClass = loader.findClass(className, Object.class);
      final BeanInfo beanInfo = Introspector.getBeanInfo(interfaceClass, Introspector.IGNORE_ALL_BEANINFO);
      
      final HashMap<String, PropertyDescriptor> descriptorMap = new HashMap<>();
      for (final PropertyDescriptor pd : beanInfo.getPropertyDescriptors()) {
        descriptorMap.put(pd.getName(), pd);
      }

      final Object instance = loader.newInstance(implementationName, Object.class);
      if (!interfaceClass.isInstance(instance)) {
        throw new IllegalArgumentException("Implementation class does not extend " + interfaceClass.getName());
      }

      for (int j = 0, c2 = xmlProperties.getLength(); j < c2; j++) {
        final Node xmlProperty = xmlProperties.item(j);
        final NamedNodeMap xmlPropertyAttributes = xmlProperty.getAttributes();

        final String propertyName = xmlPropertyAttributes.getNamedItem("name").getNodeValue();
        final String propertyValue = xmlPropertyAttributes.getNamedItem("value").getNodeValue();

        final PropertyDescriptor propertyDescriptor = descriptorMap.get(propertyName);
        if (propertyDescriptor == null) {
          throw new IllegalArgumentException(String.format(Locale.ENGLISH, "Unknown bean property %s in class %s",
              propertyName, interfaceClass.getName()));
        }
        final Method method = propertyDescriptor.getWriteMethod();
        if (method == null) {
          throw new IllegalArgumentException(String.format(Locale.ENGLISH, "Cannot set bean property %s in class %s (no write method available)",
              propertyName, interfaceClass.getName()));
        }
        method.invoke(instance, getValueFromString(propertyDescriptor.getPropertyType(), propertyValue));
      }

      entries.put(interfaceClass, instance);
    }
  }

  private Object getValueFromString(Class<?> targetType, String text) {
    final PropertyEditor editor = PropertyEditorManager.findEditor(targetType);
    if (editor == null) {
      throw new IllegalArgumentException("Cannot set properties of type " + targetType.getName());
    }
    editor.setAsText(text);
    return editor.getValue();
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  public ParseContext create() {
    final ParseContext result = new ParseContext();

    for (Map.Entry<Class<?>, Object> entry : entries.entrySet()){
      result.set((Class) entry.getKey(), entry.getValue());
    }

    return result;
  }
}
