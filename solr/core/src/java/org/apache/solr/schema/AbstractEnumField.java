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

package org.apache.solr.schema;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.EnumFieldSource;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.solr.common.EnumFieldValue;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.response.TextResponseWriter;
import org.apache.solr.search.QParser;
import org.apache.solr.util.SafeXMLParsing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/***
 * Abstract Field type for support of string values with custom sort order.
 */
public abstract class AbstractEnumField extends PrimitiveFieldType {
  protected EnumMapping enumMapping;
  
  @Override
  protected void init(IndexSchema schema, Map<String, String> args) {
    super.init(schema, args);
    enumMapping = new EnumMapping(schema, this, args);
  }

  public EnumMapping getEnumMapping() {
    return enumMapping;
  }

  /**
   * Models all the info contained in an enums config XML file
   * @lucene.internal
   */
  public static final class EnumMapping {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static final String PARAM_ENUMS_CONFIG = "enumsConfig";
    public static final String PARAM_ENUM_NAME = "enumName";
    public static final Integer DEFAULT_VALUE = -1;
    
    public final Map<String, Integer> enumStringToIntMap;
    public final Map<Integer, String> enumIntToStringMap;
    
    protected final String enumsConfigFile;
    protected final String enumName;

    /** 
     * Takes in a FieldType and the initArgs Map used for that type, removing the keys
     * that specify the enum.
     *
     * @param schema for opening resources
     * @param fieldType Used for logging or error messages
     * @param args the init args to comsume the enum name + config file from
     */
    public EnumMapping(IndexSchema schema, FieldType fieldType, Map<String, String> args) {
      final String ftName = fieldType.getTypeName();
      
      // NOTE: ghosting member variables for most of constructor
      final Map<String, Integer> enumStringToIntMap = new HashMap<>();
      final Map<Integer, String> enumIntToStringMap = new HashMap<>();
      
      enumsConfigFile = args.get(PARAM_ENUMS_CONFIG);
      if (enumsConfigFile == null) {
        throw new SolrException(SolrException.ErrorCode.NOT_FOUND,
                                ftName + ": No enums config file was configured.");
      }
      enumName = args.get(PARAM_ENUM_NAME);
      if (enumName == null) {
        throw new SolrException(SolrException.ErrorCode.NOT_FOUND,
                                ftName + ": No enum name was configured.");
      }
      
      final SolrResourceLoader loader = schema.getResourceLoader(); 
      try {
        log.debug("Reloading enums config file from {}", enumsConfigFile);
        Document doc = SafeXMLParsing.parseConfigXML(log, loader, enumsConfigFile);
        final XPathFactory xpathFactory = XPathFactory.newInstance();
        final XPath xpath = xpathFactory.newXPath();
        final String xpathStr = String.format(Locale.ROOT, "/enumsConfig/enum[@name='%s']", enumName);
        final NodeList nodes = (NodeList) xpath.evaluate(xpathStr, doc, XPathConstants.NODESET);
        final int nodesLength = nodes.getLength();
        if (nodesLength == 0) {
          String exceptionMessage = String.format
            (Locale.ENGLISH, "%s: No enum configuration found for enum '%s' in %s.",
             ftName, enumName, enumsConfigFile);
          throw new SolrException(SolrException.ErrorCode.NOT_FOUND, exceptionMessage);
        }
        if (nodesLength > 1) {
          log.warn("{}: More than one enum configuration found for enum '{}' in {}. The last one was taken."
              , ftName, enumName, enumsConfigFile);
        }
        final Node enumNode = nodes.item(nodesLength - 1);
        final NodeList valueNodes = (NodeList) xpath.evaluate("value", enumNode, XPathConstants.NODESET);
        for (int i = 0; i < valueNodes.getLength(); i++) {
          final Node valueNode = valueNodes.item(i);
          final String valueStr = valueNode.getTextContent();
          if ((valueStr == null) || (valueStr.length() == 0)) {
            final String exceptionMessage = String.format
              (Locale.ENGLISH, "%s: A value was defined with an no value in enum '%s' in %s.",
               ftName, enumName, enumsConfigFile);
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, exceptionMessage);
          }
          if (enumStringToIntMap.containsKey(valueStr)) {
            final String exceptionMessage = String.format
              (Locale.ENGLISH, "%s: A duplicated definition was found for value '%s' in enum '%s' in %s.",
               ftName, valueStr, enumName, enumsConfigFile);
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, exceptionMessage);
          }
          enumIntToStringMap.put(i, valueStr);
          enumStringToIntMap.put(valueStr, i);
        }
      } catch (IOException | SAXException | XPathExpressionException e) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                                ftName + ": Error while parsing enums config.", e);
      }
      
      if ((enumStringToIntMap.size() == 0) || (enumIntToStringMap.size() == 0)) {
        String exceptionMessage = String.format
          (Locale.ENGLISH, "%s: Invalid configuration was defined for enum '%s' in %s.",
           ftName, enumName, enumsConfigFile);
        throw new SolrException(SolrException.ErrorCode.NOT_FOUND, exceptionMessage);
      }
      
      this.enumStringToIntMap = Collections.unmodifiableMap(enumStringToIntMap);
      this.enumIntToStringMap = Collections.unmodifiableMap(enumIntToStringMap);
      
      args.remove(PARAM_ENUMS_CONFIG);
      args.remove(PARAM_ENUM_NAME);
    }
    
    
    
    /**
     * Converting the (internal) integer value (indicating the sort order) to string (displayed) value
     * @param intVal integer value
     * @return string value
     */
    public String intValueToStringValue(Integer intVal) {
      if (intVal == null)
        return null;
      
      final String enumString = enumIntToStringMap.get(intVal);
      if (enumString != null)
        return enumString;
      // can't find matching enum name - return DEFAULT_VALUE.toString()
      return DEFAULT_VALUE.toString();
    }
    
    /**
     * Converting the string (displayed) value (internal) to integer value (indicating the sort order)
     * @param stringVal string value
     * @return integer value
     */
    public Integer stringValueToIntValue(String stringVal) {
      if (stringVal == null)
        return null;
      
      Integer intValue;
      final Integer enumInt = enumStringToIntMap.get(stringVal);
      if (enumInt != null) //enum int found for string
        return enumInt;
      
      //enum int not found for string
      intValue = tryParseInt(stringVal);
      if (intValue == null) //not Integer
        intValue = DEFAULT_VALUE;
      final String enumString = enumIntToStringMap.get(intValue);
      if (enumString != null) //has matching string
        return intValue;
      
      return DEFAULT_VALUE;
    }
    
    private static Integer tryParseInt(String valueStr) {
      Integer intValue = null;
      try {
        intValue = Integer.parseInt(valueStr);
      }
      catch (NumberFormatException e) {
      }
      return intValue;
    }
  }

  @Override
  public EnumFieldValue toObject(IndexableField f) {
    Integer intValue = null;
    String stringValue = null;
    final Number val = f.numericValue();
    if (val != null) {
      intValue = val.intValue();
      stringValue = enumMapping.intValueToStringValue(intValue);
    }
    return new EnumFieldValue(intValue, stringValue);
  }

  @Override
  public SortField getSortField(SchemaField field, boolean top) {
    if (field.multiValued()) {
      MultiValueSelector selector = field.type.getDefaultMultiValueSelectorForSort(field, top);
      if (null != selector) {
        final SortField result = getSortedSetSortField(field, selector.getSortedSetSelectorType(),
                                                       // yes: Strings, it's how SortedSetSortField works
                                                       top, SortField.STRING_FIRST, SortField.STRING_LAST);
        if (null == result.getMissingValue()) {
          // special case 'enum' default behavior: assume missing values are "below" all enum values
          result.setMissingValue(SortField.STRING_FIRST);
        }
        return result;
      }
    }
    
    // else...
    // either single valued, or don't support implicit multi selector
    // (in which case let getSortField() give the error)
    final SortField result = getSortField(field, SortField.Type.INT, top, Integer.MIN_VALUE, Integer.MAX_VALUE);
    
    if (null == result.getMissingValue()) {
      // special case 'enum' default behavior: assume missing values are "below" all enum values
      result.setMissingValue(Integer.MIN_VALUE);
    }
    return result;
  }
  
  @Override
  public ValueSource getValueSource(SchemaField field, QParser qparser) {
    field.checkFieldCacheSource();
    return new EnumFieldSource(field.getName(), enumMapping.enumIntToStringMap, enumMapping.enumStringToIntMap);
  }

  @Override
  public void write(TextResponseWriter writer, String name, IndexableField f) throws IOException {
    final Number val = f.numericValue();
    if (val == null) {
      writer.writeNull(name);
      return;
    }

    final String readableValue = enumMapping.intValueToStringValue(val.intValue());
    writer.writeStr(name, readableValue, true);
  }

  @Override
  public boolean isTokenized() {
    return false;
  }

  @Override
  public NumberType getNumberType() {
    return NumberType.INTEGER;
  }

  @Override
  public String readableToIndexed(String val) {
    if (val == null)
      return null;

    final BytesRefBuilder bytes = new BytesRefBuilder();
    readableToIndexed(val, bytes);
    return bytes.get().utf8ToString();
  }

  @Override
  public String toInternal(String val) {
    return readableToIndexed(val);
  }

  @Override
  public String toExternal(IndexableField f) {
    final Number val = f.numericValue();
    if (val == null)
      return null;

    return enumMapping.intValueToStringValue(val.intValue());
  }
  
  @Override
  public Object toNativeType(Object val) {
    if (val instanceof CharSequence) {
      final String str = val.toString();
      final Integer entry = enumMapping.enumStringToIntMap.get(str);
      if (entry != null) {
        return new EnumFieldValue(entry, str);
      } else if (NumberUtils.isCreatable(str)) {
        final int num = Integer.parseInt(str);
        return new EnumFieldValue(num, enumMapping.enumIntToStringMap.get(num));
      }
    } else if (val instanceof Number) {
      final int num = ((Number) val).intValue();
      return new EnumFieldValue(num, enumMapping.enumIntToStringMap.get(num));
    }

    return super.toNativeType(val);
  }
}
