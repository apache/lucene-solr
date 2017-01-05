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

import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.legacy.LegacyFieldType;
import org.apache.lucene.legacy.LegacyIntField;
import org.apache.lucene.legacy.LegacyNumericRangeQuery;
import org.apache.lucene.legacy.LegacyNumericType;
import org.apache.lucene.legacy.LegacyNumericUtils;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.EnumFieldSource;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DocValuesRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.solr.common.EnumFieldValue;
import org.apache.solr.common.SolrException;
import org.apache.solr.response.TextResponseWriter;
import org.apache.solr.search.QParser;
import org.apache.solr.uninverting.UninvertingReader.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/***
 * Field type for support of string values with custom sort order.
 */
public class EnumField extends PrimitiveFieldType {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  protected static final String PARAM_ENUMS_CONFIG = "enumsConfig";
  protected static final String PARAM_ENUM_NAME = "enumName";
  protected static final Integer DEFAULT_VALUE = -1;
  protected static final int DEFAULT_PRECISION_STEP = Integer.MAX_VALUE;

  protected Map<String, Integer> enumStringToIntMap = new HashMap<>();
  protected Map<Integer, String> enumIntToStringMap = new HashMap<>();

  protected String enumsConfigFile;
  protected String enumName;

  /**
   * {@inheritDoc}
   */
  @Override
  protected void init(IndexSchema schema, Map<String, String> args) {
    super.init(schema, args);
    enumsConfigFile = args.get(PARAM_ENUMS_CONFIG);
    if (enumsConfigFile == null) {
      throw new SolrException(SolrException.ErrorCode.NOT_FOUND, "No enums config file was configured.");
    }
    enumName = args.get(PARAM_ENUM_NAME);
    if (enumName == null) {
      throw new SolrException(SolrException.ErrorCode.NOT_FOUND, "No enum name was configured.");
    }

    InputStream is = null;

    try {
      is = schema.getResourceLoader().openResource(enumsConfigFile);
      final DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
      try {
        final Document doc = dbf.newDocumentBuilder().parse(is);
        final XPathFactory xpathFactory = XPathFactory.newInstance();
        final XPath xpath = xpathFactory.newXPath();
        final String xpathStr = String.format(Locale.ROOT, "/enumsConfig/enum[@name='%s']", enumName);
        final NodeList nodes = (NodeList) xpath.evaluate(xpathStr, doc, XPathConstants.NODESET);
        final int nodesLength = nodes.getLength();
        if (nodesLength == 0) {
          String exceptionMessage = String.format(Locale.ENGLISH, "No enum configuration found for enum '%s' in %s.",
                  enumName, enumsConfigFile);
          throw new SolrException(SolrException.ErrorCode.NOT_FOUND, exceptionMessage);
        }
        if (nodesLength > 1) {
          if (log.isWarnEnabled())
            log.warn("More than one enum configuration found for enum '{}' in {}. The last one was taken.", enumName, enumsConfigFile);
        }
        final Node enumNode = nodes.item(nodesLength - 1);
        final NodeList valueNodes = (NodeList) xpath.evaluate("value", enumNode, XPathConstants.NODESET);
        for (int i = 0; i < valueNodes.getLength(); i++) {
          final Node valueNode = valueNodes.item(i);
          final String valueStr = valueNode.getTextContent();
          if ((valueStr == null) || (valueStr.length() == 0)) {
            final String exceptionMessage = String.format(Locale.ENGLISH, "A value was defined with an no value in enum '%s' in %s.",
                    enumName, enumsConfigFile);
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, exceptionMessage);
          }
          if (enumStringToIntMap.containsKey(valueStr)) {
            final String exceptionMessage = String.format(Locale.ENGLISH, "A duplicated definition was found for value '%s' in enum '%s' in %s.",
                    valueStr, enumName, enumsConfigFile);
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, exceptionMessage);
          }
          enumIntToStringMap.put(i, valueStr);
          enumStringToIntMap.put(valueStr, i);
        }
      }
      catch (ParserConfigurationException | XPathExpressionException | SAXException e) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Error parsing enums config.", e);
      }
    }
    catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Error while opening enums config.", e);
    } finally {
      try {
        if (is != null) {
          is.close();
        }
      }
      catch (IOException e) {
        e.printStackTrace();
      }
    }

    if ((enumStringToIntMap.size() == 0) || (enumIntToStringMap.size() == 0)) {
      String exceptionMessage = String.format(Locale.ENGLISH, "Invalid configuration was defined for enum '%s' in %s.",
              enumName, enumsConfigFile);
      throw new SolrException(SolrException.ErrorCode.NOT_FOUND, exceptionMessage);
    }

    args.remove(PARAM_ENUMS_CONFIG);
    args.remove(PARAM_ENUM_NAME);
  }


  /**
   * {@inheritDoc}
   */
  @Override
  public EnumFieldValue toObject(IndexableField f) {
    Integer intValue = null;
    String stringValue = null;
    final Number val = f.numericValue();
    if (val != null) {
      intValue = val.intValue();
      stringValue = intValueToStringValue(intValue);
    }
    return new EnumFieldValue(intValue, stringValue);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public SortField getSortField(SchemaField field, boolean top) {
    field.checkSortability();
    final Object missingValue = Integer.MIN_VALUE;
    SortField sf = new SortField(field.getName(), SortField.Type.INT, top);
    sf.setMissingValue(missingValue);
    return sf;
  }
  
  @Override
  public Type getUninversionType(SchemaField sf) {
    if (sf.multiValued()) {
      return Type.SORTED_SET_INTEGER;
    } else {
      return Type.LEGACY_INTEGER;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ValueSource getValueSource(SchemaField field, QParser qparser) {
    field.checkFieldCacheSource();
    return new EnumFieldSource(field.getName(), enumIntToStringMap, enumStringToIntMap);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void write(TextResponseWriter writer, String name, IndexableField f) throws IOException {
    final Number val = f.numericValue();
    if (val == null) {
      writer.writeNull(name);
      return;
    }

    final String readableValue = intValueToStringValue(val.intValue());
    writer.writeStr(name, readableValue, true);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isTokenized() {
    return false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LegacyNumericType getNumericType() {
    return LegacyNumericType.INT;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Query getRangeQuery(QParser parser, SchemaField field, String min, String max, boolean minInclusive, boolean maxInclusive) {
    Integer minValue = stringValueToIntValue(min);
    Integer maxValue = stringValueToIntValue(max);

    if (field.multiValued() && field.hasDocValues() && !field.indexed()) {
      // for the multi-valued dv-case, the default rangeimpl over toInternal is correct
      return super.getRangeQuery(parser, field, minValue.toString(), maxValue.toString(), minInclusive, maxInclusive);
    }
    Query query = null;
    final boolean matchOnly = field.hasDocValues() && !field.indexed();
    if (matchOnly) {
      query = new ConstantScoreQuery(DocValuesRangeQuery.newLongRange(field.getName(),
              min == null ? null : minValue.longValue(),
              max == null ? null : maxValue.longValue(),
              minInclusive, maxInclusive));
    } else {
      query = LegacyNumericRangeQuery.newIntRange(field.getName(), DEFAULT_PRECISION_STEP,
          min == null ? null : minValue,
          max == null ? null : maxValue,
          minInclusive, maxInclusive);
    }

    return query;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void checkSchemaField(SchemaField field) {
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String readableToIndexed(String val) {
    if (val == null)
      return null;

    final BytesRefBuilder bytes = new BytesRefBuilder();
    readableToIndexed(val, bytes);
    return bytes.get().utf8ToString();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void readableToIndexed(CharSequence val, BytesRefBuilder result) {
    final String s = val.toString();
    if (s == null)
      return;

    final Integer intValue = stringValueToIntValue(s);
    LegacyNumericUtils.intToPrefixCoded(intValue, 0, result);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String toInternal(String val) {
    return readableToIndexed(val);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String toExternal(IndexableField f) {
    final Number val = f.numericValue();
    if (val == null)
      return null;

    return intValueToStringValue(val.intValue());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String indexedToReadable(String indexedForm) {
    if (indexedForm == null)
      return null;
    final BytesRef bytesRef = new BytesRef(indexedForm);
    final Integer intValue = LegacyNumericUtils.prefixCodedToInt(bytesRef);
    return intValueToStringValue(intValue);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CharsRef indexedToReadable(BytesRef input, CharsRefBuilder output) {
    final Integer intValue = LegacyNumericUtils.prefixCodedToInt(input);
    final String stringValue = intValueToStringValue(intValue);
    output.grow(stringValue.length());
    output.setLength(stringValue.length());
    stringValue.getChars(0, output.length(), output.chars(), 0);
    return output.get();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public EnumFieldValue toObject(SchemaField sf, BytesRef term) {
    final Integer intValue = LegacyNumericUtils.prefixCodedToInt(term);
    final String stringValue = intValueToStringValue(intValue);
    return new EnumFieldValue(intValue, stringValue);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String storedToIndexed(IndexableField f) {
    final Number val = f.numericValue();
    if (val == null)
      return null;
    final BytesRefBuilder bytes = new BytesRefBuilder();
    LegacyNumericUtils.intToPrefixCoded(val.intValue(), 0, bytes);
    return bytes.get().utf8ToString();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public IndexableField createField(SchemaField field, Object value, float boost) {
    final boolean indexed = field.indexed();
    final boolean stored = field.stored();
    final boolean docValues = field.hasDocValues();

    if (!indexed && !stored && !docValues) {
      if (log.isTraceEnabled())
        log.trace("Ignoring unindexed/unstored field: " + field);
      return null;
    }
    final Integer intValue = stringValueToIntValue(value.toString());
    if (intValue == null || intValue.equals(DEFAULT_VALUE))
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown value for enum field: " + value.toString());

    final LegacyFieldType newType = new LegacyFieldType();

    newType.setTokenized(field.isTokenized());
    newType.setStored(field.stored());
    newType.setOmitNorms(field.omitNorms());
    newType.setIndexOptions(field.indexOptions());
    newType.setStoreTermVectors(field.storeTermVector());
    newType.setStoreTermVectorOffsets(field.storeTermOffsets());
    newType.setStoreTermVectorPositions(field.storeTermPositions());
    newType.setStoreTermVectorPayloads(field.storeTermPayloads());
    newType.setNumericType(LegacyNumericType.INT);
    newType.setNumericPrecisionStep(DEFAULT_PRECISION_STEP);

    final org.apache.lucene.document.Field f;
    f = new LegacyIntField(field.getName(), intValue.intValue(), newType);

    f.setBoost(boost);
    return f;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<IndexableField> createFields(SchemaField sf, Object value, float boost) {
    if (sf.hasDocValues()) {
      List<IndexableField> fields = new ArrayList<>();
      final IndexableField field = createField(sf, value, boost);
      fields.add(field);

      if (sf.multiValued()) {
        BytesRefBuilder bytes = new BytesRefBuilder();
        readableToIndexed(stringValueToIntValue(value.toString()).toString(), bytes);
        fields.add(new SortedSetDocValuesField(sf.getName(), bytes.toBytesRef()));
      } else {
        final long bits = field.numericValue().intValue();
        fields.add(new NumericDocValuesField(sf.getName(), bits));
      }
      return fields;
    } else {
      return Collections.singletonList(createField(sf, value, boost));
    }
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

