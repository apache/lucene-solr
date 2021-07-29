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

package org.apache.solr.handler.designer;

import java.io.IOException;
import java.math.RoundingMode;
import java.text.NumberFormat;
import java.text.ParsePosition;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.Sets;
import org.apache.commons.lang3.LocaleUtils;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.ManagedIndexSchema;
import org.apache.solr.schema.NumberType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.TextField;
import org.apache.solr.update.processor.ParseBooleanFieldUpdateProcessorFactory;
import org.apache.solr.update.processor.ParseDateFieldUpdateProcessorFactory;
import org.apache.solr.update.processor.ParseDoubleFieldUpdateProcessorFactory;
import org.apache.solr.update.processor.ParseLongFieldUpdateProcessorFactory;

import static org.apache.solr.common.params.CommonParams.VERSION_FIELD;
import static org.apache.solr.update.processor.ParseDateFieldUpdateProcessorFactory.validateFormatter;

// Just a quick hack to flush out the design, more intelligence is needed
public class DefaultSchemaSuggester implements SchemaSuggester {

  private static final List<String> DEFAULT_DATE_TIME_PATTERNS =
      Arrays.asList("yyyy-MM-dd['T'[HH:mm[:ss[.SSS]][z", "yyyy-MM-dd['T'[HH:mm[:ss[,SSS]][z", "yyyy-MM-dd HH:mm[:ss[.SSS]][z", "yyyy-MM-dd HH:mm[:ss[,SSS]][z", "[EEE, ]dd MMM yyyy HH:mm[:ss] z", "EEEE, dd-MMM-yy HH:mm:ss z", "EEE MMM ppd HH:mm:ss [z ]yyyy");

  private static final String FORMATS_PARAM = "format";
  private static final String DEFAULT_TIME_ZONE_PARAM = "defaultTimeZone";
  private static final String LOCALE_PARAM = "locale";
  private static final String TRUE_VALUES_PARAM = "trueValue";
  private static final String FALSE_VALUES_PARAM = "falseValue";
  private static final String CASE_SENSITIVE_PARAM = "caseSensitive";

  private static final String TYPE_CHANGE_ERROR = "Failed to parse all sample values as %s for changing type for field %s to %s";

  // boolean parsing
  private final Set<String> trueValues = new HashSet<>(Arrays.asList("true"));
  private final Set<String> falseValues = new HashSet<>(Arrays.asList("false"));
  private final List<DateTimeFormatter> dateTimeFormatters = new LinkedList<>();
  private boolean caseSensitive = false;

  @Override
  public void validateTypeChange(SchemaField field, FieldType toType, List<SolrInputDocument> docs) throws IOException {
    final NumberType toNumType = toType.getNumberType();
    if (toNumType != null) {
      validateNumericTypeChange(field, toType, docs, toNumType);
    }
  }

  protected void validateNumericTypeChange(SchemaField field, FieldType toType, List<SolrInputDocument> docs, final NumberType toNumType) {
    // desired type is numeric, make sure all the sample values are numbers
    List<Object> fieldValues = docs.stream()
        .map(d -> d.getFieldValue(field.getName()))
        .filter(Objects::nonNull)
        .flatMap(c -> (c instanceof Collection) ? ((Collection<?>) c).stream() : Stream.of(c))
        .collect(Collectors.toList());
    switch (toNumType) {
      case DOUBLE:
      case FLOAT:
        if (isFloatOrDouble(fieldValues, Locale.ROOT) == null) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
              String.format(Locale.ROOT, TYPE_CHANGE_ERROR, toNumType.name(), field.getName(), toType.getTypeName()));
        }
        break;
      case LONG:
      case INTEGER:
        if (isIntOrLong(fieldValues, Locale.ROOT) == null) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
              String.format(Locale.ROOT, TYPE_CHANGE_ERROR, toNumType.name(), field.getName(), toType.getTypeName()));
        }
        break;
      case DATE:
        if (!isDateTime(fieldValues)) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
              String.format(Locale.ROOT, TYPE_CHANGE_ERROR, toNumType.name(), field.getName(), toType.getTypeName()));
        }
        break;
    }
  }

  @Override
  public Optional<SchemaField> suggestField(String fieldName, List<Object> sampleValues, IndexSchema schema, List<String> langs) {

    // start by looking at the fieldName and seeing if there is a dynamic field in the schema that already applies
    if (schema.isDynamicField(fieldName)) {
      return Optional.of(schema.getFieldOrNull(fieldName));
    }

    // TODO: use passed in langs
    Locale locale = Locale.ROOT;

    boolean isMV = isMultiValued(sampleValues);
    String fieldTypeName = guessFieldType(fieldName, sampleValues, schema, isMV, locale);
    FieldType fieldType = schema.getFieldTypeByName(fieldTypeName);
    if (fieldType == null) {
      // TODO: construct this field type on-the-fly ...
      throw new IllegalStateException("FieldType '" + fieldTypeName + "' not found in the schema!");
    }

    Map<String, String> fieldProps = guessFieldProps(fieldName, fieldType, sampleValues, isMV, schema);
    SchemaField schemaField = schema.newField(fieldName, fieldTypeName, fieldProps);
    return Optional.of(schemaField);
  }

  @Override
  public ManagedIndexSchema adaptExistingFieldToData(SchemaField schemaField, List<Object> sampleValues, ManagedIndexSchema schema) {
    // Promote a single-valued to multi-valued if needed
    if (!schemaField.multiValued() && isMultiValued(sampleValues)) {
      // this existing field needs to be promoted to multi-valued
      SimpleOrderedMap<Object> fieldProps = schemaField.getNamedPropertyValues(false);
      fieldProps.add("multiValued", true);
      fieldProps.remove("name");
      fieldProps.remove("type");
      schema = schema.replaceField(schemaField.getName(), schemaField.getType(), fieldProps.asShallowMap());
    }
    // TODO: other "healing" type operations here ... but we have to be careful about overriding explicit user changes
    // such as a user making a text field a string field, we wouldn't want to revert that field back to text
    return schema;
  }

  @Override
  public Map<String, List<Object>> transposeDocs(List<SolrInputDocument> docs) {
    Map<String, List<Object>> mapByField = new HashMap<>();
    docs.forEach(doc -> doc.getFieldNames().forEach(f -> {
      // skip the version field on incoming docs
      if (!VERSION_FIELD.equals(f)) {
        List<Object> values = mapByField.computeIfAbsent(f, k -> new LinkedList<>());
        Collection<Object> fieldValues = doc.getFieldValues(f);
        if (fieldValues != null && !fieldValues.isEmpty()) {
          if (fieldValues.size() == 1) {
            // flatten so every field doesn't end up multi-valued
            values.add(fieldValues.iterator().next());
          } else {
            // truly multi-valued
            values.add(fieldValues);
          }
        }
      }
    }));
    return mapByField;
  }

  protected String guessFieldType(String fieldName, final List<Object> sampleValues, IndexSchema schema, boolean isMV, Locale locale) {
    String type = null;

    // flatten values to a single stream for easier analysis; also remove nulls
    List<Object> flattened = sampleValues.stream()
        .flatMap(c -> (c instanceof Collection) ? ((Collection<?>) c).stream() : Stream.of(c))
        .filter(Objects::nonNull)
        .collect(Collectors.toList());

    if (isBoolean(flattened)) {
      type = isMV ? "booleans" : "boolean";
    } else {
      String intType = isIntOrLong(flattened, locale);
      if (intType != null) {
        type = isMV ? intType + "s" : intType;
      } else {
        String floatType = isFloatOrDouble(flattened, locale);
        if (floatType != null) {
          type = isMV ? floatType + "s" : floatType;
        }
      }
    }

    if (type == null) {
      if (isDateTime(flattened)) {
        type = isMV ? "pdates" : "pdate";
      } else if (isText(flattened)) {
        type = "en".equals(locale.getLanguage()) ? "text_en" : "text_general";
      }
    }

    // if we get here and haven't made a decision, it's a string
    if (type == null) {
      type = isMV ? "strings" : "string";
    }

    return type;
  }

  protected boolean isText(List<Object> values) {
    if (values == null || values.isEmpty()) {
      return false;
    }

    int maxLength = -1;
    int maxTerms = -1;
    for (Object next : values) {
      if (!(next instanceof String)) {
        return false;
      }

      String cs = (String) next;
      int len = cs.length();
      if (len > maxLength) {
        maxLength = len;
      }

      String[] terms = cs.split("\\s+");
      if (terms.length > maxTerms) {
        maxTerms = terms.length;
      }
    }

    // don't want to choose text for fields where string will do
    // if most of the sample values are unique but only a few terms, then it's likely a text field
    return (maxLength > 60 || maxTerms > 12 || (maxTerms > 4 && values.size() >= 10 && ((float) Sets.newHashSet(values).size() / values.size()) > 0.9f));
  }

  protected String isFloatOrDouble(List<Object> values, Locale locale) {
    NumberFormat format = NumberFormat.getInstance(locale);
    format.setParseIntegerOnly(false);
    format.setRoundingMode(RoundingMode.CEILING);
    //boolean isFloat = true;
    for (Object next : values) {
      Object parsed = ParseDoubleFieldUpdateProcessorFactory.parsePossibleDouble(next, format);
      if (parsed == null) {
        // not a double ...
        return null;
      }

      /*
      Tried to be clever and pick pfloat if double precision is not needed, but the ParseDoubleFieldUpdateProcessorFactory
      doesn't work with pfloat, so you don't get any locale sensitive parsing in the URP chain, so pdouble it is ...

      Number num = (Number) parsed;
      String str = num.toString();
      int dotAt = str.indexOf('.');
      if (dotAt != -1) {
        String scalePart = str.substring(dotAt + 1);
        if (scalePart.length() > 2) {
          isFloat = false;
        }
      }
       */
    }

    return "pdouble";
  }

  protected boolean isBoolean(List<Object> values) {
    for (Object next : values) {
      Object parsed = ParseBooleanFieldUpdateProcessorFactory.parsePossibleBoolean(next, caseSensitive, trueValues, falseValues);
      if (parsed == null) {
        return false;
      }
    }
    // all values are booleans
    return true;
  }

  protected String isIntOrLong(List<Object> values, Locale locale) {
    NumberFormat format = NumberFormat.getInstance(locale);
    format.setParseIntegerOnly(true);
    long maxLong = Long.MIN_VALUE;
    for (Object next : values) {
      Object parsed = ParseLongFieldUpdateProcessorFactory.parsePossibleLong(next, format);
      if (parsed == null) {
        // not a long ...
        return null;
      } else {
        long parsedLong = ((Number) parsed).longValue();
        if (parsedLong > maxLong) {
          maxLong = parsedLong;
        }
      }
    }

    // if all values are less than some smallish threshold, then it's likely this field holds small numbers
    // but be very conservative here as it's simply an optimization and we can always fall back to long
    return maxLong < 10000 ? "pint" : "plong";
  }

  protected boolean isDateTime(List<Object> values) {
    if (dateTimeFormatters.isEmpty()) {
      return false;
    }

    for (Object next : values) {
      Object parsedDate = ParseDateFieldUpdateProcessorFactory.parsePossibleDate(next, dateTimeFormatters, new ParsePosition(0));
      if (parsedDate == null) {
        // not a date value
        return false;
      }
    }
    return true;
  }

  public boolean isMultiValued(String name, List<SolrInputDocument> docs) {
    Map<String, List<Object>> transposed = transposeDocs(docs);
    List<Object> sampleValues = transposed.get(name);
    return sampleValues != null && isMultiValued(sampleValues);
  }

  protected boolean isMultiValued(final List<Object> sampleValues) {
    for (Object next : sampleValues) {
      if (next instanceof Collection) {
        return true;
      }
    }
    return false;
  }

  protected Map<String, String> guessFieldProps(String fieldName, FieldType fieldType, List<Object> sampleValues, boolean isMV, IndexSchema schema) {
    Map<String, String> props = new HashMap<>();
    props.put("indexed", "true");

    if (isMV && !fieldType.isMultiValued()) {
      props.put("multiValued", "true"); // override the mv setting on the type
    }

    boolean docValues = true;
    if (fieldType instanceof TextField) {
      docValues = false;
    } else {
      // Not sure if this field supports docValues, so try creating a SchemaField
      Map<String, String> tmpProps = new HashMap<>(props);
      tmpProps.put("docValues", "true"); // to test if docValues is supported
      try {
        fieldType.checkSchemaField(schema.newField(fieldName, fieldType.getTypeName(), tmpProps));
      } catch (SolrException solrException) {
        docValues = false;
      }
    }

    props.put("docValues", String.valueOf(docValues));

    if (!docValues) {
      props.put("stored", "true");
    } else {
      props.put("stored", "false");
      props.put("useDocValuesAsStored", "true");
    }

    return props;
  }

  @Override
  @SuppressWarnings({"rawtypes"})
  public void init(NamedList args) {
    initDateTimeFormatters(args);
    initBooleanParsing(args);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  protected void initDateTimeFormatters(NamedList args) {

    Locale locale = Locale.US;
    String localeParam = (String) args.remove(LOCALE_PARAM);
    if (null != localeParam) {
      locale = LocaleUtils.toLocale(localeParam);
    }

    ZoneId defaultTimeZone = ZoneOffset.UTC;
    Object defaultTimeZoneParam = args.remove(DEFAULT_TIME_ZONE_PARAM);
    if (null != defaultTimeZoneParam) {
      defaultTimeZone = ZoneId.of(defaultTimeZoneParam.toString());
    }

    Collection<String> dateTimePatterns = args.removeConfigArgs(FORMATS_PARAM);
    if (dateTimePatterns == null || dateTimePatterns.isEmpty()) {
      dateTimePatterns = DEFAULT_DATE_TIME_PATTERNS;
    }

    for (String pattern : dateTimePatterns) {
      DateTimeFormatter formatter = new DateTimeFormatterBuilder().parseLenient().parseCaseInsensitive()
          .appendPattern(pattern).toFormatter(locale).withResolverStyle(ResolverStyle.LENIENT).withZone(defaultTimeZone);
      validateFormatter(formatter);
      dateTimeFormatters.add(formatter);
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  protected void initBooleanParsing(NamedList args) {
    Object caseSensitiveParam = args.remove(CASE_SENSITIVE_PARAM);
    if (null != caseSensitiveParam) {
      if (caseSensitiveParam instanceof Boolean) {
        caseSensitive = (Boolean) caseSensitiveParam;
      } else {
        caseSensitive = Boolean.parseBoolean(caseSensitiveParam.toString());
      }
    }

    Collection<String> trueValuesParam = args.removeConfigArgs(TRUE_VALUES_PARAM);
    if (!trueValuesParam.isEmpty()) {
      trueValues.clear();
      for (String trueVal : trueValuesParam) {
        trueValues.add(caseSensitive ? trueVal : trueVal.toLowerCase(Locale.ROOT));
      }
    }

    Collection<String> falseValuesParam = args.removeConfigArgs(FALSE_VALUES_PARAM);
    if (!falseValuesParam.isEmpty()) {
      falseValues.clear();
      for (String val : falseValuesParam) {
        final String falseVal = caseSensitive ? val : val.toLowerCase(Locale.ROOT);
        if (trueValues.contains(falseVal)) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
              "Param '" + FALSE_VALUES_PARAM + "' contains a value also in param '" + TRUE_VALUES_PARAM
                  + "': '" + val + "'");
        }
        falseValues.add(falseVal);
      }
    }
  }
}
