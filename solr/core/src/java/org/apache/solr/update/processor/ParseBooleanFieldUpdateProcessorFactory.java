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
package org.apache.solr.update.processor;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.BoolField;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.update.processor.FieldMutatingUpdateProcessor.FieldNameSelector;

/**
 * <p>
 * Attempts to mutate selected fields that have only CharSequence-typed values
 * into Boolean values.
 * </p>
 * <p>
 * The default selection behavior is to mutate both those fields that don't match
 * a schema field, as well as those fields that do match a schema field and have
 * a field type that uses class solr.BooleanField.
 * </p>
 * <p>
 * If all values are parseable as boolean (or are already Boolean), then the field
 * will be mutated, replacing each value with its parsed Boolean equivalent; 
 * otherwise, no mutation will occur.
 * </p>
 * <p>
 * The default true and false values are "true" and "false", respectively, and match
 * case-insensitively.  The following configuration changes the acceptable values, and
 * requires a case-sensitive match - note that either individual &lt;str&gt; elements
 * or &lt;arr&gt;-s of &lt;str&gt; elements may be used to specify the trueValue-s
 * and falseValue-s:
 * </p>
 *
 * <pre class="prettyprint">
 * &lt;processor class="solr.ParseBooleanFieldUpdateProcessorFactory"&gt;
 *   &lt;str name="caseSensitive"&gt;true&lt;/str&gt;
 *   &lt;str name="trueValue"&gt;True&lt;/str&gt;
 *   &lt;str name="trueValue"&gt;Yes&lt;/str&gt;
 *   &lt;arr name="falseValue"&gt;
 *     &lt;str&gt;False&lt;/str&gt;
 *     &lt;str&gt;No&lt;/str&gt;
 *   &lt;/arr&gt;
 * &lt;/processor&gt;</pre>
 * @since 4.4.0
 */
public class ParseBooleanFieldUpdateProcessorFactory extends FieldMutatingUpdateProcessorFactory {
  private static final String TRUE_VALUES_PARAM = "trueValue";
  private static final String FALSE_VALUES_PARAM = "falseValue";
  private static final String CASE_SENSITIVE_PARAM = "caseSensitive";
  
  private final Set<String> trueValues = new HashSet<>(Arrays.asList("true"));
  private final Set<String> falseValues = new HashSet<>(Arrays.asList("false"));
  private boolean caseSensitive = false;

  @Override
  public UpdateRequestProcessor getInstance(SolrQueryRequest req, 
                                            SolrQueryResponse rsp, 
                                            UpdateRequestProcessor next) {
    return new AllValuesOrNoneFieldMutatingUpdateProcessor(getSelector(), next) {
      @Override
      protected Object mutateValue(Object srcVal) {
        Object parsed = parsePossibleBoolean(srcVal, caseSensitive, trueValues, falseValues);
        return parsed != null ? parsed : SKIP_FIELD_VALUE_LIST_SINGLETON;
      }
    };
  }

  @Override
  public void init(@SuppressWarnings({"rawtypes"})NamedList args) {
    Object caseSensitiveParam = args.remove(CASE_SENSITIVE_PARAM);
    if (null != caseSensitiveParam) {
      if (caseSensitiveParam instanceof Boolean) {
        caseSensitive = (Boolean)caseSensitiveParam;
      } else {
        caseSensitive = Boolean.parseBoolean(caseSensitiveParam.toString());
      }
    }

    @SuppressWarnings({"unchecked"})
    Collection<String> trueValuesParam = args.removeConfigArgs(TRUE_VALUES_PARAM);
    if ( ! trueValuesParam.isEmpty()) {
      trueValues.clear();
      for (String trueVal : trueValuesParam) {
        trueValues.add(caseSensitive ? trueVal : trueVal.toLowerCase(Locale.ROOT));
      }
    }

    @SuppressWarnings({"unchecked"})
    Collection<String> falseValuesParam = args.removeConfigArgs(FALSE_VALUES_PARAM);
    if ( ! falseValuesParam.isEmpty()) {
      falseValues.clear();
      for (String val : falseValuesParam) {
        final String falseVal = caseSensitive ? val : val.toLowerCase(Locale.ROOT);
        if (trueValues.contains(falseVal)) {
          throw new SolrException(ErrorCode.SERVER_ERROR,
              "Param '" + FALSE_VALUES_PARAM + "' contains a value also in param '" + TRUE_VALUES_PARAM
                  + "': '" + val + "'");
        }
        falseValues.add(falseVal);
      }
    }
    super.init(args);
  }


  /**
   * Returns true if the field doesn't match any schema field or dynamic field,
   *           or if the matched field's type is BoolField
   */
  @Override
  public FieldNameSelector getDefaultSelector(final SolrCore core) {
    return fieldName -> {
      final IndexSchema schema = core.getLatestSchema();
      FieldType type = schema.getFieldTypeNoEx(fieldName);
      return (null == type) || (type instanceof BoolField);
    };
  }

  public static Object parsePossibleBoolean(Object srcVal, boolean caseSensitive, Set<String> trueValues, Set<String> falseValues) {
    if (srcVal instanceof CharSequence) {
      String stringVal = caseSensitive ? srcVal.toString() : srcVal.toString().toLowerCase(Locale.ROOT);
      if (trueValues.contains(stringVal)) {
        return true;
      } else if (falseValues.contains(stringVal)) {
        return false;
      } else {
        return null;
      }
    }
    if (srcVal instanceof Boolean) {
      return srcVal;
    }
    return null;
  }
}
