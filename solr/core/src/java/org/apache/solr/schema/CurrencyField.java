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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.util.ResourceLoaderAware;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;


/**
 * Field type for support of monetary values.
 * <p>
 * See <a href="http://wiki.apache.org/solr/CurrencyField">http://wiki.apache.org/solr/CurrencyField</a>
 * @deprecated Use {@link CurrencyFieldType}
 */
@Deprecated
public class CurrencyField extends CurrencyFieldType implements SchemaAware, ResourceLoaderAware {
  protected static final String FIELD_SUFFIX_AMOUNT_RAW = "_amount_raw";
  protected static final String FIELD_SUFFIX_CURRENCY = "_currency";
  protected static final String FIELD_TYPE_AMOUNT_RAW = "amount_raw_type_long";
  protected static final String FIELD_TYPE_CURRENCY = "currency_type_string";
  protected static final String PARAM_PRECISION_STEP = "precisionStep";
  protected static final String DEFAULT_PRECISION_STEP = "0";

  @Override
  protected void init(IndexSchema schema, Map<String, String> args) {
    
    // Fail if amountLongSuffix or codeStrSuffix are specified
    List<String> unknownParams = new ArrayList<>();
    fieldSuffixAmountRaw = args.get(PARAM_FIELD_SUFFIX_AMOUNT_RAW);
    if (fieldSuffixAmountRaw != null) {
      unknownParams.add(PARAM_FIELD_SUFFIX_AMOUNT_RAW); 
    }
    fieldSuffixCurrency = args.get(PARAM_FIELD_SUFFIX_CURRENCY);
    if (fieldSuffixCurrency != null) {
      unknownParams.add(PARAM_FIELD_SUFFIX_CURRENCY);
    }
    if ( ! unknownParams.isEmpty()) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "Unknown parameter(s): " + unknownParams);
    }
    
    String precisionStepString = args.get(PARAM_PRECISION_STEP);
    if (precisionStepString == null) {
      precisionStepString = DEFAULT_PRECISION_STEP;
    } else {
      args.remove(PARAM_PRECISION_STEP);
    }

    // NOTE: because we're not using the PluginLoader to register these field types, they aren't "real"
    // field types and never get Schema default properties (based on schema.xml's version attribute)
    // so only the properties explicitly set here (or on the SchemaField's we create from them) are used.
    //
    // In theory we should fix this, but since this class is already deprecated, we'll leave it alone
    // to simplify the risk of back-compat break for existing users.
    
    // Initialize field type for amount
    fieldTypeAmountRaw = new TrieLongField();
    fieldTypeAmountRaw.setTypeName(FIELD_TYPE_AMOUNT_RAW);
    Map<String,String> map = new HashMap<>(1);
    map.put("precisionStep", precisionStepString);
    fieldTypeAmountRaw.init(schema, map);
    fieldSuffixAmountRaw = FIELD_SUFFIX_AMOUNT_RAW;

    // Initialize field type for currency string
    fieldTypeCurrency = new StrField();
    fieldTypeCurrency.setTypeName(FIELD_TYPE_CURRENCY);
    fieldTypeCurrency.init(schema, Collections.emptyMap());
    fieldSuffixCurrency = FIELD_SUFFIX_CURRENCY;

    super.init(schema, args); // Must be called last so that field types are not doubly created
  }

  private void createDynamicCurrencyField(String suffix, FieldType type) {
    String name = "*" + POLY_FIELD_SEPARATOR + suffix;
    Map<String, String> props = new HashMap<>();
    props.put("indexed", "true");
    props.put("stored", "false");
    props.put("multiValued", "false");
    props.put("omitNorms", "true");
    props.put("uninvertible", "true");
    int p = SchemaField.calcProps(name, type, props);
    schema.registerDynamicFields(SchemaField.create(name, type, p, null));
  }

  /**
   * When index schema is informed, add dynamic fields "*____currency" and "*____amount_raw". 
   *
   * {@inheritDoc}
   *
   * @param schema {@inheritDoc}
   */
  @Override
  public void inform(IndexSchema schema) {
    createDynamicCurrencyField(FIELD_SUFFIX_CURRENCY,   fieldTypeCurrency);
    createDynamicCurrencyField(FIELD_SUFFIX_AMOUNT_RAW, fieldTypeAmountRaw);
    super.inform(schema);
  }
}
