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

import org.apache.commons.lang3.LocaleUtils;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.update.processor.FieldMutatingUpdateProcessor.FieldNameSelector;

import java.util.Locale;

/**
 * Abstract base class for numeric parsing update processor factories.
 * Subclasses can optionally configure a locale.  If no locale is configured,
 * then {@link Locale#ROOT} will be used.  E.g. to configure the French/France
 * locale:
 * 
 * <pre class="prettyprint">
 * &lt;processor class="solr.Parse[Type]FieldUpdateProcessorFactory"&gt;
 *   &lt;str name="locale"&gt;fr_FR&lt;/str&gt;
 *   [...]
 * &lt;/processor&gt;</pre>
 *
 * <p>
 * See {@link Locale} for a description of acceptable language, country (optional)
 * and variant (optional) values, joined with underscore(s).
 * </p>
 * @since 4.4.0
 */
public abstract class ParseNumericFieldUpdateProcessorFactory extends FieldMutatingUpdateProcessorFactory {

  private static final String LOCALE_PARAM = "locale";

  protected Locale locale = Locale.ROOT;

  @Override
  public void init(@SuppressWarnings({"rawtypes"})NamedList args) {
    String localeParam = (String)args.remove(LOCALE_PARAM);
    if (null != localeParam) {
      locale = LocaleUtils.toLocale(localeParam);
    }
    super.init(args);
  }

  /**
   * Returns true if the given FieldType is compatible with this parsing factory.
   */
  protected abstract boolean isSchemaFieldTypeCompatible(FieldType type);  

  /**
   * Returns true if the field doesn't match any schema field or dynamic field,
   *           or if the matched field's type is compatible
   * @param core Where to get the current schema from
   */
  @Override
  public FieldNameSelector getDefaultSelector(final SolrCore core) {
    return fieldName -> {
      final IndexSchema schema = core.getLatestSchema();
      FieldType type = schema.getFieldTypeNoEx(fieldName);
      return (null == type) || isSchemaFieldTypeCompatible(type);
    };
  }
}
