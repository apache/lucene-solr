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

import org.apache.commons.lang.LocaleUtils;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.DateValueFieldType;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * <p>
 * Attempts to mutate selected fields that have only CharSequence-typed values
 * into Date values.  Solr will continue to index date/times in the UTC time
 * zone, but the input date/times may be expressed using other time zones,
 * and will be converted to UTC when they are mutated.
 * </p>
 * <p>
 * The default selection behavior is to mutate both those fields that don't match
 * a schema field, as well as those fields that match a schema field with a field 
 * type that uses class solr.DateField or a sub-class, including solr.TrieDateField.
 * </p>
 * <p>
 * If all values are parseable as dates (or are already Date), then the field will
 * be mutated, replacing each value with its parsed Date equivalent; otherwise, no
 * mutation will occur.
 * </p>
 * <p>
 * One or more date "format" specifiers must be specified.  See 
 * <a href="http://joda-time.sourceforge.net/apidocs/org/joda/time/format/DateTimeFormat.html"
 * >Joda-time's DateTimeFormat javadocs</a> for a description of format strings.
 * </p>
 * <p>
 * A default time zone name or offset may optionally be specified for those dates
 * that don't include an explicit zone/offset.  NOTE: three-letter zone
 * designations like "EST" are not parseable (with the single exception of "UTC"),
 * because they are ambiguous.  If no default time zone is specified, UTC will be
 * used. See <a href="http://en.wikipedia.org/wiki/List_of_tz_database_time_zones"
 * >Wikipedia's list of TZ database time zone names</a>.
 * </p>
 * <p>
 * The locale to use when parsing field values using the specified formats may
 * optionally be specified.  If no locale is configured, then {@link Locale#ROOT}
 * will be used. The following configuration specifies the French/France locale and
 * two date formats that will parse the strings "le mardi 8 janvier 2013" and 
 * "le 28 déc. 2010 à 15 h 30", respectively.  Note that either individual &lt;str&gt;
 * elements or &lt;arr&gt;-s of &lt;str&gt; elements may be used to specify the
 * date format(s):
 * </p>
 *
 * <pre class="prettyprint">
 * &lt;processor class="solr.ParseDateFieldUpdateProcessorFactory"&gt;
 *   &lt;str name="defaultTimeZone"&gt;Europe/Paris&lt;/str&gt;
 *   &lt;str name="locale"&gt;fr_FR&lt;/str&gt;
 *   &lt;arr name="format"&gt;
 *     &lt;str&gt;'le' EEEE dd MMMM yyyy&lt;/str&gt;
 *     &lt;str&gt;'le' dd MMM. yyyy 'à' HH 'h' mm&lt;/str&gt;
 *   &lt;/arr&gt;
 * &lt;/processor&gt;</pre>
 *
 * <p>
 * See {@link Locale} for a description of acceptable language, country (optional)
 * and variant (optional) values, joined with underscore(s).
 * </p>
 */
public class ParseDateFieldUpdateProcessorFactory extends FieldMutatingUpdateProcessorFactory {
  public static final Logger log = LoggerFactory.getLogger(ParseDateFieldUpdateProcessorFactory.class);

  private static final String FORMATS_PARAM = "format";
  private static final String DEFAULT_TIME_ZONE_PARAM = "defaultTimeZone";
  private static final String LOCALE_PARAM = "locale";

  private Map<String,DateTimeFormatter> formats = new LinkedHashMap<String,DateTimeFormatter>();

  @Override
  public UpdateRequestProcessor getInstance(SolrQueryRequest req,
                                            SolrQueryResponse rsp,
                                            UpdateRequestProcessor next) {
    return new AllValuesOrNoneFieldMutatingUpdateProcessor(getSelector(), next) {
      @Override
      protected Object mutateValue(Object srcVal) {
        if (srcVal instanceof CharSequence) {
          String srcStringVal = srcVal.toString();
          for (Map.Entry<String,DateTimeFormatter> format : formats.entrySet()) {
            DateTimeFormatter parser = format.getValue();
            try {
              DateTime dateTime = parser.parseDateTime(srcStringVal);
              return dateTime.withZone(DateTimeZone.UTC).toDate();
            } catch (IllegalArgumentException e) {
              log.debug("value '{}' is not parseable with format '{}'",
                        new Object[] { srcStringVal, format.getKey() });
            }
          }
          log.debug("value '{}' was not parsed by any configured format, thus was not mutated", srcStringVal);
          return SKIP_FIELD_VALUE_LIST_SINGLETON;
        }
        if (srcVal instanceof Date) {
          return srcVal;
        }
        return SKIP_FIELD_VALUE_LIST_SINGLETON;
      }
    };
  }

  @Override
  public void init(NamedList args) {
    
    Locale locale = Locale.ROOT;
    
    String localeParam = (String)args.remove(LOCALE_PARAM);
    if (null != localeParam) {
      locale = LocaleUtils.toLocale(localeParam);
    }

    Object defaultTimeZoneParam = args.remove(DEFAULT_TIME_ZONE_PARAM);
    DateTimeZone defaultTimeZone = DateTimeZone.UTC;
    if (null != defaultTimeZoneParam) {
      defaultTimeZone = DateTimeZone.forID(defaultTimeZoneParam.toString());
    }

    Collection<String> formatsParam = oneOrMany(args, FORMATS_PARAM);
    if (null != formatsParam) {
      for (String value : formatsParam) {
        formats.put(value, DateTimeFormat.forPattern(value).withZone(defaultTimeZone).withLocale(locale));
      }
    }
    super.init(args);
  }

  /**
   * Returns true if the field doesn't match any schema field or dynamic field,
   *           or if the matched field's type is BoolField
   */
  @Override
  public FieldMutatingUpdateProcessor.FieldNameSelector
  getDefaultSelector(final SolrCore core) {

    return new FieldMutatingUpdateProcessor.FieldNameSelector() {
      @Override
      public boolean shouldMutate(final String fieldName) {
        final IndexSchema schema = core.getLatestSchema();
        FieldType type = schema.getFieldTypeNoEx(fieldName);
        return (null == type) || type instanceof DateValueFieldType;
      }
    };
  }
}
