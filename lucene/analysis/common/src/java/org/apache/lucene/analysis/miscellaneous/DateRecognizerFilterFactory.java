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
package org.apache.lucene.analysis.miscellaneous;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.Map;
import org.apache.lucene.analysis.TokenFilterFactory;
import org.apache.lucene.analysis.TokenStream;

/**
 * Factory for {@link DateRecognizerFilter}.
 *
 * <pre class="prettyprint">
 * &lt;fieldType name="text_filter_none_date" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.WhitespaceTokenizerFactory"/&gt;
 *     &lt;filter class="solr.DateRecognizerFilterFactory" datePattern="yyyy/mm/dd" locale="en-US" /&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;
 * </pre>
 *
 * <p>The {@code datePattern} is optional. If omitted, {@link DateRecognizerFilter} will be created
 * with the default date format of the system. The {@code locale} is optional and if omitted the
 * filter will be created with {@link Locale#ENGLISH}.
 *
 * @since 5.5.0
 * @lucene.spi {@value #NAME}
 */
public class DateRecognizerFilterFactory extends TokenFilterFactory {

  /** SPI name */
  public static final String NAME = "dateRecognizer";

  public static final String DATE_PATTERN = "datePattern";
  public static final String LOCALE = "locale";

  private final DateFormat dateFormat;
  private final Locale locale;

  /** Creates a new FingerprintFilterFactory */
  public DateRecognizerFilterFactory(Map<String, String> args) {
    super(args);
    this.locale = getLocale(get(args, LOCALE));
    this.dateFormat = getDataFormat(get(args, DATE_PATTERN));
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }

  /** Default ctor for compatibility with SPI */
  public DateRecognizerFilterFactory() {
    throw defaultCtorException();
  }

  @Override
  public TokenStream create(TokenStream input) {
    return new DateRecognizerFilter(input, dateFormat);
  }

  private Locale getLocale(String localeStr) {
    if (localeStr == null) {
      return Locale.ENGLISH;
    } else {
      return new Locale.Builder().setLanguageTag(localeStr).build();
    }
  }

  public DateFormat getDataFormat(String datePattern) {
    if (datePattern != null) {
      return new SimpleDateFormat(datePattern, locale);
    } else {
      return DateFormat.getDateInstance(DateFormat.DEFAULT, locale);
    }
  }
}
