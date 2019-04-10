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

package org.apache.lucene.luke.models.search;

import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.stream.Collectors;

import org.apache.lucene.document.DateTools;

/**
 * Configurations for query parser.
 */
public final class QueryParserConfig {

  /** query operators */
  public enum Operator {
    AND, OR
  }

  private final boolean useClassicParser;

  private final boolean enablePositionIncrements;

  private final boolean allowLeadingWildcard;

  private final DateTools.Resolution dateResolution;

  private final Operator defaultOperator;

  private final float fuzzyMinSim;

  private final int fuzzyPrefixLength;

  private final Locale locale;

  private final TimeZone timeZone;

  private final int phraseSlop;

  // classic parser only configurations
  private final boolean autoGenerateMultiTermSynonymsPhraseQuery;

  private final boolean autoGeneratePhraseQueries;

  private final boolean splitOnWhitespace;

  // standard parser only configurations
  private final Map<String, Class<? extends Number>> typeMap;

  /** Builder for {@link QueryParserConfig} */
  public static class Builder {
    private boolean useClassicParser = true;
    private boolean enablePositionIncrements = true;
    private boolean allowLeadingWildcard = false;
    private DateTools.Resolution dateResolution = DateTools.Resolution.MILLISECOND;
    private Operator defaultOperator = Operator.OR;
    private float fuzzyMinSim = 2f;
    private int fuzzyPrefixLength = 0;
    private Locale locale = Locale.getDefault();
    private TimeZone timeZone = TimeZone.getDefault();
    private int phraseSlop = 0;
    private boolean autoGenerateMultiTermSynonymsPhraseQuery = false;
    private boolean autoGeneratePhraseQueries = false;
    private boolean splitOnWhitespace = false;
    private Map<String, Class<? extends Number>> typeMap = new HashMap<>();

    /** Builder for {@link QueryParserConfig} */
    public Builder useClassicParser(boolean value) {
      useClassicParser = value;
      return this;
    }

    public Builder enablePositionIncrements(boolean value) {
      enablePositionIncrements = value;
      return this;
    }

    public Builder allowLeadingWildcard(boolean value) {
      allowLeadingWildcard = value;
      return this;
    }

    public Builder dateResolution(DateTools.Resolution value) {
      dateResolution = value;
      return this;
    }

    public Builder defaultOperator(Operator op) {
      defaultOperator = op;
      return this;
    }

    public Builder fuzzyMinSim(float val) {
      fuzzyMinSim = val;
      return this;
    }

    public Builder fuzzyPrefixLength(int val) {
      fuzzyPrefixLength = val;
      return this;
    }

    public Builder locale(Locale val) {
      locale = val;
      return this;
    }

    public Builder timeZone(TimeZone val) {
      timeZone = val;
      return this;
    }

    public Builder phraseSlop(int val) {
      phraseSlop = val;
      return this;
    }

    public Builder autoGenerateMultiTermSynonymsPhraseQuery(boolean val) {
      autoGenerateMultiTermSynonymsPhraseQuery = val;
      return this;
    }

    public Builder autoGeneratePhraseQueries(boolean val) {
      autoGeneratePhraseQueries = val;
      return this;
    }

    public Builder splitOnWhitespace(boolean val) {
      splitOnWhitespace = val;
      return this;
    }

    public Builder typeMap(Map<String, Class<? extends Number>> val) {
      typeMap = val;
      return this;
    }

    public QueryParserConfig build() {
      return new QueryParserConfig(this);
    }
  }

  private QueryParserConfig(Builder builder) {
    this.useClassicParser = builder.useClassicParser;
    this.enablePositionIncrements = builder.enablePositionIncrements;
    this.allowLeadingWildcard = builder.allowLeadingWildcard;
    this.dateResolution = builder.dateResolution;
    this.defaultOperator = builder.defaultOperator;
    this.fuzzyMinSim = builder.fuzzyMinSim;
    this.fuzzyPrefixLength = builder.fuzzyPrefixLength;
    this.locale = builder.locale;
    this.timeZone = builder.timeZone;
    this.phraseSlop = builder.phraseSlop;
    this.autoGenerateMultiTermSynonymsPhraseQuery = builder.autoGenerateMultiTermSynonymsPhraseQuery;
    this.autoGeneratePhraseQueries = builder.autoGeneratePhraseQueries;
    this.splitOnWhitespace = builder.splitOnWhitespace;
    this.typeMap = Collections.unmodifiableMap(builder.typeMap);
  }

  public boolean isUseClassicParser() {
    return useClassicParser;
  }

  public boolean isAutoGenerateMultiTermSynonymsPhraseQuery() {
    return autoGenerateMultiTermSynonymsPhraseQuery;
  }

  public boolean isEnablePositionIncrements() {
    return enablePositionIncrements;
  }

  public boolean isAllowLeadingWildcard() {
    return allowLeadingWildcard;
  }

  public boolean isAutoGeneratePhraseQueries() {
    return autoGeneratePhraseQueries;
  }

  public boolean isSplitOnWhitespace() {
    return splitOnWhitespace;
  }

  public DateTools.Resolution getDateResolution() {
    return dateResolution;
  }

  public Operator getDefaultOperator() {
    return defaultOperator;
  }

  public float getFuzzyMinSim() {
    return fuzzyMinSim;
  }

  public int getFuzzyPrefixLength() {
    return fuzzyPrefixLength;
  }

  public Locale getLocale() {
    return locale;
  }

  public TimeZone getTimeZone() {
    return timeZone;
  }

  public int getPhraseSlop() {
    return phraseSlop;
  }

  public Map<String, Class<? extends Number>> getTypeMap() {
    return typeMap;
  }

  @Override
  public String toString() {
    return "QueryParserConfig: [" +
        " default operator=" + defaultOperator.name() + ";" +
        " enable position increment=" + enablePositionIncrements + ";" +
        " allow leading wildcard=" + allowLeadingWildcard + ";" +
        " split whitespace=" + splitOnWhitespace + ";" +
        " generate phrase query=" + autoGeneratePhraseQueries + ";" +
        " generate multiterm sysnonymsphrase query=" + autoGenerateMultiTermSynonymsPhraseQuery + ";" +
        " phrase slop=" + phraseSlop + ";" +
        " date resolution=" + dateResolution.name() +
        " locale=" + locale.toLanguageTag() + ";" +
        " time zone=" + timeZone.getID() + ";" +
        " numeric types=" + String.join(",", getTypeMap().entrySet().stream()
        .map(e -> e.getKey() + "=" + e.getValue().toString()).collect(Collectors.toSet())) + ";" +
        "]";
  }
}
