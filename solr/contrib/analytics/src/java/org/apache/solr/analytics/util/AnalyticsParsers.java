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
package org.apache.solr.analytics.util;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;

import org.apache.solr.legacy.LegacyNumericUtils;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.TrieDateField;
import org.apache.solr.schema.TrieDoubleField;
import org.apache.solr.schema.TrieFloatField;
import org.apache.solr.schema.TrieIntField;
import org.apache.solr.schema.TrieLongField;

/** 
 * Class to hold the parsers used for Solr Analytics.
 */
public class AnalyticsParsers {

  /**
   * Returns a parser that will translate a BytesRef or long from DocValues into 
   * a String that correctly represents the value.
   * @param class1 class of the FieldType of the field being faceted on.
   * @return A Parser
   */
  public static Parser getParser(Class<? extends FieldType> class1) {
    if (class1.equals(TrieIntField.class)) {
      return AnalyticsParsers.INT_DOC_VALUES_PARSER;
    } else if (class1.equals(TrieLongField.class)) {
      return AnalyticsParsers.LONG_DOC_VALUES_PARSER;
    } else if (class1.equals(TrieFloatField.class)) {
      return AnalyticsParsers.FLOAT_DOC_VALUES_PARSER;
    } else if (class1.equals(TrieDoubleField.class)) {
      return AnalyticsParsers.DOUBLE_DOC_VALUES_PARSER;
    } else if (class1.equals(TrieDateField.class)) {
      return AnalyticsParsers.DATE_DOC_VALUES_PARSER;
    } else {
      return AnalyticsParsers.STRING_PARSER;
    }
  }

  /**
   * For use in classes that grab values by docValue.
   * Converts a BytesRef object into the correct readable text.
   */
  public static interface Parser {
    String parse(BytesRef bytes) throws IOException;
  }
  
  /**
   * Converts the long returned by NumericDocValues into the
   * correct number and return it as a string.
   */
  public static interface NumericParser extends Parser {
    String parseNum(long l);
  }
  
  /**
   * Converts the BytesRef or long to the correct int string.
   */
  public static final NumericParser INT_DOC_VALUES_PARSER = new NumericParser() {
    public String parse(BytesRef bytes) throws IOException {
      try {
        return ""+ LegacyNumericUtils.prefixCodedToInt(bytes);
      } catch (NumberFormatException e) {
        throw new IOException("The byte array "+Arrays.toString(bytes.bytes)+" cannot be converted to an int.");
      }
    }
    @Override
    public String parseNum(long l) {
      return ""+(int)l;
    }
  };
  
  /**
   * Converts the BytesRef or long to the correct long string.
   */
  public static final NumericParser LONG_DOC_VALUES_PARSER = new NumericParser() {
    public String parse(BytesRef bytes) throws IOException {
      try {
        return ""+ LegacyNumericUtils.prefixCodedToLong(bytes);
      } catch (NumberFormatException e) {
        throw new IOException("The byte array "+Arrays.toString(bytes.bytes)+" cannot be converted to a long.");
      }
    }
    @Override
    public String parseNum(long l) {
      return ""+l;
    }
  };
  
  /**
   * Converts the BytesRef or long to the correct float string.
   */
  public static final NumericParser FLOAT_DOC_VALUES_PARSER = new NumericParser() {
    public String parse(BytesRef bytes) throws IOException {
      try {
        return ""+ NumericUtils.sortableIntToFloat(LegacyNumericUtils.prefixCodedToInt(bytes));
      } catch (NumberFormatException e) {
        throw new IOException("The byte array "+Arrays.toString(bytes.bytes)+" cannot be converted to a float.");
      }
    }
    @Override
    public String parseNum(long l) {
      return ""+ NumericUtils.sortableIntToFloat((int) l);
    }
  };
  
  /**
   * Converts the BytesRef or long to the correct double string.
   */
  public static final NumericParser DOUBLE_DOC_VALUES_PARSER = new NumericParser() {
    public String parse(BytesRef bytes) throws IOException {
      try {
        return ""+ NumericUtils.sortableLongToDouble(LegacyNumericUtils.prefixCodedToLong(bytes));
      } catch (NumberFormatException e) {
        throw new IOException("The byte array "+Arrays.toString(bytes.bytes)+" cannot be converted to a double.");
      }
    }
    @Override
    public String parseNum(long l) {
      return ""+ NumericUtils.sortableLongToDouble(l);
    }
  };
  
  /**
   * Converts the BytesRef or long to the correct date string.
   */
  public static final NumericParser DATE_DOC_VALUES_PARSER = new NumericParser() {
    @SuppressWarnings("deprecation")
    public String parse(BytesRef bytes) throws IOException {
      try {
        return Instant.ofEpochMilli(LegacyNumericUtils.prefixCodedToLong(bytes)).toString();
      } catch (NumberFormatException e) {
        throw new IOException("The byte array "+Arrays.toString(bytes.bytes)+" cannot be converted to a date.");
      }
    }
    @SuppressWarnings("deprecation")
    @Override
    public String parseNum(long l) {
      return Instant.ofEpochMilli(l).toString();
    }
  };
  
  /**
   * Converts the BytesRef to the correct string.
   */
  public static final Parser STRING_PARSER = new Parser() {
    public String parse(BytesRef bytes) {
      return bytes.utf8ToString();
    }
  };
}
