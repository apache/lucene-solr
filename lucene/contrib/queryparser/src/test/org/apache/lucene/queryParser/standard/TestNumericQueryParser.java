package org.apache.lucene.queryParser.standard;

/**
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

import java.io.IOException;
import java.text.DateFormat;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.TimeZone;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.queryParser.core.QueryNodeException;
import org.apache.lucene.queryParser.core.parser.EscapeQuerySyntax;
import org.apache.lucene.queryParser.standard.config.NumberDateFormat;
import org.apache.lucene.queryParser.standard.config.NumericConfig;
import org.apache.lucene.queryParser.standard.parser.EscapeQuerySyntaxImpl;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestNumericQueryParser extends LuceneTestCase {
  
  private static enum NumberType {
    NEGATIVE, ZERO, POSITIVE;
  }
  
  final private static int[] DATE_STYLES = {DateFormat.FULL, DateFormat.LONG,
      DateFormat.MEDIUM, DateFormat.SHORT};
  
  final private static int PRECISION_STEP = 8;
  final private static String FIELD_NAME = "field";
  private static Locale LOCALE;
  private static TimeZone TIMEZONE;
  private static Map<String,Number> RANDOM_NUMBER_MAP;
  final private static EscapeQuerySyntax ESCAPER = new EscapeQuerySyntaxImpl();
  final private static String DATE_FIELD_NAME = "date";
  private static int DATE_STYLE;
  private static int TIME_STYLE;
  
  private static Analyzer ANALYZER;
  
  private static NumberFormat NUMBER_FORMAT;
  
  private static StandardQueryParser qp;
  
  private static NumberDateFormat DATE_FORMAT;
  
  static void init() {
    try {
      LOCALE = randomLocale(random);
      LOCALE = Locale.getDefault();
      TIMEZONE = randomTimeZone(random);
      DATE_STYLE = randomDateStyle(random);
      TIME_STYLE = randomDateStyle(random);
      ANALYZER = new MockAnalyzer(random);
      qp = new StandardQueryParser(ANALYZER);
      NUMBER_FORMAT = NumberFormat.getNumberInstance(LOCALE);
      NUMBER_FORMAT.setMaximumFractionDigits((random.nextInt() & 20) + 1);
      NUMBER_FORMAT.setMinimumFractionDigits((random.nextInt() & 20) + 1);
      NUMBER_FORMAT.setMaximumIntegerDigits((random.nextInt() & 20) + 1);
      NUMBER_FORMAT.setMinimumIntegerDigits((random.nextInt() & 20) + 4); // the loop checks for < 1000, this is a must!
      
      // assumes localized date pattern will have at least year, month, day, hour, minute
      SimpleDateFormat dateFormat = (SimpleDateFormat) DateFormat.getDateTimeInstance(
          DATE_STYLE, TIME_STYLE, LOCALE);
      
      // not all date patterns includes era, full year, timezone and second, so we add them here
      dateFormat.applyPattern(dateFormat.toPattern() + " G s Z yyyy");
      dateFormat.setTimeZone(TIMEZONE);
      DATE_FORMAT  = new NumberDateFormat(dateFormat);
      
      HashMap<String,Number> randomNumberMap = new HashMap<String,Number>();
      
      double randomDouble;
      long randomLong;
      int randomInt;
      float randomFloat;
      long randomDate;
      
      while ((randomLong = normalizeNumber(Math.abs(random.nextLong()))
          .longValue()) == 0)
        ;
      while ((randomDouble = normalizeNumber(Math.abs(random.nextDouble()))
          .doubleValue()) == 0)
        ;
      while ((randomFloat = normalizeNumber(Math.abs(random.nextFloat()))
          .floatValue()) == 0)
        ;
      while ((randomInt = normalizeNumber(Math.abs(random.nextInt()))
          .intValue()) == 0)
        ;
      
      // make sure random date is at least one second from 0
      while ((randomDate = normalizeNumber(Math.abs(random.nextLong()))
          .longValue()) < 1000)
        ;

      // prune date value so it doesn't pass in insane values to some calendars.
      randomDate = randomDate % 3400000000000l;

      // truncate to second
      randomDate = (randomDate / 1000) * 1000;
      
      randomNumberMap.put(NumericField.DataType.LONG.name(), randomLong);
      randomNumberMap.put(NumericField.DataType.INT.name(), randomInt);
      randomNumberMap.put(NumericField.DataType.FLOAT.name(), randomFloat);
      randomNumberMap.put(NumericField.DataType.DOUBLE.name(), randomDouble);
      randomNumberMap.put(DATE_FIELD_NAME, randomDate);
      
      RANDOM_NUMBER_MAP = Collections.unmodifiableMap(randomNumberMap);
      
    } catch (ParseException e) {
      throw new RuntimeException(e);
    }
  }
  
  private static Directory directory = null;
  private static IndexReader reader = null;
  private static IndexSearcher searcher = null;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    init();
    directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random, directory,
        newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random))
            .setMaxBufferedDocs(_TestUtil.nextInt(random, 50, 1000))
            .setMergePolicy(newLogMergePolicy()));
    
    Document doc = new Document();
    HashMap<String,NumericConfig> numericConfigMap = new HashMap<String,NumericConfig>();
    HashMap<String,NumericField> numericFieldMap = new HashMap<String,NumericField>();
    qp.setNumericConfigMap(numericConfigMap);
    
    for (NumericField.DataType type : NumericField.DataType.values()) {
      numericConfigMap.put(type.name(), new NumericConfig(PRECISION_STEP,
          NUMBER_FORMAT, type));
      
      NumericField field = new NumericField(type.name(), PRECISION_STEP,
          Field.Store.YES, true);
      
      numericFieldMap.put(type.name(), field);
      doc.add(field);
      
    }
    
    numericConfigMap.put(DATE_FIELD_NAME, new NumericConfig(PRECISION_STEP,
        DATE_FORMAT, NumericField.DataType.LONG));
    NumericField dateField = new NumericField(DATE_FIELD_NAME, PRECISION_STEP,
        Field.Store.YES, true);
    numericFieldMap.put(DATE_FIELD_NAME, dateField);
    doc.add(dateField);
    
    for (NumberType numberType : NumberType.values()) {
      setFieldValues(numberType, numericFieldMap);
      if (VERBOSE) System.out.println("Indexing document: " + doc);
      writer.addDocument(doc);
    }
    
    reader = writer.getReader();
    searcher = newSearcher(reader);
    writer.close();
    

//  SimpleDateFormat df = new SimpleDateFormat( 
//      "yyyy.MM.dd G 'at' HH:mm:ss z", LOCALE.ENGLISH);
// assumes localized date pattern will have at least year, month, day, hour, minute
  SimpleDateFormat df = (SimpleDateFormat) DateFormat.getDateTimeInstance(
      randomDateStyle(random), randomDateStyle(random), LOCALE.ENGLISH);
  System.out.println(df.toPattern());
  // most of date pattern do not include era, so we add it here. Also,
  // sometimes second is not available, we make sure it's present too
  df.applyPattern(df.toPattern() + " G s Z yyyy");
  df.setTimeZone(TIMEZONE);
  System.out.println(TIMEZONE);
  System.out.println(TIMEZONE);
  System.out.println(TIMEZONE);
  long l1 = 0;
  long l2 = -30000;
  String d1 = df.format(new Date(l1));
  String d2 = df.format(new Date(l2));
  long newL1 = df.parse(d1).getTime();
  long newL2 = df.parse(d2).getTime();
  
  System.out.println(l1 + " => " + d1 + " => " + newL1);
  System.out.println(l2 + " => " + d2 + " => " + newL2);
  
   
  }
  
  private static Number getNumberType(NumberType numberType, String fieldName) {
    
    switch (numberType) {
      
      case POSITIVE:
        return RANDOM_NUMBER_MAP.get(fieldName);
        
      case NEGATIVE:
        Number number = RANDOM_NUMBER_MAP.get(fieldName);
        
        if (NumericField.DataType.LONG.name().equals(fieldName)
            || DATE_FIELD_NAME.equals(fieldName)) {
          number = -number.longValue();
          
        } else if (NumericField.DataType.DOUBLE.name().equals(fieldName)) {
          number = -number.doubleValue();
          
        } else if (NumericField.DataType.FLOAT.name().equals(fieldName)) {
          number = -number.floatValue();
          
        } else if (NumericField.DataType.INT.name().equals(fieldName)) {
          number = -number.intValue();
          
        } else {
          throw new IllegalArgumentException("field name not found: "
              + fieldName);
        }
        
        return number;
        
      default:
        return 0;
    }
    
  }
  
  private static void setFieldValues(NumberType numberType,
      HashMap<String,NumericField> numericFieldMap) {
    
    Number number = getNumberType(numberType, NumericField.DataType.DOUBLE
        .name());
    numericFieldMap.get(NumericField.DataType.DOUBLE.name()).setDoubleValue(
        number.doubleValue());
    
    number = getNumberType(numberType, NumericField.DataType.INT.name());
    numericFieldMap.get(NumericField.DataType.INT.name()).setIntValue(
        number.intValue());
    
    number = getNumberType(numberType, NumericField.DataType.LONG.name());
    numericFieldMap.get(NumericField.DataType.LONG.name()).setLongValue(
        number.longValue());
    
    number = getNumberType(numberType, NumericField.DataType.FLOAT.name());
    numericFieldMap.get(NumericField.DataType.FLOAT.name()).setFloatValue(
        number.floatValue());
    
    number = getNumberType(numberType, DATE_FIELD_NAME);
    numericFieldMap.get(DATE_FIELD_NAME).setLongValue(number.longValue());
    
  }
  
  private static int randomDateStyle(Random random) {
    return DATE_STYLES[random.nextInt(DATE_STYLES.length)];
  }
  
  @Test
  public void testInclusiveNumericRange() throws Exception {
    assertRangeQuery(NumberType.ZERO, NumberType.ZERO, true, true, 1);
    assertRangeQuery(NumberType.ZERO, NumberType.POSITIVE, true, true, 2);
    assertRangeQuery(NumberType.NEGATIVE, NumberType.ZERO, true, true, 2);
    assertRangeQuery(NumberType.NEGATIVE, NumberType.POSITIVE, true, true, 3);
    assertRangeQuery(NumberType.NEGATIVE, NumberType.NEGATIVE, true, true, 1);
  }
  
  // @Test
  // test disabled since standard syntax parser does not work with inclusive and
  // exclusive at the same time
//   public void testInclusiveLowerNumericRange() throws Exception {
//   assertRangeQuery(NumberType.NEGATIVE, NumberType.ZERO, true, false, 1);
//   assertRangeQuery(NumberType.ZERO, NumberType.POSITIVE, true, false, 1);
//   assertRangeQuery(NumberType.NEGATIVE, NumberType.POSITIVE, true, false, 2);
//   assertRangeQuery(NumberType.NEGATIVE, NumberType.NEGATIVE, true, false, 1);
//   }
  
  // @Test
  // test disabled since standard syntax parser does not work with inclusive and
  // exclusive at the same time
//   public void testInclusiveUpperNumericRange() throws Exception {
//     assertRangeQuery(NumberType.NEGATIVE, NumberType.ZERO, false, true, 1);
//     assertRangeQuery(NumberType.ZERO, NumberType.POSITIVE, false, true, 1);
//     assertRangeQuery(NumberType.NEGATIVE, NumberType.POSITIVE, false, true, 2);
//     assertRangeQuery(NumberType.NEGATIVE, NumberType.NEGATIVE, false, true, 1);
//   }
  
  @Test
  public void testExclusiveNumericRange() throws Exception {
    assertRangeQuery(NumberType.ZERO, NumberType.ZERO, false, false, 0);
    assertRangeQuery(NumberType.ZERO, NumberType.POSITIVE, false, false, 0);
    assertRangeQuery(NumberType.NEGATIVE, NumberType.ZERO, false, false, 0);
    assertRangeQuery(NumberType.NEGATIVE, NumberType.POSITIVE, false, false, 1);
    assertRangeQuery(NumberType.NEGATIVE, NumberType.NEGATIVE, false, false, 0);
  }
  
  @Test
  public void testSimpleNumericQuery() throws Exception {
    assertSimpleQuery(NumberType.ZERO, 1);
    assertSimpleQuery(NumberType.POSITIVE, 1);
    assertSimpleQuery(NumberType.NEGATIVE, 1);
  }
  
  public void assertRangeQuery(NumberType lowerType, NumberType upperType,
      boolean upperInclusive, boolean lowerInclusive, int expectedDocCount)
      throws QueryNodeException, IOException {
    
    StringBuilder sb = new StringBuilder();
    
    String lowerInclusiveStr = (lowerInclusive ? "[" : "{");
    String upperInclusiveStr = (upperInclusive ? "]" : "}");
    
    for (NumericField.DataType type : NumericField.DataType.values()) {
      String lowerStr = numberToString(getNumberType(lowerType, type.name()));
      String upperStr = numberToString(getNumberType(upperType, type.name()));
      
      sb.append("+").append(type.name()).append(':').append(lowerInclusiveStr)
          .append('"').append(lowerStr).append("\" TO \"").append(upperStr)
          .append('"').append(upperInclusiveStr).append(' ');
    }
    
    String lowerDateStr = ESCAPER.escape(
        DATE_FORMAT.format(new Date(getNumberType(lowerType, DATE_FIELD_NAME)
            .longValue())), LOCALE, EscapeQuerySyntax.Type.STRING).toString();
    
    String upperDateStr = ESCAPER.escape(
        DATE_FORMAT.format(new Date(getNumberType(upperType, DATE_FIELD_NAME)
            .longValue())), LOCALE, EscapeQuerySyntax.Type.STRING).toString();
    
    sb.append("+").append(DATE_FIELD_NAME).append(':')
        .append(lowerInclusiveStr).append('"').append(lowerDateStr).append(
            "\" TO \"").append(upperDateStr).append('"').append(
            upperInclusiveStr);
    
    testQuery(sb.toString(), expectedDocCount);
    
  }
  
  public void assertSimpleQuery(NumberType numberType, int expectedDocCount)
      throws QueryNodeException, IOException {
    StringBuilder sb = new StringBuilder();
    
    for (NumericField.DataType type : NumericField.DataType.values()) {
      String numberStr = numberToString(getNumberType(numberType, type.name()));
      sb.append('+').append(type.name()).append(":\"").append(numberStr)
          .append("\" ");
    }
    
    String dateStr = ESCAPER.escape(
        DATE_FORMAT.format(new Date(getNumberType(numberType, DATE_FIELD_NAME)
            .longValue())), LOCALE, EscapeQuerySyntax.Type.STRING).toString();
    
    sb.append('+').append(DATE_FIELD_NAME).append(":\"").append(dateStr)
        .append('"');
    
    testQuery(sb.toString(), expectedDocCount);
    
  }
  
  private void testQuery(String queryStr, int expectedDocCount)
      throws QueryNodeException, IOException {
    if (VERBOSE) System.out.println("Parsing: " + queryStr);
    
    Query query = qp.parse(queryStr, FIELD_NAME);
    if (VERBOSE) System.out.println("Querying: " + query);
    TopDocs topDocs = searcher.search(query, 1000);
    
    String msg = "Query <" + queryStr + "> retrieved " + topDocs.totalHits
        + " document(s), " + expectedDocCount + " document(s) expected.";
    
    if (VERBOSE) System.out.println(msg);
    
    assertEquals(msg, expectedDocCount, topDocs.totalHits);
    
  }
  
  private static String numberToString(Number number) {
    return ESCAPER.escape(NUMBER_FORMAT.format(number), LOCALE,
        EscapeQuerySyntax.Type.STRING).toString();
  }
  
  private static Number normalizeNumber(Number number) throws ParseException {
    return NUMBER_FORMAT.parse(NUMBER_FORMAT.format(number));
  }
  
  @AfterClass
  public static void afterClass() throws Exception {
    searcher.close();
    searcher = null;
    reader.close();
    reader = null;
    directory.close();
    directory = null;
  }
  
}
