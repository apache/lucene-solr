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
package org.apache.solr.analytics.statistics;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Supplier;

import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.BytesRefFieldSource;
import org.apache.lucene.queries.function.valuesource.DoubleFieldSource;
import org.apache.lucene.queries.function.valuesource.FloatFieldSource;
import org.apache.lucene.queries.function.valuesource.IntFieldSource;
import org.apache.lucene.queries.function.valuesource.LongFieldSource;
import org.apache.solr.analytics.expression.ExpressionFactory;
import org.apache.solr.analytics.request.ExpressionRequest;
import org.apache.solr.analytics.util.AnalyticsParams;
import org.apache.solr.analytics.util.valuesource.AbsoluteValueDoubleFunction;
import org.apache.solr.analytics.util.valuesource.AddDoubleFunction;
import org.apache.solr.analytics.util.valuesource.ConcatStringFunction;
import org.apache.solr.analytics.util.valuesource.ConstDateSource;
import org.apache.solr.analytics.util.valuesource.ConstDoubleSource;
import org.apache.solr.analytics.util.valuesource.ConstStringSource;
import org.apache.solr.analytics.util.valuesource.DateFieldSource;
import org.apache.solr.analytics.util.valuesource.DateMathFunction;
import org.apache.solr.analytics.util.valuesource.DivDoubleFunction;
import org.apache.solr.analytics.util.valuesource.DualDoubleFunction;
import org.apache.solr.analytics.util.valuesource.FilterFieldSource;
import org.apache.solr.analytics.util.valuesource.LogDoubleFunction;
import org.apache.solr.analytics.util.valuesource.MultiDateFunction;
import org.apache.solr.analytics.util.valuesource.MultiDoubleFunction;
import org.apache.solr.analytics.util.valuesource.MultiplyDoubleFunction;
import org.apache.solr.analytics.util.valuesource.NegateDoubleFunction;
import org.apache.solr.analytics.util.valuesource.PowDoubleFunction;
import org.apache.solr.analytics.util.valuesource.ReverseStringFunction;
import org.apache.solr.analytics.util.valuesource.SingleDoubleFunction;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.StrField;
import org.apache.solr.schema.TrieDateField;
import org.apache.solr.schema.TrieDoubleField;
import org.apache.solr.schema.TrieFloatField;
import org.apache.solr.schema.TrieIntField;
import org.apache.solr.schema.TrieLongField;
import org.apache.solr.util.DateMathParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatsCollectorSupplierFactory {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  // FunctionTypes
  final static int NUMBER_TYPE = 0;
  final static int DATE_TYPE = 1;
  final static int STRING_TYPE = 2;
  final static int FIELD_TYPE = 3;
  final static int FILTER_TYPE = 4;
  
  /**
   * Builds a Supplier that will generate identical arrays of new StatsCollectors.
   * 
   * @param schema The Schema being used.
   * @param exRequests The expression requests to generate a StatsCollector[] from.
   * @return A Supplier that will return an array of new StatsCollector.
   */
  @SuppressWarnings("unchecked")
  public static Supplier<StatsCollector[]> create(IndexSchema schema, List<ExpressionRequest> exRequests ) {
    final Map<String, Set<String>> collectorStats =  new TreeMap<>();
    final Map<String, Set<Integer>> collectorPercs =  new TreeMap<>();
    final Map<String, ValueSource> collectorSources =  new TreeMap<>();
    
    // Iterate through all expression request to make a list of ValueSource strings
    // and statistics that need to be calculated on those ValueSources.
    for (ExpressionRequest expRequest : exRequests) {
      String statExpression = expRequest.getExpressionString();
      Set<String> statistics = getStatistics(statExpression);
      if (statistics == null) {
        continue;
      }
      for (String statExp : statistics) {
        String stat;
        String operands;
        try {
          stat = statExp.substring(0, statExp.indexOf('(')).trim();
          operands = statExp.substring(statExp.indexOf('(')+1, statExp.lastIndexOf(')')).trim();
        } catch (Exception e) {
          throw new SolrException(ErrorCode.BAD_REQUEST,"Unable to parse statistic: ["+statExpression+"]",e);
        }
        String[] arguments = ExpressionFactory.getArguments(operands);
        String source = arguments[0];
        if (stat.equals(AnalyticsParams.STAT_PERCENTILE)) {
          // The statistic is a percentile, extra parsing is required
          if (arguments.length<2) {
            throw new SolrException(ErrorCode.BAD_REQUEST,"Too few arguments given for "+stat+"() in ["+statExp+"].");
          } else if (arguments.length>2) {
            throw new SolrException(ErrorCode.BAD_REQUEST,"Too many arguments given for "+stat+"() in ["+statExp+"].");
          }
          source = arguments[1];
          Set<Integer> percs = collectorPercs.get(source);
          if (percs == null) {
            percs = new HashSet<>();
            collectorPercs.put(source, percs);
          }
          try {
            int perc = Integer.parseInt(arguments[0]);
            if (perc>0 && perc<100) {
              percs.add(perc);
            } else {
              throw new SolrException(ErrorCode.BAD_REQUEST,"The percentile in ["+statExp+"] is not between 0 and 100, exculsive.");
            }
          } catch (NumberFormatException e) {
            throw new SolrException(ErrorCode.BAD_REQUEST,"\""+arguments[0]+"\" cannot be converted into a percentile.",e);
          }
        } else if (arguments.length>1) {
          throw new SolrException(ErrorCode.BAD_REQUEST,"Too many arguments given for "+stat+"() in ["+statExp+"].");
        } else if (arguments.length==0) {
          throw new SolrException(ErrorCode.BAD_REQUEST,"No arguments given for "+stat+"() in ["+statExp+"].");
        } 
        // Only unique ValueSources will be made; therefore statistics must be accumulated for
        // each ValueSource, even across different expression requests
        Set<String> stats = collectorStats.get(source);
        if (stats == null) {
          stats = new HashSet<>();
          collectorStats.put(source, stats);
        }
        if(AnalyticsParams.STAT_PERCENTILE.equals(stat)) {
          stats.add(stat + "_"+ arguments[0]);
        } else {
          stats.add(stat);
        }
      }
    }
    String[] keys = collectorStats.keySet().toArray(new String[0]);
    for (String sourceStr : keys) {
      // Build one ValueSource for each unique value source string
      ValueSource source = buildSourceTree(schema, sourceStr);
      if (source == null) {
        throw new SolrException(ErrorCode.BAD_REQUEST,"The statistic ["+sourceStr+"] could not be parsed.");
      }
      String builtString = source.toString();
      collectorSources.put(builtString,source);
      // Replace the user given string with the correctly built string
      if (!builtString.equals(sourceStr)) {
        Set<String> stats = collectorStats.remove(sourceStr);
        if (stats!=null) {
          collectorStats.put(builtString, stats);
        }
        Set<Integer> percs = collectorPercs.remove(sourceStr);
        if (percs!=null) {
          collectorPercs.put(builtString, percs);
        }
        for (ExpressionRequest er : exRequests) {
          er.setExpressionString(er.getExpressionString().replace(sourceStr, builtString));
        }
      }
    }
    if (collectorSources.size()==0) {
      return new Supplier<StatsCollector[]>() {
        @Override
        public StatsCollector[] get() {
          return new StatsCollector[0];
        }
      };
    }
    
    log.info("Stats objects: "+collectorStats.size()+" sr="+collectorSources.size()+" pr="+collectorPercs.size() );
    
    // All information is stored in final arrays so that nothing 
    // has to be computed when the Supplier's get() method is called.
    final Set<String>[] statsArr = collectorStats.values().toArray(new Set[0]);
    final ValueSource[] sourceArr = collectorSources.values().toArray(new ValueSource[0]);
    final boolean[] uniqueBools = new boolean[statsArr.length];
    final boolean[] medianBools = new boolean[statsArr.length];
    final boolean[] numericBools = new boolean[statsArr.length];
    final boolean[] dateBools = new boolean[statsArr.length];
    final double[][] percsArr = new double[statsArr.length][];
    final String[][] percsNames = new String[statsArr.length][];
    for (int count = 0; count < sourceArr.length; count++) {
      uniqueBools[count] = statsArr[count].contains(AnalyticsParams.STAT_UNIQUE);
      medianBools[count] = statsArr[count].contains(AnalyticsParams.STAT_MEDIAN);
      numericBools[count] = statsArr[count].contains(AnalyticsParams.STAT_SUM)||statsArr[count].contains(AnalyticsParams.STAT_SUM_OF_SQUARES)||statsArr[count].contains(AnalyticsParams.STAT_MEAN)||statsArr[count].contains(AnalyticsParams.STAT_STANDARD_DEVIATION);
      dateBools[count] = (sourceArr[count] instanceof DateFieldSource) | (sourceArr[count] instanceof MultiDateFunction) | (sourceArr[count] instanceof ConstDateSource);
      Set<Integer> ps = collectorPercs.get(sourceArr[count].toString());
      if (ps!=null) {
        percsArr[count] = new double[ps.size()];
        percsNames[count] = new String[ps.size()];
        int percCount = 0;
        for (int p : ps) {
          percsArr[count][percCount] = p/100.0;
          percsNames[count][percCount++] = AnalyticsParams.STAT_PERCENTILE+"_"+p;
        }
      }
    }
    // Making the Supplier
    return new Supplier<StatsCollector[]>() {
      public StatsCollector[] get() {
        StatsCollector[] collectors = new StatsCollector[statsArr.length];
        for (int count = 0; count < statsArr.length; count++) {
          if(numericBools[count]){
            StatsCollector sc = new NumericStatsCollector(sourceArr[count], statsArr[count]);
            if(uniqueBools[count]) sc = new UniqueStatsCollector(sc);
            if(medianBools[count]) sc = new MedianStatsCollector(sc);
            if(percsArr[count]!=null) sc = new PercentileStatsCollector(sc,percsArr[count],percsNames[count]);
            collectors[count]=sc;
          } else if (dateBools[count]) {
            StatsCollector sc = new MinMaxStatsCollector(sourceArr[count], statsArr[count]);
            if(uniqueBools[count]) sc = new UniqueStatsCollector(sc);
            if(medianBools[count]) sc = new DateMedianStatsCollector(sc);
            if(percsArr[count]!=null) sc = new PercentileStatsCollector(sc,percsArr[count],percsNames[count]);
           collectors[count]=sc;
          } else {
            StatsCollector sc = new MinMaxStatsCollector(sourceArr[count], statsArr[count]);
            if(uniqueBools[count]) sc = new UniqueStatsCollector(sc);
            if(medianBools[count]) sc = new MedianStatsCollector(sc);
            if(percsArr[count]!=null) sc = new PercentileStatsCollector(sc,percsArr[count],percsNames[count]);
            collectors[count]=sc;
          }
        }
        return collectors;
      }
    };
  }
  
  /**
   * Finds the set of statistics that must be computed for the expression.
   * @param expression The string representation of an expression
   * @return The set of statistics (sum, mean, median, etc.) found in the expression
   */
  public static Set<String> getStatistics(String expression) {
    HashSet<String> set = new HashSet<>();
    int firstParen = expression.indexOf('(');
    if (firstParen>0) {
      String topOperation = expression.substring(0,firstParen).trim();
      if (AnalyticsParams.ALL_STAT_SET.contains(topOperation)) {
        set.add(expression);
      } else if (!(topOperation.equals(AnalyticsParams.CONSTANT_NUMBER)||topOperation.equals(AnalyticsParams.CONSTANT_DATE)||topOperation.equals(AnalyticsParams.CONSTANT_STRING))) {
        String operands = expression.substring(firstParen+1, expression.lastIndexOf(')')).trim();
        String[] arguments = ExpressionFactory.getArguments(operands);
        for (String argument : arguments) {
          Set<String> more = getStatistics(argument);
          if (more!=null) {
            set.addAll(more);
          }
        }
      }
    }
    if (set.size()==0) {
      return null;
    }
    return set;
  }
  
  /**
   * Builds a Value Source from a given string
   * 
   * @param schema The schema being used.
   * @param expression The string to be turned into an expression.
   * @return The completed ValueSource
   */
  private static ValueSource buildSourceTree(IndexSchema schema, String expression) {
    return buildSourceTree(schema,expression,FIELD_TYPE);
  }
  
  /**
   * Builds a Value Source from a given string and a given source type
   * 
   * @param schema The schema being used.
   * @param expression The string to be turned into an expression.
   * @param sourceType The type of source that must be returned.
   * @return The completed ValueSource
   */
  private static ValueSource buildSourceTree(IndexSchema schema, String expression, int sourceType) {
    int expressionType = getSourceType(expression);
    if (sourceType != FIELD_TYPE && expressionType != FIELD_TYPE && 
        expressionType != FILTER_TYPE && expressionType != sourceType) {
      return null;
    }
    switch (expressionType) {
    case NUMBER_TYPE : return buildNumericSource(schema, expression);
    case DATE_TYPE : return buildDateSource(schema, expression);
    case STRING_TYPE : return buildStringSource(schema, expression);
    case FIELD_TYPE : return buildFieldSource(schema, expression, sourceType);
    case FILTER_TYPE : return buildFilterSource(schema, expression.substring(expression.indexOf('(')+1,expression.lastIndexOf(')')), sourceType);
    default : throw new SolrException(ErrorCode.BAD_REQUEST,expression+" is not a valid operation.");
    }
  }

  /**
   * Determines what type of value source the expression represents.
   * 
   * @param expression The expression representing the desired ValueSource
   * @return NUMBER_TYPE, DATE_TYPE, STRING_TYPE or -1
   */
  private static int getSourceType(String expression) {
    int paren = expression.indexOf('(');
    if (paren<0) {
      return FIELD_TYPE;
    }
    String operation = expression.substring(0,paren).trim();

    if (AnalyticsParams.NUMERIC_OPERATION_SET.contains(operation)) {
      return NUMBER_TYPE;
    } else if (AnalyticsParams.DATE_OPERATION_SET.contains(operation)) {
      return DATE_TYPE;
    } else if (AnalyticsParams.STRING_OPERATION_SET.contains(operation)) {
      return STRING_TYPE;
    } else if (operation.equals(AnalyticsParams.FILTER)) {
      return FILTER_TYPE;
    }
    throw new SolrException(ErrorCode.BAD_REQUEST,"The operation \""+operation+"\" in ["+expression+"] is not supported.");
  }
  
  /**
   *  Builds a value source for a given field, making sure that the field fits a given source type.
   * @param schema the schema
   * @param expressionString The name of the field to build a Field Source from.
   * @param sourceType FIELD_TYPE for any type of field, NUMBER_TYPE for numeric fields, 
   * DATE_TYPE for date fields and STRING_TYPE for string fields.
   * @return a value source
   */
  private static ValueSource buildFieldSource(IndexSchema schema, String expressionString, int sourceType) {
    SchemaField sf;
    try {
      sf = schema.getField(expressionString);
    } catch (SolrException e) {
      throw new SolrException(ErrorCode.BAD_REQUEST,"The field "+expressionString+" does not exist.",e);
    }
    FieldType type = sf.getType();
    if ( type instanceof TrieIntField) {
      if (sourceType!=NUMBER_TYPE&&sourceType!=FIELD_TYPE) {
        return null;
      }
      return new IntFieldSource(expressionString) {
        public String description() {
          return field;
        }
      };
    } else if (type instanceof TrieLongField) {
      if (sourceType!=NUMBER_TYPE&&sourceType!=FIELD_TYPE) {
        return null;
      }
      return new LongFieldSource(expressionString) {
        public String description() {
          return field;
        }
      };
    } else if (type instanceof TrieFloatField) {
      if (sourceType!=NUMBER_TYPE&&sourceType!=FIELD_TYPE) {
        return null;
      }
      return new FloatFieldSource(expressionString) {
        public String description() {
          return field;
        }
      };
    } else if (type instanceof TrieDoubleField) {
      if (sourceType!=NUMBER_TYPE&&sourceType!=FIELD_TYPE) {
        return null;
      }
      return new DoubleFieldSource(expressionString) {
        public String description() {
          return field;
        }
      };
    } else if (type instanceof TrieDateField) {
      if (sourceType!=DATE_TYPE&&sourceType!=FIELD_TYPE) {
        return null;
      }
      return new DateFieldSource(expressionString) {
        public String description() {
          return field;
        }
      };
    } else if (type instanceof StrField) {
      if (sourceType!=STRING_TYPE&&sourceType!=FIELD_TYPE) {
        return null;
      }
      return new BytesRefFieldSource(expressionString) {
        public String description() {
          return field;
        }
      };
    }
    throw new SolrException(ErrorCode.BAD_REQUEST, type.toString()+" is not a supported field type in Solr Analytics.");
  }
  
  /**
   * Builds a default is missing source that wraps a given source. A missing value is required for all 
   * non-field value sources.
   * @param schema the schema
   * @param expressionString The name of the field to build a Field Source from.
   * @param sourceType FIELD_TYPE for any type of field, NUMBER_TYPE for numeric fields, 
   * DATE_TYPE for date fields and STRING_TYPE for string fields.
   * @return a value source
   */
  @SuppressWarnings("deprecation")
  private static ValueSource buildFilterSource(IndexSchema schema, String expressionString, int sourceType) {
    String[] arguments = ExpressionFactory.getArguments(expressionString);
    if (arguments.length!=2) {
      throw new SolrException(ErrorCode.BAD_REQUEST,"Invalid arguments were given for \""+AnalyticsParams.FILTER+"\".");
    }
    ValueSource delegateSource = buildSourceTree(schema, arguments[0], sourceType);
    if (delegateSource==null) {
      return null;
    }
    Object defaultObject;

    ValueSource src = delegateSource;
    if (delegateSource instanceof FilterFieldSource) {
      src = ((FilterFieldSource)delegateSource).getRootSource();
    }
    if ( src instanceof IntFieldSource) {
      try {
        defaultObject = new Integer(arguments[1]);
      } catch (NumberFormatException e) {
        throw new SolrException(ErrorCode.BAD_REQUEST,"The filter value "+arguments[1]+" cannot be converted into an integer.",e);
      }
    } else if ( src instanceof DateFieldSource || src instanceof MultiDateFunction) {
      defaultObject = DateMathParser.parseMath(null, arguments[1]);
    } else if ( src instanceof LongFieldSource ) {
      try {
        defaultObject = new Long(arguments[1]);
      } catch (NumberFormatException e) {
        throw new SolrException(ErrorCode.BAD_REQUEST,"The filter value "+arguments[1]+" cannot be converted into a long.",e);
      }
    } else if ( src instanceof FloatFieldSource ) {
      try {
        defaultObject = new Float(arguments[1]);
      } catch (NumberFormatException e) {
        throw new SolrException(ErrorCode.BAD_REQUEST,"The filter value "+arguments[1]+" cannot be converted into a float.",e);
      }
    } else if ( src instanceof DoubleFieldSource || src instanceof SingleDoubleFunction ||
                src instanceof DualDoubleFunction|| src instanceof MultiDoubleFunction) {
      try {
        defaultObject = new Double(arguments[1]);
      } catch (NumberFormatException e) {
        throw new SolrException(ErrorCode.BAD_REQUEST,"The filter value "+arguments[1]+" cannot be converted into a double.",e);
      }
    } else {
      defaultObject = arguments[1];
    }
    return new FilterFieldSource(delegateSource,defaultObject);
  } 
  
  /**
   * Recursively parses and breaks down the expression string to build a numeric ValueSource.
   * 
   * @param schema The schema to pull fields from.
   * @param expressionString The expression string to build a ValueSource from.
   * @return The value source represented by the given expressionString
   */
  private static ValueSource buildNumericSource(IndexSchema schema, String expressionString) {
    int paren = expressionString.indexOf('(');
    String[] arguments;
    String operands;
    if (paren<0) {
      return buildFieldSource(schema,expressionString,NUMBER_TYPE);
    } else {
      try {
        operands = expressionString.substring(paren+1, expressionString.lastIndexOf(')')).trim();
      } catch (Exception e) {
        throw new SolrException(ErrorCode.BAD_REQUEST,"Missing closing parenthesis in ["+expressionString+"]");
      }
      arguments = ExpressionFactory.getArguments(operands);
    }
    String operation = expressionString.substring(0, paren).trim();
    if (operation.equals(AnalyticsParams.CONSTANT_NUMBER)) {
      if (arguments.length!=1) {
        throw new SolrException(ErrorCode.BAD_REQUEST,"The constant number declaration ["+expressionString+"] does not have exactly 1 argument.");
      }
      return new ConstDoubleSource(Double.parseDouble(arguments[0]));
    } else if (operation.equals(AnalyticsParams.NEGATE)) {
      if (arguments.length!=1) {
        throw new SolrException(ErrorCode.BAD_REQUEST,"The negate operation ["+expressionString+"] does not have exactly 1 argument.");
      }
      ValueSource argSource = buildNumericSource(schema, arguments[0]);
      if (argSource==null) {
        throw new SolrException(ErrorCode.BAD_REQUEST,"The operation \""+AnalyticsParams.NEGATE+"\" requires a numeric field or operation as argument. \""+arguments[0]+"\" is not a numeric field or operation.");
      }
      return new NegateDoubleFunction(argSource);
    }  else if (operation.equals(AnalyticsParams.ABSOLUTE_VALUE)) {
      if (arguments.length!=1) {
        throw new SolrException(ErrorCode.BAD_REQUEST,"The absolute value operation ["+expressionString+"] does not have exactly 1 argument.");
      }
      ValueSource argSource = buildNumericSource(schema, arguments[0]);
      if (argSource==null) {
        throw new SolrException(ErrorCode.BAD_REQUEST,"The operation \""+AnalyticsParams.NEGATE+"\" requires a numeric field or operation as argument. \""+arguments[0]+"\" is not a numeric field or operation.");
      }
      return new AbsoluteValueDoubleFunction(argSource);
    } else if (operation.equals(AnalyticsParams.FILTER)) {
      return buildFilterSource(schema, operands, NUMBER_TYPE);
    }
    List<ValueSource> subExpressions = new ArrayList<>();
    for (String argument : arguments) {
      ValueSource argSource = buildNumericSource(schema, argument);
      if (argSource == null) {
        throw new SolrException(ErrorCode.BAD_REQUEST,"The operation \""+operation+"\" requires numeric fields or operations as arguments. \""+argument+"\" is not a numeric field or operation.");
      }
      subExpressions.add(argSource);
    }
    if (operation.equals(AnalyticsParams.ADD)) {
      return new AddDoubleFunction(subExpressions.toArray(new ValueSource[0]));
    } else if (operation.equals(AnalyticsParams.MULTIPLY)) {
      return new MultiplyDoubleFunction(subExpressions.toArray(new ValueSource[0]));
    } else if (operation.equals(AnalyticsParams.DIVIDE)) {
      if (subExpressions.size()!=2) {
        throw new SolrException(ErrorCode.BAD_REQUEST,"The divide operation ["+expressionString+"] does not have exactly 2 arguments.");
      }
      return new DivDoubleFunction(subExpressions.get(0),subExpressions.get(1));
    } else if (operation.equals(AnalyticsParams.POWER)) {
      if (subExpressions.size()!=2) {
        throw new SolrException(ErrorCode.BAD_REQUEST,"The power operation ["+expressionString+"] does not have exactly 2 arguments.");
      }
      return new PowDoubleFunction(subExpressions.get(0),subExpressions.get(1));
    } else if (operation.equals(AnalyticsParams.LOG)) {
      if (subExpressions.size()!=2) {
        throw new SolrException(ErrorCode.BAD_REQUEST,"The log operation ["+expressionString+"] does not have exactly 2 arguments.");
      }
      return new LogDoubleFunction(subExpressions.get(0), subExpressions.get(1));
    } 
    if (AnalyticsParams.DATE_OPERATION_SET.contains(operation)||AnalyticsParams.STRING_OPERATION_SET.contains(operation)) {
      return null;
    }
    throw new SolrException(ErrorCode.BAD_REQUEST,"The operation ["+expressionString+"] is not supported.");
  }

  
  /**
   * Recursively parses and breaks down the expression string to build a date ValueSource.
   * 
   * @param schema The schema to pull fields from.
   * @param expressionString The expression string to build a ValueSource from.
   * @return The value source represented by the given expressionString
   */
  @SuppressWarnings("deprecation")
  private static ValueSource buildDateSource(IndexSchema schema, String expressionString) {
    int paren = expressionString.indexOf('(');
    String[] arguments;
    if (paren<0) {
      return buildFieldSource(schema, expressionString, DATE_TYPE);
    } else {
      arguments = ExpressionFactory.getArguments(expressionString.substring(paren+1, expressionString.lastIndexOf(')')).trim());
    }
    String operands = arguments[0];
    String operation = expressionString.substring(0, paren).trim();
    if (operation.equals(AnalyticsParams.CONSTANT_DATE)) {
      if (arguments.length!=1) {
        throw new SolrException(ErrorCode.BAD_REQUEST,"The constant date declaration ["+expressionString+"] does not have exactly 1 argument.");
      }
      return new ConstDateSource(DateMathParser.parseMath(null, operands));
    } else if (operation.equals(AnalyticsParams.FILTER)) {
      return buildFilterSource(schema, operands, DATE_TYPE);
    }
    if (operation.equals(AnalyticsParams.DATE_MATH)) {
      List<ValueSource> subExpressions = new ArrayList<>();
      boolean first = true;
      for (String argument : arguments) {
        ValueSource argSource;
        if (first) {
          first = false;
          argSource = buildDateSource(schema, argument);
          if (argSource == null) {
            throw new SolrException(ErrorCode.BAD_REQUEST,"\""+AnalyticsParams.DATE_MATH+"\" requires the first argument be a date operation or field. ["+argument+"] is not a date operation or field.");
          }
        } else {
          argSource = buildStringSource(schema, argument);
          if (argSource == null) {
            throw new SolrException(ErrorCode.BAD_REQUEST,"\""+AnalyticsParams.DATE_MATH+"\" requires that all arguments except the first be string operations. ["+argument+"] is not a string operation.");
          }
        }
        subExpressions.add(argSource);
      }
      return new DateMathFunction(subExpressions.toArray(new ValueSource[0]));
    }
    if (AnalyticsParams.NUMERIC_OPERATION_SET.contains(operation)||AnalyticsParams.STRING_OPERATION_SET.contains(operation)) {
      return null;
    }
    throw new SolrException(ErrorCode.BAD_REQUEST,"The operation ["+expressionString+"] is not supported.");
  }

  
  /**
   * Recursively parses and breaks down the expression string to build a string ValueSource.
   * 
   * @param schema The schema to pull fields from.
   * @param expressionString The expression string to build a ValueSource from.
   * @return The value source represented by the given expressionString
   */
  private static ValueSource buildStringSource(IndexSchema schema, String expressionString) {
    int paren = expressionString.indexOf('(');
    String[] arguments;
    if (paren<0) {
      return buildFieldSource(schema, expressionString, FIELD_TYPE);
    } else {
      arguments = ExpressionFactory.getArguments(expressionString.substring(paren+1, expressionString.lastIndexOf(')')).trim());
    }
    String operands = arguments[0];
    String operation = expressionString.substring(0, paren).trim();
    if (operation.equals(AnalyticsParams.CONSTANT_STRING)) {
      operands = expressionString.substring(paren+1, expressionString.lastIndexOf(')'));
      return new ConstStringSource(operands);
    } else if (operation.equals(AnalyticsParams.FILTER)) {
      return buildFilterSource(schema,operands,FIELD_TYPE);
    } else if (operation.equals(AnalyticsParams.REVERSE)) {
      if (arguments.length!=1) {
        throw new SolrException(ErrorCode.BAD_REQUEST,"\""+AnalyticsParams.REVERSE+"\" requires exactly one argument. The number of arguments in "+expressionString+" is not 1.");
      }
      return new ReverseStringFunction(buildStringSource(schema, operands));
    }
    List<ValueSource> subExpressions = new ArrayList<>();
    for (String argument : arguments) {
      subExpressions.add(buildSourceTree(schema, argument));
    }
    if (operation.equals(AnalyticsParams.CONCATENATE)) {
      return new ConcatStringFunction(subExpressions.toArray(new ValueSource[0]));
    } 
    if (AnalyticsParams.NUMERIC_OPERATION_SET.contains(operation)) {
      return buildNumericSource(schema, expressionString);
    } else if (AnalyticsParams.DATE_OPERATION_SET.contains(operation)) {
      return buildDateSource(schema, expressionString);
    }
    throw new SolrException(ErrorCode.BAD_REQUEST,"The operation ["+expressionString+"] is not supported.");
  }
}
