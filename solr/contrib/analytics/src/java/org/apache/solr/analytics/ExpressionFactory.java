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
package org.apache.solr.analytics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.solr.analytics.function.MergingReductionCollectionManager;
import org.apache.solr.analytics.function.ReductionCollectionManager;
import org.apache.solr.analytics.function.mapping.*;
import org.apache.solr.analytics.function.mapping.ComparisonFunction.GTEFunction;
import org.apache.solr.analytics.function.mapping.ComparisonFunction.GTFunction;
import org.apache.solr.analytics.function.mapping.ComparisonFunction.LTEFunction;
import org.apache.solr.analytics.function.mapping.ComparisonFunction.LTFunction;
import org.apache.solr.analytics.function.mapping.ConcatFunction.SeparatedConcatFunction;
import org.apache.solr.analytics.function.mapping.DecimalNumericConversionFunction.CeilingFunction;
import org.apache.solr.analytics.function.mapping.DecimalNumericConversionFunction.FloorFunction;
import org.apache.solr.analytics.function.mapping.DecimalNumericConversionFunction.RoundFunction;
import org.apache.solr.analytics.function.mapping.LogicFunction.AndFunction;
import org.apache.solr.analytics.function.mapping.LogicFunction.OrFunction;
import org.apache.solr.analytics.function.reduction.*;
import org.apache.solr.analytics.function.reduction.data.ReductionDataCollector;
import org.apache.solr.analytics.value.*;
import org.apache.solr.analytics.value.AnalyticsValueStream.ExpressionType;
import org.apache.solr.analytics.value.constant.ConstantValue;
import org.apache.solr.analytics.function.field.*;
import org.apache.solr.analytics.function.ReductionFunction;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.schema.BoolField;
import org.apache.solr.schema.DatePointField;
import org.apache.solr.schema.DoublePointField;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.FloatPointField;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.IntPointField;
import org.apache.solr.schema.LongPointField;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.StrField;
import org.apache.solr.schema.TrieDateField;
import org.apache.solr.schema.TrieDoubleField;
import org.apache.solr.schema.TrieFloatField;
import org.apache.solr.schema.TrieIntField;
import org.apache.solr.schema.TrieLongField;

/**
 * A factory to parse and create expressions, and capture information about those expressions along the way.
 *
 * <p>
 * In order to use, first call {@link #startRequest()} and create all ungrouped expressions,
 * then call {@link #createReductionManager} to get the ungrouped reduction manager.
 * <br>
 * Then for each grouping call {@link #startGrouping()} first then create all expressions within that grouping,
 * finally calling {@link #createGroupingReductionManager}  to get the reduction manager for that grouping.
 */
public class ExpressionFactory {
  private static final Pattern functionNamePattern = Pattern.compile("^\\s*([^().\\s]+)\\s*(?:\\(.*\\)\\s*)?$", Pattern.CASE_INSENSITIVE);
  private static final Pattern functionParamsPattern = Pattern.compile("^\\s*(?:[^(.)]+)\\s*\\(\\s*(.+)\\s*\\)\\s*$", Pattern.CASE_INSENSITIVE);
  private static final String funtionVarParamUniqueName = ".%s_(%d)";

  /**
   * Used to denote a variable length parameter.
   */
  public final static String variableLengthParamSuffix = "..";
  /**
   * The character used to denote the start of a for each lambda expression
   */
  public final static char variableForEachSep = ':';
  /**
   * The character used to denote the looped parameter in the for each lambda expression
   */
  public final static char variableForEachParam = '_';

  private HashMap<String, VariableFunctionInfo> systemVariableFunctions;
  private HashMap<String, VariableFunctionInfo> variableFunctions;
  private HashSet<String> variableFunctionNameHistory;

  private HashMap<String, CreatorFunction> expressionCreators;
  private final ConstantFunction constantCreator;

  private LinkedHashMap<String, ReductionFunction> reductionFunctions;
  private LinkedHashMap<String, ReductionDataCollector<?>> collectors;
  private LinkedHashMap<String, AnalyticsField> fields;
  private HashMap<String, AnalyticsValueStream> expressions;

  private IndexSchema schema;

  private Map<String, ReductionDataCollector<?>> groupedCollectors;
  private Map<String, AnalyticsField> groupedFields;
  private boolean isGrouped;

  public ExpressionFactory(IndexSchema schema) {
    this.schema = schema;

    expressionCreators = new HashMap<>();
    systemVariableFunctions = new HashMap<>();

    constantCreator = ConstantValue.creatorFunction;
    addSystemFunctions();
  }

  /**
   * Get the index schema used by this factory.
   *
   * @return the index schema
   */
  public IndexSchema getSchema() {
    return schema;
  }

  /**
   * Prepare the factory to start building the request.
   */
  public void startRequest() {
    reductionFunctions = new LinkedHashMap<>();
    collectors = new LinkedHashMap<>();
    fields = new LinkedHashMap<>();
    expressions = new HashMap<>();

    variableFunctions = new HashMap<>();
    variableFunctions.putAll(systemVariableFunctions);
    variableFunctionNameHistory = new HashSet<>();

    isGrouped = false;
  }

  /**
   * Prepare the factory to start building the next grouping.
   * <br>
   * NOTE: MUST be called before each new grouping.
   */
  public void startGrouping() {
    groupedCollectors = new HashMap<>();
    groupedFields = new HashMap<>();

    isGrouped = true;
  }

  /**
   * Add a system function to the expression factory.
   * This will be treated as a native function and not a variable function.
   *
   * @param functionName the unique name for the function
   * @param functionCreator the creator function to generate an expression
   * @return this factory, to easily chain function adds
   * @throws SolrException if the functionName is not unique
   */
  public ExpressionFactory addSystemFunction(final String functionName, final CreatorFunction functionCreator) throws SolrException {
    if (expressionCreators.containsKey(functionName)) {
      throw new SolrException(ErrorCode.BAD_REQUEST,"System function " + functionName + " defined twice.");
    }
    expressionCreators.put(CountFunction.name, CountFunction.creatorFunction);
    return this;
  }

  /**
   * Add a variable function that will be treated like a system function.
   *
   * @param functionName the function's name
   * @param functionParams the comma separated and ordered parameters of the function (e.g. {@code "a,b"} )
   * @param returnSignature the return signature of the variable function (e.g. {@code div(sum(a,b),count(b))} )
   * @return this factory, to easily chain function adds
   * @throws SolrException if the name of the function is not unique or the syntax of either signature is incorrect
   */
  public ExpressionFactory addSystemVariableFunction(final String functionName, final String functionParams, final String returnSignature) throws SolrException {
    return addVariableFunction(functionName,
                               Arrays.stream(functionParams.split(",")).map(param -> param.trim()).toArray(size -> new String[size]),
                               returnSignature,
                               systemVariableFunctions);
  }

  /**
   * Add a variable function that was defined in an analytics request.
   *
   * @param functionSignature the function signature of the variable function (e.g. {@code func(a,b)} )
   * @param returnSignature the return signature of the variable function (e.g. {@code div(sum(a,b),count(b))} )
   * @return this factory, to easily chain function adds
   * @throws SolrException if the name of the function is not unique or the syntax of either signature is incorrect
   */
  public ExpressionFactory addUserDefinedVariableFunction(final String functionSignature, final String returnSignature) throws SolrException {
    return addVariableFunction(functionSignature, returnSignature, variableFunctions);
  }

  /**
   * Add a variable function to the given map of variable functions.
   *
   * @param functionSignature the function signature of the variable function (e.g. {@code func(a,b)} )
   * @param returnSignature the return signature of the variable function (e.g. {@code div(sum(a,b),count(b))} )
   * @param variableFunctions the map of variable functions to add the new function to
   * @return this factory, to easily chain function adds
   * @throws SolrException if the name of the function is not unique or the syntax of either signature is incorrect
   */
  private ExpressionFactory addVariableFunction(final String functionSignature,
                                                final String returnSignature,
                                                Map<String,VariableFunctionInfo> variableFunctions) throws SolrException {
    addVariableFunction(getFunctionName(functionSignature), getParams(functionSignature, null, null), returnSignature, variableFunctions);
    return this;
  }

  /**
   * Add a variable function to the given map of variable functions.
   *
   * @param functionName the function's name
   * @param functionParams the parameters of the function (this is ordered)
   * @param returnSignature the return signature of the variable function (e.g. {@code div(sum(a,b),count(b))} )
   * @param variableFunctions the map of variable functions to add the new function to
   * @return this factory, to easily chain function adds
   * @throws SolrException if the name of the function is not unique or the syntax of either signature is incorrect
   */
  private ExpressionFactory addVariableFunction(final String functionName,
                                                final String[] functionParams,
                                                final String returnSignature,
                                                Map<String,VariableFunctionInfo> variableFunctions) throws SolrException {
    if (expressionCreators.containsKey(functionName)) {
      throw new SolrException(ErrorCode.BAD_REQUEST,"Users cannot define a variable function with the same name as an existing function: " + functionName);
    }
    VariableFunctionInfo varFuncInfo = new VariableFunctionInfo();
    varFuncInfo.params = functionParams;
    varFuncInfo.returnSignature = returnSignature;
    variableFunctions.put(functionName, varFuncInfo);
    return this;
  }

  /**
   * Create a reduction manager to manage the collection of all expressions that have been created since
   * {@link #startRequest()} was called.
   *
   * @param isCloudCollection whether the request is a distributed request
   * @return a reduction manager
   */
  public ReductionCollectionManager createReductionManager(boolean isCloudCollection) {
    ReductionDataCollector<?>[] collectorsArr = new ReductionDataCollector<?>[collectors.size()];
    collectors.values().toArray(collectorsArr);
    if (isCloudCollection) {
      return new MergingReductionCollectionManager(collectorsArr, fields.values());
    } else {
      return new ReductionCollectionManager(collectorsArr, fields.values());
    }
  }

  /**
   * Create a reduction manager to manage the collection of all expressions that have been created since
   * {@link #startGrouping()} was called.
   *
   * @param isCloudCollection whether the request is a distributed request
   * @return a reduction manager
   */
  public ReductionCollectionManager createGroupingReductionManager(boolean isCloudCollection) {
    ReductionDataCollector<?>[] collectorsArr = new ReductionDataCollector<?>[groupedCollectors.size()];
    groupedCollectors.values().toArray(collectorsArr);
    if (isCloudCollection) {
      return new MergingReductionCollectionManager(collectorsArr, groupedFields.values());
    } else {
      return new ReductionCollectionManager(collectorsArr, groupedFields.values());
    }
  }

  /**
   * Parse and build an expression from the given expression string.
   *
   * @param expressionStr string that represents the desired expression
   * @return the object representation of the expression
   * @throws SolrException if an error occurs while constructing the expression
   */
  public AnalyticsValueStream createExpression(String expressionStr) throws SolrException {
    return createExpression(expressionStr, new HashMap<>(), null, null);
  }

  /**
   * Create an expression from the given expression string, with the given variable function information.
   *
   * @param expressionStr string that represents the desired expression
   * @param varFuncParams the current set of variable function parameters and their values. If this expression is not a variable function
   * return signature, the map should be empty.
   * @param varFuncVarParamName if the current expression is a variable function return signature, this must be the name of the variable length
   * parameter if it is included in the function signature.
   * @param varFuncVarParamValues if the current expression is a variable function return signature, this must be the array values of the variable length
   * parameter if they are included when calling the function.
   * @return the object representation of the expression
   * @throws SolrException if an error occurs while constructing the expression
   */
  private AnalyticsValueStream createExpression(String expressionStr, Map<String,AnalyticsValueStream> varFuncParams,
                                                String varFuncVarParamName, String[] varFuncVarParamValues) throws SolrException {
    AnalyticsValueStream expression;
    expressionStr = expressionStr.trim();

    boolean isField = false;
    try {
      // Try to make a constant value
      expression = constantCreator.apply(expressionStr);
    } catch (SolrException e1) {
      // Not a constant
      // If the expression has parens, it is an expression otherwise it is a field
      if (!expressionStr.contains("(")) {
        // Try to make a field out of it
        expression = createField(schema.getField(expressionStr));
        isField = true;
      } else {
        // Must be a function
        expression = createFunction(expressionStr, varFuncParams, varFuncVarParamName, varFuncVarParamValues);
      }
    }

    // Try to use an already made expression instead of the new one.
    // This will decrease the amount of collection needed to be done.
    if (expressions.containsKey(expression.getExpressionStr())) {
      expression = expressions.get(expression.getExpressionStr());
      // If this is a grouped expression, make sure that the reduction info for the expression is included in the grouped reduction manager.
      if (expression.getExpressionType() == ExpressionType.REDUCTION && isGrouped) {
        ((ReductionFunction)expression).synchronizeDataCollectors( collector -> {
          groupedCollectors.put(collector.getExpressionStr(), collector);
          return collector;
        });
      }
    }
    else {
      expressions.put(expression.getExpressionStr(), expression);
      // Make sure that the reduction info for the expression is included in the reduction manager and grouped reduction manager if necessary.
      if (expression.getExpressionType() == ExpressionType.REDUCTION) {
        reductionFunctions.put(expression.getExpressionStr(), (ReductionFunction)expression);
        ((ReductionFunction)expression).synchronizeDataCollectors( collector -> {
          String collectorStr = collector.getExpressionStr();
          ReductionDataCollector<?> usedCollector = collectors.get(collectorStr);
          if (usedCollector == null) {
            usedCollector = collector;
            collectors.put(collectorStr, collector);
          }
          if (isGrouped) {
            groupedCollectors.put(collectorStr, usedCollector);
          }
          return usedCollector;
        });
      }
      // Add the field info to the reduction manager
      if (isField) {
        fields.put(expression.getExpressionStr(), (AnalyticsField)expression);
      }
    }
    // If this is a grouped expression, make sure that the field info is included in the grouped reduction manager.
    if (isField && isGrouped) {
      groupedFields.put(expression.getExpressionStr(), (AnalyticsField)expression);
    }
    return expression;
  }

  /**
   * Create a function expression from the given expression string, with the given variable function information.
   *
   * @param expressionStr string that represents the desired expression
   * @param varFuncParams the current set of variable function parameters and their values. If this expression is not a variable function
   * return signature, the map should be empty.
   * @param varFuncVarParamName if the current expression is a variable function return signature, this must be the name of the variable length
   * parameter if it is included in the function signature.
   * @param varFuncVarParamValues if the current expression is a variable function return signature, this must be the array values of the variable length
   * parameter if they are included when calling the function.
   * @return the object representation of the expression
   * @throws SolrException if an error occurs while constructing the expression
   */
  private AnalyticsValueStream createFunction(String expressionStr, Map<String,AnalyticsValueStream> varFuncParams,
                                              String varFuncVarParamName, String[] varFuncVarParamValues) throws SolrException {
    AnalyticsValueStream expression = null;
    String name = getFunctionName(expressionStr);

    final String[] params = getParams(expressionStr, varFuncVarParamName, varFuncVarParamValues);
    AnalyticsValueStream[] paramStreams = new AnalyticsValueStream[params.length];

    boolean allParamsConstant = true;

    for (int i = 0; i < params.length; i++) {
      // First check if the parameter is a variable function variable otherwise create the expression
      if (varFuncParams.containsKey(params[i])) {
        paramStreams[i] = varFuncParams.get(params[i]);
      } else {
        paramStreams[i] = createExpression(params[i], varFuncParams, varFuncVarParamName, varFuncVarParamValues);
      }

      // Then update whether all of the params are constant
      allParamsConstant &= paramStreams[i].getExpressionType().equals(ExpressionType.CONST);
    }

    // Check to see if the function name is a variable function name, if so apply the variables to the return signature
    if (variableFunctions.containsKey(name)) {
      if (variableFunctionNameHistory.contains(name)) {
        throw new SolrException(ErrorCode.BAD_REQUEST,"The following variable function is self referencing : " + name);
      }
      variableFunctionNameHistory.add(name);
      VariableFunctionInfo newVarFunc = variableFunctions.get(name);
      Map<String, AnalyticsValueStream> newVarFuncParams = new HashMap<>();

      boolean varLenEnd = false;

      if (paramStreams.length < newVarFunc.params.length) {
        throw new SolrException(ErrorCode.BAD_REQUEST,"The variable function '" + name + "' requires at least " + newVarFunc.params.length + " parameters."
            + " Only " + paramStreams.length + " arguments given in the following invocation : " + expressionStr);
      }
      for (int i = 0; i < newVarFunc.params.length; ++i) {
        String variable = newVarFunc.params[i];
        if (variable.endsWith(variableLengthParamSuffix)) {
          if (i != newVarFunc.params.length - 1) {
            throw new SolrException(ErrorCode.BAD_REQUEST,"The following invocation of a variable function has the incorrect number of arguments : " + expressionStr);
          }
          variable = variable.substring(0, variable.length() - variableLengthParamSuffix.length()).trim();
          int numVars = paramStreams.length - i;
          String[] newVarFuncVarParamValues = new String[numVars];
          for (int j = 0; j < numVars; ++j) {
            // Create a new name for each variable length parameter value
            String paramName = String.format(Locale.ROOT, funtionVarParamUniqueName, variable, j);
            newVarFuncVarParamValues[j] = paramName;
            newVarFuncParams.put(paramName, paramStreams[i + j]);
          }
          expression = createFunction(newVarFunc.returnSignature, newVarFuncParams, variable, newVarFuncVarParamValues);
          varLenEnd = true;
        } else {
          newVarFuncParams.put(variable, paramStreams[i]);
        }
      }
      if (!varLenEnd) {
        if (paramStreams.length > newVarFunc.params.length) {
          throw new SolrException(ErrorCode.BAD_REQUEST,"The variable function '" + name + "' requires " + newVarFunc.params.length + " parameters."
              + " " + paramStreams.length + " arguments given in the following invocation : " + expressionStr);
        }
        expression = createExpression(newVarFunc.returnSignature, newVarFuncParams, null, null);
      }
      variableFunctionNameHistory.remove(name);
    } else if (expressionCreators.containsKey(name)) {
      // It is a regular system function
      expression = expressionCreators.get(name).apply(paramStreams);
    } else {
      throw new SolrException(ErrorCode.BAD_REQUEST,"The following function does not exist: " + name);
    }

    // If the all params are constant, then try to convert the expression to a constant value.
    expression = expression.convertToConstant();

    return expression;
  }

  /**
   * Create an {@link AnalyticsField} out of the given {@link SchemaField}.
   * <p>
   * Currently only fields with doc-values enabled are supported.
   *
   * @param field the field to convert for analytics
   * @return an analytics representation of the field
   * @throws SolrException if the field is not supported by the analytics framework
   */
  private AnalyticsField createField(SchemaField field) throws SolrException {
    String fieldName = field.getName();
    if (fields.containsKey(fieldName)) {
      return fields.get(fieldName);
    }
    if (!field.hasDocValues()) {
      throw new SolrException(ErrorCode.BAD_REQUEST,"The field "+fieldName+" does not have docValues enabled.");
    }
    boolean multivalued = field.multiValued();
    FieldType fieldType = field.getType();
    AnalyticsField aField;
    if (fieldType instanceof BoolField) {
      if (multivalued) {
        aField = new BooleanMultiField(fieldName);
      } else {
        aField = new BooleanField(fieldName);
      }
    } else if (fieldType instanceof TrieIntField) {
      if (multivalued) {
        aField = new IntMultiTrieField(fieldName);
      } else {
        aField = new IntField(fieldName);
      }
    } else if (fieldType instanceof IntPointField) {
      if (multivalued) {
        aField = new IntMultiPointField(fieldName);
      } else {
        aField = new IntField(fieldName);
      }
    } else if (fieldType instanceof TrieLongField) {
      if (multivalued) {
        aField = new LongMultiTrieField(fieldName);
      } else {
        aField = new LongField(fieldName);
      }
    } else if (fieldType instanceof LongPointField) {
      if (multivalued) {
        aField = new LongMultiPointField(fieldName);
      } else {
        aField = new LongField(fieldName);
      }
    } else if (fieldType instanceof TrieFloatField) {
      if (multivalued) {
        aField = new FloatMultiTrieField(fieldName);
      } else {
        aField = new FloatField(fieldName);
      }
    } else if (fieldType instanceof FloatPointField) {
      if (multivalued) {
        aField = new FloatMultiPointField(fieldName);
      } else {
        aField = new FloatField(fieldName);
      }
    } else if (fieldType instanceof TrieDoubleField) {
      if (multivalued) {
        aField = new DoubleMultiTrieField(fieldName);
      } else {
        aField = new DoubleField(fieldName);
      }
    } else if (fieldType instanceof DoublePointField) {
      if (multivalued) {
        aField = new DoubleMultiPointField(fieldName);
      } else {
        aField = new DoubleField(fieldName);
      }
    } else if (fieldType instanceof TrieDateField) {
      if (multivalued) {
        aField = new DateMultiTrieField(fieldName);
      } else {
        aField = new DateField(fieldName);
      }
    } else if (fieldType instanceof DatePointField) {
      if (multivalued) {
        aField = new DateMultiPointField(fieldName);
      } else {
        aField = new DateField(fieldName);
      }
    } else if (fieldType instanceof StrField) {
      if (multivalued) {
        aField = new StringMultiField(fieldName);
      } else {
        aField = new StringField(fieldName);
      }
    } else {
      throw new SolrException(ErrorCode.BAD_REQUEST,"FieldType of the following field not supported by analytics: "+fieldName);
    }
    return aField;
  }

  /**
   * Get the name of the top function used in the given expression.
   *
   * @param expression the expression to find the function name of
   * @return the name of the function
   * @throws SolrException if the expression has incorrect syntax
   */
  private static String getFunctionName(String expression) throws SolrException {
    Matcher m = functionNamePattern.matcher(expression);
    if (!m.matches()) {
      throw new SolrException(ErrorCode.BAD_REQUEST,"The following function has no name: " + expression);
    }
    String name = m.group(1);
    return name;
  }

  /**
   * Get the params of a function.
   *
   * @param function the function to parse
   * @return an array of param strings
   * @throws SolrException if the function has incorrect syntax
   */
  private static String[] getFunctionParams(String function) throws SolrException {
    return getParams(function, null, null);
  }

  /**
   * Parse a function expression string and break up the parameters of the function into separate strings.
   * <p>
   * The parsing replaces the variable length parameter, and lambda for-each's using the variable length parameter,
   * with the parameter values in the returned parameter string.
   * <p>
   * Parsing breaks up parameters by commas (',') and ignores ',' inside of extra parens and quotes (both ' and "), since these commas are either
   * splitting up the parameters of nested functions or are apart of strings.
   * <br>
   * The only escaping that needs to be done is " within a double quote string and ' within a single quote string and \ within any string.
   * For example\:
   * <ul>
   * <li> {@code func("This is \" the \\ escaping ' example")} will be treated as {@code func(This is " the \ escaping ' example)}
   * <li> {@code func('This is " the \\ escaping \' example')} will be treated as {@code func(This is " the \ escaping ' example)}
   * </ul>
   * In string constants the \ character is used to escape quotes, so it can never be used alone. in order to write a \ you must write \\
   *
   * @param expression the function expression to parse
   * @param varLengthParamName the name of the variable length parameter that is used in the expression, pass null if none is used.
   * @param varLengthParamValues the values of the variable length parameter that are used in the expression, pass null if none are used.
   * @return the parsed and split arguments to the function
   * @throws SolrException if the expression has incorrect syntax.
   */
  private static String[] getParams(String expression, String varLengthParamName, String[] varLengthParamValues) throws SolrException {
    Matcher m = functionParamsPattern.matcher(expression);
    if (!m.matches()) {
      return new String[0];
    }
    String paramsStr = m.group(1);

    ArrayList<String> paramsList = new ArrayList<String>();
    StringBuilder param = new StringBuilder();

    // Variables to help while filling out the values of for-each lambda functions.
    boolean inForEach = false;
    int forEachStart = -1;
    int forEachIter = -1;
    int forEachLevel = -1;

    // The current level of nested parenthesis, 0 means the iteration is in no nested parentheses
    int parenCount = 0;
    // If the iteration is currently in a single-quote string constant
    boolean singleQuoteOn = false;
    // If the iteration is currently in a double-quote string constant
    boolean doubleQuoteOn = false;
    // If the iteration is currently in any kind of string constant
    boolean quoteOn = false;
    // Is the next character escaped.
    boolean escaped = false;

    char[] chars = paramsStr.toCharArray();

    // Iterate through every character, building the params one at a time
    for (int i = 0; i < chars.length; ++i) {
      char c = chars[i];

      if (c == ' ' && !quoteOn) {
        // Ignore white space that is not in string constants
        continue;
      } else if (c == ',' && parenCount == 0 && !quoteOn) {
        // This signifies the end of one parameter and the start of another, since we are not in a nested parenthesis or a string constant
        String paramStr = param.toString();
        if (paramStr.length() == 0) {
          throw new SolrException(ErrorCode.BAD_REQUEST,"Empty parameter in expression: " + expression);
        }
        // check to see if the parameter is a variable length parameter
        if (paramStr.equals(varLengthParamName)) {
          // Add every variable length parameter value, since there are a variable amount
          for (String paramName : varLengthParamValues) {
            paramsList.add(paramName);
          }
        } else {
          paramsList.add(paramStr);
        }

        param.setLength(0);
        continue;
      } else if (c == ',' && !quoteOn && inForEach) {
        // separate the for each parameters, so they can be replaced with the result of the for each
        if (param.charAt(param.length()-1) == variableForEachParam &&
            (param.charAt(param.length()-2) == '(' || param.charAt(param.length()-2) == ',')) {
          param.setLength(param.length()-1);
          param.append(varLengthParamValues[forEachIter++]);
        }
      } else if (c == '"' && !singleQuoteOn) {
        // Deal with escaping, or ending string constants
        if (doubleQuoteOn && !escaped) {
          doubleQuoteOn = false;
          quoteOn = false;
        } else if (!quoteOn) {
          doubleQuoteOn = true;
          quoteOn = true;
        } else {
          // only happens if escaped is true
          escaped = false;
        }
      }  else if (c== '\'' && !doubleQuoteOn) {
        // Deal with escaping, or ending string constants
        if (singleQuoteOn && !escaped) {
          singleQuoteOn = false;
          quoteOn = false;
        } else if (!singleQuoteOn) {
          singleQuoteOn = true;
          quoteOn = true;
        } else {
          // only happens if escaped is true
          escaped = false;
        }
      } else if (c == '(' && !quoteOn) {
        // Reached a further level of nested parentheses
        parenCount++;
      } else if (c == ')' && !quoteOn) {
        // Returned from a level of nested parentheses
        parenCount--;
        if (parenCount < 0) {
          throw new SolrException(ErrorCode.BAD_REQUEST,"The following expression has extra end parens: " + param.toString());
        }
        if (inForEach) {
          if (param.charAt(param.length()-1) == variableForEachParam &&
              (param.charAt(param.length()-2) == '(' || param.charAt(param.length()-2) == ',')) {
            param.setLength(param.length()-1);
            param.append(varLengthParamValues[forEachIter++]);
          }
          if (forEachLevel == parenCount) {
            // at the end of the for-each start the parsing of the for-each again, with the next value of the variable length parameter
            if (forEachIter == 0) {
              throw new SolrException(ErrorCode.BAD_REQUEST,"For each statement for variable '" + varLengthParamName + "' has no use of lambda variable " + variableForEachParam);
            } else if (forEachIter < varLengthParamValues.length) {
              if (parenCount == 0) {
                param.append(')');
                paramsList.add(param.toString());
                param.setLength(0);
              } else {
                param.append(')');
                param.append(',');
              }
              i = forEachStart;
              continue;
            } else {
              inForEach = false;
            }
          }
        }
      }
      if (c == '\\') {
        // Escaping or escaped backslash
        if (!quoteOn) {
          throw new SolrException(ErrorCode.BAD_REQUEST,"The following expression has escaped characters outside of quotation marks: " + expression.toString());
        }
        if (escaped) {
          escaped = false;
        } else {
          escaped = true;
          if (parenCount == 0) {
            continue;
          }
        }
      } else if (escaped) {
        throw new SolrException(ErrorCode.BAD_REQUEST,"Invalid escape character '" + c + "' used in the following expression: " + expression.toString());
      }
      if (c == variableForEachSep && !quoteOn && varLengthParamName != null) {
        int varStart = param.length()-varLengthParamName.length();
        if (param.subSequence(varStart, param.length()).equals(varLengthParamName)) {
          inForEach = true;
          forEachStart = i;
          forEachIter = 0;
          forEachLevel = parenCount;
          param.setLength(varStart);
          continue;
        }
        throw new SolrException(ErrorCode.BAD_REQUEST,"For-each called on invalid parameter '" + param.toString().trim());
      }
      param.append(c);
    }
    String paramStr = param.toString().trim();
    if (paramStr.length() == 0) {
      throw new SolrException(ErrorCode.BAD_REQUEST,"Empty parameter in expression: " + expression);
    }
    if (parenCount > 0) {
      throw new SolrException(ErrorCode.BAD_REQUEST,"The following expression needs more end parens: " + param.toString());
    }
    if (quoteOn) {
      throw new SolrException(ErrorCode.BAD_REQUEST,"Misplaced quotation marks in expression: " + expression);
    }
    if (paramStr.equals(varLengthParamName)) {
      for (String paramName : varLengthParamValues) {
        paramsList.add(paramName);
      }
    } else {
      paramsList.add(paramStr);
    }
    return paramsList.toArray(new String[paramsList.size()]);
  }

  /**
   * Add the natively supported functionality.
   */
  public void addSystemFunctions() {
    // Mapping Functions
    expressionCreators.put(AbsoluteValueFunction.name, AbsoluteValueFunction.creatorFunction);
    expressionCreators.put(AndFunction.name, AndFunction.creatorFunction);
    expressionCreators.put(AddFunction.name, AddFunction.creatorFunction);
    expressionCreators.put(BottomFunction.name, BottomFunction.creatorFunction);
    expressionCreators.put(CeilingFunction.name, CeilingFunction.creatorFunction);
    expressionCreators.put(ConcatFunction.name, ConcatFunction.creatorFunction);
    expressionCreators.put(SeparatedConcatFunction.name, SeparatedConcatFunction.creatorFunction);
    expressionCreators.put(DateMathFunction.name, DateMathFunction.creatorFunction);
    expressionCreators.put(DateParseFunction.name, DateParseFunction.creatorFunction);
    expressionCreators.put(DivideFunction.name, DivideFunction.creatorFunction);
    expressionCreators.put(EqualFunction.name,EqualFunction.creatorFunction);
    expressionCreators.put(ExistsFunction.name,ExistsFunction.creatorFunction);
    expressionCreators.put(FillMissingFunction.name, FillMissingFunction.creatorFunction);
    expressionCreators.put(FilterFunction.name, FilterFunction.creatorFunction);
    expressionCreators.put(FloorFunction.name, FloorFunction.creatorFunction);
    expressionCreators.put(GTFunction.name,GTFunction.creatorFunction);
    expressionCreators.put(GTEFunction.name,GTEFunction.creatorFunction);
    expressionCreators.put(IfFunction.name, IfFunction.creatorFunction);
    expressionCreators.put(LogFunction.name,LogFunction.creatorFunction);
    expressionCreators.put(LTFunction.name,LTFunction.creatorFunction);
    expressionCreators.put(LTEFunction.name,LTEFunction.creatorFunction);
    expressionCreators.put(MultFunction.name, MultFunction.creatorFunction);
    expressionCreators.put(NegateFunction.name, NegateFunction.creatorFunction);
    expressionCreators.put(OrFunction.name, OrFunction.creatorFunction);
    expressionCreators.put(PowerFunction.name, PowerFunction.creatorFunction);
    expressionCreators.put(ReplaceFunction.name, ReplaceFunction.creatorFunction);
    expressionCreators.put(RemoveFunction.name, RemoveFunction.creatorFunction);
    expressionCreators.put(RoundFunction.name, RoundFunction.creatorFunction);
    expressionCreators.put(StringCastFunction.name, StringCastFunction.creatorFunction);
    expressionCreators.put(SubtractFunction.name, SubtractFunction.creatorFunction);
    expressionCreators.put(TopFunction.name, TopFunction.creatorFunction);

    // Reduction Functions
    expressionCreators.put(CountFunction.name, CountFunction.creatorFunction);
    expressionCreators.put(DocCountFunction.name, DocCountFunction.creatorFunction);
    expressionCreators.put(MaxFunction.name, MaxFunction.creatorFunction);
    expressionCreators.put(MeanFunction.name, MeanFunction.creatorFunction);
    expressionCreators.put(MedianFunction.name, MedianFunction.creatorFunction);
    expressionCreators.put(MinFunction.name, MinFunction.creatorFunction);
    expressionCreators.put(MissingFunction.name, MissingFunction.creatorFunction);
    expressionCreators.put(OrdinalFunction.name, OrdinalFunction.creatorFunction);
    expressionCreators.put(PercentileFunction.name, PercentileFunction.creatorFunction);
    expressionCreators.put(SumFunction.name, SumFunction.creatorFunction);
    expressionCreators.put(UniqueFunction.name, UniqueFunction.creatorFunction);

    // Variables
    addSystemVariableFunction(WeightedMeanVariableFunction.name, WeightedMeanVariableFunction.params, WeightedMeanVariableFunction.function);
    addSystemVariableFunction(SumOfSquaresVariableFunction.name, SumOfSquaresVariableFunction.params, SumOfSquaresVariableFunction.function);
    addSystemVariableFunction(SquareRootVariableFunction.name, SquareRootVariableFunction.params, SquareRootVariableFunction.function);
    addSystemVariableFunction(VarianceVariableFunction.name, VarianceVariableFunction.params, VarianceVariableFunction.function);
    addSystemVariableFunction(SandardDeviationVariableFunction.name, SandardDeviationVariableFunction.params, SandardDeviationVariableFunction.function);
    addSystemVariableFunction(CSVVariableFunction.name, CSVVariableFunction.params, CSVVariableFunction.function);
    addSystemVariableFunction(CSVOutputVariableFunction.name, CSVOutputVariableFunction.params, CSVOutputVariableFunction.function);
  }

  /**
   * Used for system analytics functions for initialization. Should take in a list of expression parameters and return an expression.
   */
  @FunctionalInterface
  public static interface CreatorFunction {
    AnalyticsValueStream apply(AnalyticsValueStream[] t) throws SolrException;
  }
  /**
   * Used to initialize analytics constants.
   */
  @FunctionalInterface
  public static interface ConstantFunction {
    AnalyticsValueStream apply(String t) throws SolrException;
  }
  static class VariableFunctionInfo {
    public String[] params;
    public String returnSignature;
  }
  static class WeightedMeanVariableFunction {
    public static final String name = "wmean";
    public static final String params = "a,b";
    public static final String function = DivideFunction.name+"("+SumFunction.name+"("+MultFunction.name+"(a,b)),"+SumFunction.name+"("+FilterFunction.name+"(b,"+ExistsFunction.name+"(a))))";
  }
  static class SumOfSquaresVariableFunction {
    public static final String name = "sumofsquares";
    public static final String params = "a";
    public static final String function = SumFunction.name+"("+PowerFunction.name+"(a,2))";
  }
  static class SquareRootVariableFunction {
    public static final String name = "sqrt";
    public static final String params = "a";
    public static final String function = PowerFunction.name+"(a,0.5)";
  }
  static class VarianceVariableFunction {
    public static final String name = "variance";
    public static final String params = "a";
    public static final String function = SubtractFunction.name+"("+MeanFunction.name+"("+PowerFunction.name+"(a,2)),"+PowerFunction.name+"("+MeanFunction.name+"(a),2))";
  }
  static class SandardDeviationVariableFunction {
    public static final String name = "stddev";
    public static final String params = "a";
    public static final String function = SquareRootVariableFunction.name+"("+VarianceVariableFunction.name+"(a))";
  }
  static class CSVVariableFunction {
    public static final String name = "csv";
    public static final String params = "a"+ExpressionFactory.variableLengthParamSuffix;
    public static final String function = SeparatedConcatFunction.name+"(',',a)";
  }
  static class CSVOutputVariableFunction {
    public static final String name = "csv_output";
    public static final String params = "a"+ExpressionFactory.variableLengthParamSuffix;
    public static final String function = "concat_sep(',',a"+ExpressionFactory.variableForEachSep+FillMissingFunction.name+"("+SeparatedConcatFunction.name+"(';',"+ExpressionFactory.variableForEachParam+"),''))";
  }
}
