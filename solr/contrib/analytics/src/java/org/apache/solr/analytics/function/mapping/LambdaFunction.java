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
package org.apache.solr.analytics.function.mapping;

import java.util.function.Consumer;
import java.util.function.DoubleConsumer;
import java.util.function.IntConsumer;
import java.util.function.LongConsumer;

import org.apache.solr.analytics.function.mapping.LambdaFunction.BoolInBoolOutLambda;
import org.apache.solr.analytics.function.mapping.LambdaFunction.DoubleInDoubleOutLambda;
import org.apache.solr.analytics.function.mapping.LambdaFunction.FloatInFloatOutLambda;
import org.apache.solr.analytics.function.mapping.LambdaFunction.IntInIntOutLambda;
import org.apache.solr.analytics.function.mapping.LambdaFunction.LongInLongOutLambda;
import org.apache.solr.analytics.function.mapping.LambdaFunction.StringInStringOutLambda;
import org.apache.solr.analytics.function.mapping.LambdaFunction.TwoBoolInBoolOutLambda;
import org.apache.solr.analytics.function.mapping.LambdaFunction.TwoDoubleInDoubleOutLambda;
import org.apache.solr.analytics.function.mapping.LambdaFunction.TwoFloatInFloatOutLambda;
import org.apache.solr.analytics.function.mapping.LambdaFunction.TwoIntInIntOutLambda;
import org.apache.solr.analytics.function.mapping.LambdaFunction.TwoLongInLongOutLambda;
import org.apache.solr.analytics.function.mapping.LambdaFunction.TwoStringInStringOutLambda;
import org.apache.solr.analytics.util.function.BooleanConsumer;
import org.apache.solr.analytics.util.function.FloatConsumer;
import org.apache.solr.analytics.value.AnalyticsValueStream;
import org.apache.solr.analytics.value.BooleanValue;
import org.apache.solr.analytics.value.BooleanValueStream;
import org.apache.solr.analytics.value.DateValue;
import org.apache.solr.analytics.value.DateValueStream;
import org.apache.solr.analytics.value.DoubleValue;
import org.apache.solr.analytics.value.DoubleValueStream;
import org.apache.solr.analytics.value.FloatValue;
import org.apache.solr.analytics.value.FloatValueStream;
import org.apache.solr.analytics.value.IntValue;
import org.apache.solr.analytics.value.IntValueStream;
import org.apache.solr.analytics.value.LongValue;
import org.apache.solr.analytics.value.LongValueStream;
import org.apache.solr.analytics.value.StringValue;
import org.apache.solr.analytics.value.StringValueStream;
import org.apache.solr.analytics.value.BooleanValue.AbstractBooleanValue;
import org.apache.solr.analytics.value.BooleanValueStream.AbstractBooleanValueStream;
import org.apache.solr.analytics.value.DateValue.AbstractDateValue;
import org.apache.solr.analytics.value.DateValueStream.AbstractDateValueStream;
import org.apache.solr.analytics.value.DoubleValue.AbstractDoubleValue;
import org.apache.solr.analytics.value.DoubleValueStream.AbstractDoubleValueStream;
import org.apache.solr.analytics.value.FloatValue.AbstractFloatValue;
import org.apache.solr.analytics.value.FloatValueStream.AbstractFloatValueStream;
import org.apache.solr.analytics.value.IntValue.AbstractIntValue;
import org.apache.solr.analytics.value.IntValueStream.AbstractIntValueStream;
import org.apache.solr.analytics.value.LongValue.AbstractLongValue;
import org.apache.solr.analytics.value.LongValueStream.AbstractLongValueStream;
import org.apache.solr.analytics.value.StringValue.AbstractStringValue;
import org.apache.solr.analytics.value.StringValueStream.AbstractStringValueStream;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;

/**
 * Lambda Functions are used to easily create basic mapping functions.
 * <p>
 * There are lambda functions for all types: boolean, int, long, float, double, date and string.
 * Lambda functions must have parameters of all the same type, which will be the same type as the returned Value or ValueStream.
 * <p>
 * The types of functions that are accepted are: (multi = multi-valued expression, single = single-valued expression)
 * <ul>
 * <li> {@code func(single) -> single}
 * <li> {@code func(multi) -> multi}
 * <li> {@code func(single,single) -> single}
 * <li> {@code func(multi,single) -> multi}
 * <li> {@code func(single,multi) -> multi}
 * <li> {@code func(multi) -> single}
 * <li> {@code func(single,single,...) -> single}
 * (You can also specify whether all parameters must exist, or at least one must exist for the returned value to exist)
 * </ul>
 * <p>
 * NOTE: The combination of name and parameters MUST be unique for an expression.
 * <br>
 * For example consider if {@code join(fieldA, ',')} and {@code join(fieldA, ';')} are both called. If the JoinFunction uses:
 * <br>
 * {@code LambdaFunction.createStringLambdaFunction("join", (a,b) -> a + sep + b, (StringValueStream)params[0])}
 * <br>
 * then both the name "join" and single parameter fieldA will be used for two DIFFERENT expressions.
 * This does not meet the uniqueness requirmenet and will break the query.
 * <br>
 * A solution to this is to name the function using the missing information:
 * <br>
 * {@code LambdaFunction.createStringLambdaFunction("join(" + sep + ")", (a,b) -> a + sep + b, (StringValueStream)params[0])}
 * <br>
 * Therefore both expressions will have fieldA as the only parameter, but the names {@code "join(,)"} and {@code "join(;)"} will be different.
 * This meets the uniqueness requirement for lambda functions.
 */
public class LambdaFunction {
  private static final boolean defaultMultiExistsMethod = true;

  /* *********************
   *
   *  Boolean Functions
   *
   * *********************/

  /**
   * Creates a function that takes in either a single or multi valued boolean expression and returns the same type of expression with
   * the given lambda function applied to every value.
   *
   * @param name name for the function
   * @param lambda the function to be applied to every value: {@code (boolean) -> boolean}
   * @param param the expression to apply the lambda to
   * @return an expression the same type as was given with the lambda applied
   */
  public static BooleanValueStream createBooleanLambdaFunction(String name, BoolInBoolOutLambda lambda, BooleanValueStream param) {
    if (param instanceof BooleanValue) {
      return new BooleanValueInBooleanValueOutFunction(name,lambda,(BooleanValue)param);
    } else {
      return new BooleanStreamInBooleanStreamOutFunction(name,lambda,param);
    }
  }
  /**
   * Creates a function that takes in a multi-valued boolean expression and returns a single-valued boolean expression.
   * The given lambda is used to associatively (order not guaranteed) reduce all values for a document down to a single value.
   *
   * @param name name for the function
   * @param lambda the associative function used to reduce the values: {@code (boolean, boolean) -> boolean}
   * @param param the expression to be reduced per-document
   * @return a single-valued expression which has been reduced for every document
   */
  public static BooleanValue createBooleanLambdaFunction(String name, TwoBoolInBoolOutLambda lambda, BooleanValueStream param) {
    return new BooleanStreamInBooleanValueOutFunction(name,lambda,param);
  }
  /**
   * Creates a function that maps two booleans to a single boolean.
   * This can take the following shapes:
   * <ul>
   * <li> Taking in two single-valued expressions and returning a single-valued expression which represents the lambda combination of the inputs.
   * <li> Taking in a single-valued expression and a multi-valued expression and returning a multi-valued expression which
   * represents the lambda combination of the single-value input with each of the values of the multi-value input.
   * <br>
   * The inputs can be either {@code func(single,multi)} or {@code func(multi,single)}.
   * </ul>
   *
   * @param name name for the function
   * @param lambda the function to be applied to every value: {@code (boolean,boolean) -> boolean}
   * @param param1 the first parameter in the lambda
   * @param param2 the second parameter in the lambda
   * @return a single or multi valued expression combining the two parameters with the given lambda
   * @throws SolrException if neither parameter is single-valued
   */
  public static BooleanValueStream createBooleanLambdaFunction(String name, TwoBoolInBoolOutLambda lambda, BooleanValueStream param1, BooleanValueStream param2) throws SolrException {
    if (param1 instanceof BooleanValue && param2 instanceof BooleanValue) {
      return new TwoBooleanValueInBooleanValueOutFunction(name,lambda,(BooleanValue)param1,(BooleanValue)param2);
    } else if (param1 instanceof BooleanValue) {
      return new BooleanValueBooleanStreamInBooleanStreamOutFunction(name,lambda,(BooleanValue)param1,param2);
    } else if (param2 instanceof BooleanValue) {
      return new BooleanStreamBooleanValueInBooleanStreamOutFunction(name,lambda,param1,(BooleanValue)param2);
    } else {
      throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function requires at least 1 single-valued parameter.");
    }
  }
  /**
   * Forwards the creation of the function to {@link #createBooleanLambdaFunction(String, TwoBoolInBoolOutLambda, BooleanValue[], boolean)},
   * using {@value #defaultMultiExistsMethod} for the last argument ({@code allMustExist}).
   *
   * @param name name for the function
   * @param lambda the associative function used to reduce the values: {@code (boolean, boolean) -> boolean}
   * @param params the expressions to reduce
   * @return a single-value expression that reduces the parameters with the given lambda
   */
  public static BooleanValue createBooleanLambdaFunction(String name, TwoBoolInBoolOutLambda lambda, BooleanValue[] params) {
    return createBooleanLambdaFunction(name,lambda,params,defaultMultiExistsMethod);
  }
  /**
   * Creates a function that associatively (order is guaranteed) reduces multiple
   * single-value boolean expressions into a single-value boolean expression for each document.
   * <br>
   * For a document, every parameter's value must exist for the resulting value to exist if {@code allMustExist} is true.
   * If {@code allMustExist} is false, only one of the parameters' values must exist.
   *
   * @param name name for the function
   * @param lambda the associative function used to reduce the values: {@code (boolean, boolean) -> boolean}
   * @param params the expressions to reduce
   * @param allMustExist whether all parameters are required to exist
   * @return a single-value expression that reduces the parameters with the given lambda
   */
  public static BooleanValue createBooleanLambdaFunction(String name, TwoBoolInBoolOutLambda lambda, BooleanValue[] params, boolean allMustExist) {
    if (allMustExist) {
      return new MultiBooleanValueInBooleanValueOutRequireAllFunction(name,lambda,params);
    } else {
      return new MultiBooleanValueInBooleanValueOutRequireOneFunction(name,lambda,params);
    }
  }

  /* *********************
   *
   *  Integer Functions
   *
   * *********************/

  /**
   * Creates a function that takes in either a single or multi valued integer expression and returns the same type of expression with
   * the given lambda function applied to every value.
   *
   * @param name name for the function
   * @param lambda the function to be applied to every value: {@code (integer) -> integer}
   * @param param the expression to apply the lambda to
   * @return an expression the same type as was given with the lambda applied
   */
  public static IntValueStream createIntLambdaFunction(String name, IntInIntOutLambda lambda, IntValueStream param) {
    if (param instanceof IntValue) {
      return new IntValueInIntValueOutFunction(name,lambda,(IntValue)param);
    } else {
      return new IntStreamInIntStreamOutFunction(name,lambda,param);
    }
  }
  /**
   * Creates a function that takes in a multi-valued integer expression and returns a single-valued integer expression.
   * The given lambda is used to associatively (order not guaranteed) reduce all values for a document down to a single value.
   *
   * @param name name for the function
   * @param lambda the associative function used to reduce the values: {@code (integer, integer) -> integer}
   * @param param the expression to be reduced per-document
   * @return a single-valued expression which has been reduced for every document
   */
  public static IntValue createIntLambdaFunction(String name, TwoIntInIntOutLambda lambda, IntValueStream param) {
    return new IntStreamInIntValueOutFunction(name,lambda,param);
  }
  /**
   * Creates a function that maps two integers to a single integer.
   * This can take the following shapes:
   * <ul>
   * <li> Taking in two single-valued expressions and returning a single-valued expression which represents the lambda combination of the inputs.
   * <li> Taking in a single-valued expression and a multi-valued expression and returning a multi-valued expression which
   * represents the lambda combination of the single-value input with each of the values of the multi-value input.
   * <br>
   * The inputs can be either {@code func(single,multi)} or {@code func(multi,single)}.
   * </ul>
   *
   * @param name name for the function
   * @param lambda the function to be applied to every value: {@code (integer,integer) -> integer}
   * @param param1 the first parameter in the lambda
   * @param param2 the second parameter in the lambda
   * @return a single or multi valued expression combining the two parameters with the given lambda
   * @throws SolrException if neither parameter is single-valued
   */
  public static IntValueStream createIntLambdaFunction(String name, TwoIntInIntOutLambda lambda, IntValueStream param1, IntValueStream param2) throws SolrException {
    if (param1 instanceof IntValue && param2 instanceof IntValue) {
      return new TwoIntValueInIntValueOutFunction(name,lambda,(IntValue)param1,(IntValue)param2);
    } else if (param1 instanceof IntValue) {
      return new IntValueIntStreamInIntStreamOutFunction(name,lambda,(IntValue)param1,param2);
    } else if (param2 instanceof IntValue) {
      return new IntStreamIntValueInIntStreamOutFunction(name,lambda,param1,(IntValue)param2);
    } else {
      throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function requires at least 1 single-valued parameter.");
    }
  }
  /**
   * Forwards the creation of the function to {@link #createIntLambdaFunction(String, TwoIntInIntOutLambda, IntValue[], boolean)},
   * using {@value #defaultMultiExistsMethod} for the last argument ({@code allMustExist}).
   *
   * @param name name for the function
   * @param lambda the associative function used to reduce the values: {@code (boolean, boolean) -> boolean}
   * @param params the expressions to reduce
   * @return a single-value expression that reduces the parameters with the given lambda
   */
  public static IntValue createIntLambdaFunction(String name, TwoIntInIntOutLambda lambda, IntValue[] params) {
    return createIntLambdaFunction(name,lambda,params,defaultMultiExistsMethod);
  }
  /**
   * Creates a function that associatively (order is guaranteed) reduces multiple
   * single-value integer expressions into a single-value integer expression for each document.
   * <br>
   * For a document, every parameter's value must exist for the resulting value to exist if {@code allMustExist} is true.
   * If {@code allMustExist} is false, only one of the parameters' values must exist.
   *
   * @param name name for the function
   * @param lambda the associative function used to reduce the values: {@code (integer, integer) -> integer}
   * @param params the expressions to reduce
   * @param allMustExist whether all parameters are required to exist
   * @return a single-value expression that reduces the parameters with the given lambda
   */
  public static IntValue createIntLambdaFunction(String name, TwoIntInIntOutLambda lambda, IntValue[] params, boolean allMustExist) {
    if (allMustExist) {
      return new MultiIntValueInIntValueOutRequireAllFunction(name,lambda,params);
    } else {
      return new MultiIntValueInIntValueOutRequireOneFunction(name,lambda,params);
    }
  }

  /* *********************
   *
   *  Long Functions
   *
   * *********************/

  /**
   * Creates a function that takes in either a single or multi valued long expression and returns the same type of expression with
   * the given lambda function applied to every value.
   *
   * @param name name for the function
   * @param lambda the function to be applied to every value: {@code (long) -> long}
   * @param param the expression to apply the lambda to
   * @return an expression the same type as was given with the lambda applied
   */
  public static LongValueStream createLongLambdaFunction(String name, LongInLongOutLambda lambda, LongValueStream param) {
    if (param instanceof LongValue) {
      return new LongValueInLongValueOutFunction(name,lambda,(LongValue)param);
    } else {
      return new LongStreamInLongStreamOutFunction(name,lambda,param);
    }
  }
  /**
   * Creates a function that takes in a multi-valued long expression and returns a single-valued long expression.
   * The given lambda is used to associatively (order not guaranteed) reduce all values for a document down to a single value.
   *
   * @param name name for the function
   * @param lambda the associative function used to reduce the values: {@code (boolean, boolean) -> boolean}
   * @param param the expression to be reduced per-document
   * @return a single-valued expression which has been reduced for every document
   */
  public static LongValue createLongLambdaFunction(String name, TwoLongInLongOutLambda lambda, LongValueStream param) {
    return new LongStreamInLongValueOutFunction(name,lambda,param);
  }
  /**
   * Creates a function that maps two longs to a single long.
   * This can take the following shapes:
   * <ul>
   * <li> Taking in two single-valued expressions and returning a single-valued expression which represents the lambda combination of the inputs.
   * <li> Taking in a single-valued expression and a multi-valued expression and returning a multi-valued expression which
   * represents the lambda combination of the single-value input with each of the values of the multi-value input.
   * <br>
   * The inputs can be either {@code func(single,multi)} or {@code func(multi,single)}.
   * </ul>
   *
   * @param name name for the function
   * @param lambda the function to be applied to every value: {@code (long,long) -> long}
   * @param param1 the first parameter in the lambda
   * @param param2 the second parameter in the lambda
   * @return a single or multi valued expression combining the two parameters with the given lambda
   * @throws SolrException if neither parameter is single-valued
   */
  public static LongValueStream createLongLambdaFunction(String name, TwoLongInLongOutLambda lambda, LongValueStream param1, LongValueStream param2) throws SolrException {
    if (param1 instanceof LongValue && param2 instanceof LongValue) {
      return new TwoLongValueInLongValueOutFunction(name,lambda,(LongValue)param1,(LongValue)param2);
    } else if (param1 instanceof LongValue) {
      return new LongValueLongStreamInLongStreamOutFunction(name,lambda,(LongValue)param1,param2);
    } else if (param2 instanceof LongValue) {
      return new LongStreamLongValueInLongStreamOutFunction(name,lambda,param1,(LongValue)param2);
    } else {
      throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function requires at least 1 single-valued parameter.");
    }
  }
  /**
   * Forwards the creation of the function to {@link #createLongLambdaFunction(String, TwoLongInLongOutLambda, LongValue[], boolean)},
   * using {@value #defaultMultiExistsMethod} for the last argument ({@code allMustExist}).
   *
   * @param name name for the function
   * @param lambda the associative function used to reduce the values: {@code (boolean, boolean) -> boolean}
   * @param params the expressions to reduce
   * @return a single-value expression that reduces the parameters with the given lambda
   */
  public static LongValue createLongLambdaFunction(String name, TwoLongInLongOutLambda lambda, LongValue[] params) {
    return createLongLambdaFunction(name,lambda,params,defaultMultiExistsMethod);
  }
  /**
   * Creates a function that associatively (order is guaranteed) reduces multiple
   * single-value long expressions into a single-value long expression for each document.
   * <br>
   * For a document, every parameter's value must exist for the resulting value to exist if {@code allMustExist} is true.
   * If {@code allMustExist} is false, only one of the parameters' values must exist.
   *
   * @param name name for the function
   * @param lambda the associative function used to reduce the values: {@code (long, long) -> long}
   * @param params the expressions to reduce
   * @param allMustExist whether all parameters are required to exist
   * @return a single-value expression that reduces the parameters with the given lambda
   */
  public static LongValue createLongLambdaFunction(String name, TwoLongInLongOutLambda lambda, LongValue[] params, boolean allMustExist) {
    if (allMustExist) {
      return new MultiLongValueInLongValueOutRequireAllFunction(name,lambda,params);
    } else {
      return new MultiLongValueInLongValueOutRequireOneFunction(name,lambda,params);
    }
  }

  /* *********************
   *
   *  Float Functions
   *
   * *********************/

  /**
   * Creates a function that takes in either a single or multi valued float expression and returns the same type of expression with
   * the given lambda function applied to every value.
   *
   * @param name name for the function
   * @param lambda the function to be applied to every value: {@code (float) -> float}
   * @param param the expression to apply the lambda to
   * @return an expression the same type as was given with the lambda applied
   */
  public static FloatValueStream createFloatLambdaFunction(String name, FloatInFloatOutLambda lambda, FloatValueStream param) {
    if (param instanceof FloatValue) {
      return new FloatValueInFloatValueOutFunction(name,lambda,(FloatValue)param);
    } else {
      return new FloatStreamInFloatStreamOutFunction(name,lambda,param);
    }
  }
  /**
   * Creates a function that takes in a multi-valued float expression and returns a single-valued float expression.
   * The given lambda is used to associatively (order not guaranteed) reduce all values for a document down to a single value.
   *
   * @param name name for the function
   * @param lambda the associative function used to reduce the values: {@code (float, float) -> float}
   * @param param the expression to be reduced per-document
   * @return a single-valued expression which has been reduced for every document
   */
  public static FloatValue createFloatLambdaFunction(String name, TwoFloatInFloatOutLambda lambda, FloatValueStream param) {
    return new FloatStreamInFloatValueOutFunction(name,lambda,param);
  }
  /**
   * Creates a function that maps two floats to a single float.
   * This can take the following shapes:
   * <ul>
   * <li> Taking in two single-valued expressions and returning a single-valued expression which represents the lambda combination of the inputs.
   * <li> Taking in a single-valued expression and a multi-valued expression and returning a multi-valued expression which
   * represents the lambda combination of the single-value input with each of the values of the multi-value input.
   * <br>
   * The inputs can be either {@code func(single,multi)} or {@code func(multi,single)}.
   * </ul>
   *
   * @param name name for the function
   * @param lambda the function to be applied to every value: {@code (float,float) -> float}
   * @param param1 the first parameter in the lambda
   * @param param2 the second parameter in the lambda
   * @return a single or multi valued expression combining the two parameters with the given lambda
   * @throws SolrException if neither parameter is single-valued
   */
  public static FloatValueStream createFloatLambdaFunction(String name, TwoFloatInFloatOutLambda lambda, FloatValueStream param1, FloatValueStream param2) throws SolrException {
    if (param1 instanceof FloatValue && param2 instanceof FloatValue) {
      return new TwoFloatValueInFloatValueOutFunction(name,lambda,(FloatValue)param1,(FloatValue)param2);
    } else if (param1 instanceof FloatValue) {
      return new FloatValueFloatStreamInFloatStreamOutFunction(name,lambda,(FloatValue)param1,param2);
    } else if (param2 instanceof FloatValue) {
      return new FloatStreamFloatValueInFloatStreamOutFunction(name,lambda,param1,(FloatValue)param2);
    } else {
      throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function requires at least 1 single-valued parameter.");
    }
  }
  /**
   * Forwards the creation of the function to {@link #createFloatLambdaFunction(String, TwoFloatInFloatOutLambda, FloatValue[], boolean)},
   * using {@value #defaultMultiExistsMethod} for the last argument ({@code allMustExist}).
   *
   * @param name name for the function
   * @param lambda the associative function used to reduce the values: {@code (boolean, boolean) -> boolean}
   * @param params the expressions to reduce
   * @return a single-value expression that reduces the parameters with the given lambda
   */
  public static FloatValue createFloatLambdaFunction(String name, TwoFloatInFloatOutLambda lambda, FloatValue[] params) {
    return createFloatLambdaFunction(name,lambda,params,defaultMultiExistsMethod);
  }
  /**
   * Creates a function that associatively (order is guaranteed) reduces multiple
   * single-value float expressions into a single-value float expression for each document.
   * <br>
   * For a document, every parameter's value must exist for the resulting value to exist if {@code allMustExist} is true.
   * If {@code allMustExist} is false, only one of the parameters' values must exist.
   *
   * @param name name for the function
   * @param lambda the associative function used to reduce the values: {@code (float, float) -> float}
   * @param params the expressions to reduce
   * @param allMustExist whether all parameters are required to exist
   * @return a single-value expression that reduces the parameters with the given lambda
   */
  public static FloatValue createFloatLambdaFunction(String name, TwoFloatInFloatOutLambda lambda, FloatValue[] params, boolean allMustExist) {
    if (allMustExist) {
      return new MultiFloatValueInFloatValueOutRequireAllFunction(name,lambda,params);
    } else {
      return new MultiFloatValueInFloatValueOutRequireOneFunction(name,lambda,params);
    }
  }

  /* *********************
   *
   *  Double Functions
   *
   * *********************/

  /**
   * Creates a function that takes in either a single or multi valued double expression and returns the same type of expression with
   * the given lambda function applied to every value.
   *
   * @param name name for the function
   * @param lambda the function to be applied to every value: {@code (double) -> double}
   * @param param the expression to apply the lambda to
   * @return an expression the same type as was given with the lambda applied
   */
  public static DoubleValueStream createDoubleLambdaFunction(String name, DoubleInDoubleOutLambda lambda, DoubleValueStream param) {
    if (param instanceof DoubleValue) {
      return new DoubleValueInDoubleValueOutFunction(name,lambda,(DoubleValue)param);
    } else {
      return new DoubleStreamInDoubleStreamOutFunction(name,lambda,param);
    }
  }
  /**
   * Creates a function that takes in a multi-valued double expression and returns a single-valued double expression.
   * The given lambda is used to associatively (order not guaranteed) reduce all values for a document down to a single value.
   *
   * @param name name for the function
   * @param lambda the associative function used to reduce the values: {@code (double, double) -> double}
   * @param param the expression to be reduced per-document
   * @return a single-valued expression which has been reduced for every document
   */
  public static DoubleValue createDoubleLambdaFunction(String name, TwoDoubleInDoubleOutLambda lambda, DoubleValueStream param) {
    return new DoubleStreamInDoubleValueOutFunction(name,lambda,param);
  }
  /**
   * Creates a function that maps two doubles to a single double.
   * This can take the following shapes:
   * <ul>
   * <li> Taking in two single-valued expressions and returning a single-valued expression which represents the lambda combination of the inputs.
   * <li> Taking in a single-valued expression and a multi-valued expression and returning a multi-valued expression which
   * represents the lambda combination of the single-value input with each of the values of the multi-value input.
   * <br>
   * The inputs can be either {@code func(single,multi)} or {@code func(multi,single)}.
   * </ul>
   *
   * @param name name for the function
   * @param lambda the function to be applied to every value: {@code (double,double) -> double}
   * @param param1 the first parameter in the lambda
   * @param param2 the second parameter in the lambda
   * @return a single or multi valued expression combining the two parameters with the given lambda
   * @throws SolrException if neither parameter is single-valued
   */
  public static DoubleValueStream createDoubleLambdaFunction(String name, TwoDoubleInDoubleOutLambda lambda, DoubleValueStream param1, DoubleValueStream param2) throws SolrException {
    if (param1 instanceof DoubleValue && param2 instanceof DoubleValue) {
      return new TwoDoubleValueInDoubleValueOutFunction(name,lambda,(DoubleValue)param1,(DoubleValue)param2);
    } else if (param1 instanceof DoubleValue) {
      return new DoubleValueDoubleStreamInDoubleStreamOutFunction(name,lambda,(DoubleValue)param1,param2);
    } else if (param2 instanceof DoubleValue) {
      return new DoubleStreamDoubleValueInDoubleStreamOutFunction(name,lambda,param1,(DoubleValue)param2);
    } else {
      throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function requires at least 1 single-valued parameter.");
    }
  }
  /**
   * Forwards the creation of the function to {@link #createDoubleLambdaFunction(String, TwoDoubleInDoubleOutLambda, DoubleValue[], boolean)},
   * using {@value #defaultMultiExistsMethod} for the last argument ({@code allMustExist}).
   *
   * @param name name for the function
   * @param lambda the associative function used to reduce the values: {@code (boolean, boolean) -> boolean}
   * @param params the expressions to reduce
   * @return a single-value expression that reduces the parameters with the given lambda
   */
  public static DoubleValue createDoubleLambdaFunction(String name, TwoDoubleInDoubleOutLambda lambda, DoubleValue[] params) {
    return createDoubleLambdaFunction(name,lambda,params,defaultMultiExistsMethod);
  }
  /**
   * Creates a function that associatively (order is guaranteed) reduces multiple
   * single-value double expressions into a single-value double expression for each document.
   * <br>
   * For a document, every parameter's value must exist for the resulting value to exist if {@code allMustExist} is true.
   * If {@code allMustExist} is false, only one of the parameters' values must exist.
   *
   * @param name name for the function
   * @param lambda the associative function used to reduce the values: {@code (double, double) -> double}
   * @param params the expressions to reduce
   * @param allMustExist whether all parameters are required to exist
   * @return a single-value expression that reduces the parameters with the given lambda
   */
  public static DoubleValue createDoubleLambdaFunction(String name, TwoDoubleInDoubleOutLambda lambda, DoubleValue[] params, boolean allMustExist) {
    if (allMustExist) {
      return new MultiDoubleValueInDoubleValueOutRequireAllFunction(name,lambda,params);
    } else {
      return new MultiDoubleValueInDoubleValueOutRequireOneFunction(name,lambda,params);
    }
  }

  /* *********************
   *
   *  Date Functions
   *
   * *********************/

  /**
   * Creates a function that takes in either a single or multi valued date expression and returns the same type of expression with
   * the given lambda function applied to every value.
   *
   * <p>
   * NOTE: The lambda must work on longs, not Date objects
   *
   * @param name name for the function
   * @param lambda the function to be applied to every value: {@code (long) -> long}
   * @param param the expression to apply the lambda to
   * @return an expression the same type as was given with the lambda applied
   */
  public static DateValueStream createDateLambdaFunction(String name, LongInLongOutLambda lambda, DateValueStream param) {
    if (param instanceof DateValue) {
      return new DateValueInDateValueOutFunction(name,lambda,(DateValue)param);
    } else {
      return new DateStreamInDateStreamOutFunction(name,lambda,param);
    }
  }
  /**
   * Creates a function that takes in a multi-valued date expression and returns a single-valued date expression.
   * The given lambda is used to associatively (order not guaranteed) reduce all values for a document down to a single value.
   *
   * <p>
   * NOTE: The lambda must work on longs, not Date objects
   *
   * @param name name for the function
   * @param lambda the associative function used to reduce the values: {@code (long, long) -> long}
   * @param param the expression to be reduced per-document
   * @return a single-valued expression which has been reduced for every document
   */
  public static DateValue createDateLambdaFunction(String name, TwoLongInLongOutLambda lambda, DateValueStream param) {
    return new DateStreamInDateValueOutFunction(name,lambda,param);
  }
  /**
   * Creates a function that maps two dates to a single date.
   * This can take the following shapes:
   * <ul>
   * <li> Taking in two single-valued expressions and returning a single-valued expression which represents the lambda combination of the inputs.
   * <li> Taking in a single-valued expression and a multi-valued expression and returning a multi-valued expression which
   * represents the lambda combination of the single-value input with each of the values of the multi-value input.
   * <br>
   * The inputs can be either {@code func(single,multi)} or {@code func(multi,single)}.
   * </ul>
   *
   * <p>
   * NOTE: The lambda must work on longs, not Date objects
   *
   * @param name name for the function
   * @param lambda the function to be applied to every value: {@code (long,long) -> long}
   * @param param1 the first parameter in the lambda
   * @param param2 the second parameter in the lambda
   * @return a single or multi valued expression combining the two parameters with the given lambda
   * @throws SolrException if neither parameter is single-valued
   */
  public static DateValueStream createDateLambdaFunction(String name, TwoLongInLongOutLambda lambda, DateValueStream param1, DateValueStream param2) throws SolrException {
    if (param1 instanceof DateValue && param2 instanceof DateValue) {
      return new TwoDateValueInDateValueOutFunction(name,lambda,(DateValue)param1,(DateValue)param2);
    } else if (param1 instanceof DateValue) {
      return new DateValueDateStreamInDateStreamOutFunction(name,lambda,(DateValue)param1,param2);
    } else if (param2 instanceof DateValue) {
      return new DateStreamDateValueInDateStreamOutFunction(name,lambda,param1,(DateValue)param2);
    } else {
      throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function requires at least 1 single-valued parameter.");
    }
  }
  /**
   * Forwards the creation of the function to {@link #createDateLambdaFunction(String, TwoLongInLongOutLambda, DateValue[], boolean)},
   * using {@value #defaultMultiExistsMethod} for the last argument ({@code allMustExist}).
   *
   * @param name name for the function
   * @param lambda the associative function used to reduce the values: {@code (boolean, boolean) -> boolean}
   * @param params the expressions to reduce
   * @return a single-value expression that reduces the parameters with the given lambda
   */
  public static DateValue createDateLambdaFunction(String name, TwoLongInLongOutLambda lambda, DateValue[] params) {
    return createDateLambdaFunction(name,lambda,params,defaultMultiExistsMethod);
  }
  /**
   * Creates a function that associatively (order is guaranteed) reduces multiple
   * single-value date expressions into a single-value date expression for each document.
   * <br>
   * For a document, every parameter's value must exist for the resulting value to exist if {@code allMustExist} is true.
   * If {@code allMustExist} is false, only one of the parameters' values must exist.
   *
   * <p>
   * NOTE: The lambda must work on longs, not Date objects
   *
   * @param name name for the function
   * @param lambda the associative function used to reduce the values: {@code (long, long) -> long}
   * @param params the expressions to reduce
   * @param allMustExist whether all parameters are required to exist
   * @return a single-value expression that reduces the parameters with the given lambda
   */
  public static DateValue createDateLambdaFunction(String name, TwoLongInLongOutLambda lambda, DateValue[] params, boolean allMustExist) {
    if (allMustExist) {
      return new MultiDateValueInDateValueOutRequireAllFunction(name,lambda,params);
    } else {
      return new MultiDateValueInDateValueOutRequireOneFunction(name,lambda,params);
    }
  }

  /* *********************
   *
   *  String Functions
   *
   * *********************/

  /**
   * Creates a function that takes in either a single or multi valued string expression and returns the same type of expression with
   * the given lambda function applied to every value.
   *
   * @param name name for the function
   * @param lambda the function to be applied to every value: {@code (String) -> String}
   * @param param the expression to apply the lambda to
   * @return an expression the same type as was given with the lambda applied
   */
  public static StringValueStream createStringLambdaFunction(String name, StringInStringOutLambda lambda, StringValueStream param) {
    if (param instanceof StringValue) {
      return new StringValueInStringValueOutFunction(name,lambda,(StringValue)param);
    } else {
      return new StringStreamInStringStreamOutFunction(name,lambda,param);
    }
  }
  /**
   * Creates a function that takes in a multi-valued string expression and returns a single-valued string expression.
   * The given lambda is used to associatively (order not guaranteed) reduce all values for a document down to a single value.
   *
   * @param name name for the function
   * @param lambda the associative function used to reduce the values: {@code (String, String) -> String}
   * @param param the expression to be reduced per-document
   * @return a single-valued expression which has been reduced for every document
   */
  public static StringValue createStringLambdaFunction(String name, TwoStringInStringOutLambda lambda, StringValueStream param) {
    return new StringStreamInStringValueOutFunction(name,lambda,param);
  }
  /**
   * Creates a function that maps two strings to a single string.
   * This can take the following shapes:
   * <ul>
   * <li> Taking in two single-valued expressions and returning a single-valued expression which represents the lambda combination of the inputs.
   * <li> Taking in a single-valued expression and a multi-valued expression and returning a multi-valued expression which
   * represents the lambda combination of the single-value input with each of the values of the multi-value input.
   * <br>
   * The inputs can be either {@code func(single,multi)} or {@code func(multi,single)}.
   * </ul>
   *
   * @param name name for the function
   * @param lambda the function to be applied to every value: {@code (String,String) -> String}
   * @param param1 the first parameter in the lambda
   * @param param2 the second parameter in the lambda
   * @return a single or multi valued expression combining the two parameters with the given lambda
   * @throws SolrException if neither parameter is single-valued
   */
  public static StringValueStream createStringLambdaFunction(String name, TwoStringInStringOutLambda lambda, StringValueStream param1, StringValueStream param2) throws SolrException {
    if (param1 instanceof StringValue && param2 instanceof StringValue) {
      return new TwoStringValueInStringValueOutFunction(name,lambda,(StringValue)param1,(StringValue)param2);
    } else if (param1 instanceof StringValue) {
      return new StringValueStringStreamInStringStreamOutFunction(name,lambda,(StringValue)param1,param2);
    } else if (param2 instanceof StringValue) {
      return new StringStreamStringValueInStringStreamOutFunction(name,lambda,param1,(StringValue)param2);
    } else {
      throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function requires at least 1 single-valued parameter.");
    }
  }
  /**
   * Forwards the creation of the function to {@link #createStringLambdaFunction(String, TwoStringInStringOutLambda, StringValue[], boolean)},
   * using {@value #defaultMultiExistsMethod} for the last argument ({@code allMustExist}).
   *
   * @param name name for the function
   * @param lambda the associative function used to reduce the values: {@code (boolean, boolean) -> boolean}
   * @param params the expressions to reduce
   * @return a single-value expression that reduces the parameters with the given lambda
   */
  public static StringValue createStringLambdaFunction(String name, TwoStringInStringOutLambda lambda, StringValue[] params) {
    return createStringLambdaFunction(name,lambda,params,defaultMultiExistsMethod);
  }
  /**
   * Creates a function that associatively (order is guaranteed) reduces multiple
   * single-value string expressions into a single-value string expression for each document.
   * <br>
   * For a document, every parameter's value must exist for the resulting value to exist if {@code allMustExist} is true.
   * If {@code allMustExist} is false, only one of the parameters' values must exist.
   *
   * @param name name for the function
   * @param lambda the associative function used to reduce the values: {@code (String, String) -> String}
   * @param params the expressions to reduce
   * @param allMustExist whether all parameters are required to exist
   * @return a single-value expression that reduces the parameters with the given lambda
   */
  public static StringValue createStringLambdaFunction(String name, TwoStringInStringOutLambda lambda, StringValue[] params, boolean allMustExist) {
    if (allMustExist) {
      return new MultiStringValueInStringValueOutRequireAllFunction(name,lambda,params);
    } else {
      return new MultiStringValueInStringValueOutRequireOneFunction(name,lambda,params);
    }
  }


  /*
   * Single Parameter
   */
  // Boolean Out
  @FunctionalInterface
  public static interface BoolInBoolOutLambda   { boolean apply(boolean a); }
  @FunctionalInterface
  public static interface IntInBoolOutLambda    { boolean apply(int     a); }
  @FunctionalInterface
  public static interface LongInBoolOutLambda   { boolean apply(long    a); }
  @FunctionalInterface
  public static interface FloatInBoolOutLambda  { boolean apply(float   a); }
  @FunctionalInterface
  public static interface DoubleInBoolOutLambda { boolean apply(double  a); }
  @FunctionalInterface
  public static interface StringInBoolOutLambda { boolean apply(double  a); }
  // Int Out
  @FunctionalInterface
  public static interface BoolInIntOutLambda   { int apply(boolean a); }
  @FunctionalInterface
  public static interface IntInIntOutLambda    { int apply(int     a); }
  @FunctionalInterface
  public static interface LongInIntOutLambda   { int apply(long    a); }
  @FunctionalInterface
  public static interface FloatInIntOutLambda  { int apply(float   a); }
  @FunctionalInterface
  public static interface DoubleInIntOutLambda { int apply(double  a); }
  @FunctionalInterface
  public static interface StringInIntOutLambda { int apply(double  a); }
  // Long Out
  @FunctionalInterface
  public static interface BoolInLongOutLambda   { long apply(boolean a); }
  @FunctionalInterface
  public static interface IntInLongOutLambda    { long apply(int     a); }
  @FunctionalInterface
  public static interface LongInLongOutLambda   { long apply(long    a); }
  @FunctionalInterface
  public static interface FloatInLongOutLambda  { long apply(float   a); }
  @FunctionalInterface
  public static interface DoubleInLongOutLambda { long apply(double  a); }
  @FunctionalInterface
  public static interface StringInLongOutLambda { long apply(double  a); }
  // Float Out
  @FunctionalInterface
  public static interface BoolInFloatOutLambda   { float apply(boolean a); }
  @FunctionalInterface
  public static interface IntInFloatOutLambda    { float apply(int     a); }
  @FunctionalInterface
  public static interface LongInFloatOutLambda   { float apply(long    a); }
  @FunctionalInterface
  public static interface FloatInFloatOutLambda  { float apply(float   a); }
  @FunctionalInterface
  public static interface DoubleInFloatOutLambda { float apply(double  a); }
  @FunctionalInterface
  public static interface StringInFloatOutLambda { float apply(String  a); }
  //Double Out
  @FunctionalInterface
  public static interface BoolInDoubleOutLambda   { double apply(boolean a); }
  @FunctionalInterface
  public static interface IntInDoubleOutLambda    { double apply(int     a); }
  @FunctionalInterface
  public static interface LongInDoubleOutLambda   { double apply(long    a); }
  @FunctionalInterface
  public static interface FloatInDoubleOutLambda  { double apply(float   a); }
  @FunctionalInterface
  public static interface DoubleInDoubleOutLambda { double apply(double  a); }
  @FunctionalInterface
  public static interface StringInDoubleOutLambda { double apply(String  a); }
  //String Out
  @FunctionalInterface
  public static interface BoolInStringOutLambda   { String apply(boolean a); }
  @FunctionalInterface
  public static interface IntInStringOutLambda    { String apply(int     a); }
  @FunctionalInterface
  public static interface LongInStringOutLambda   { String apply(long    a); }
  @FunctionalInterface
  public static interface FloatInStringOutLambda  { String apply(float   a); }
  @FunctionalInterface
  public static interface DoubleInStringOutLambda { String apply(double  a); }
  @FunctionalInterface
  public static interface StringInStringOutLambda { String apply(String  a); }

  /*
   * Two Parameters
   */
  //Boolean Out
  @FunctionalInterface
  public static interface TwoBoolInBoolOutLambda   { boolean apply(boolean a, boolean b); }
  @FunctionalInterface
  public static interface TwoIntInBoolOutLambda    { boolean apply(int     a, int     b); }
  @FunctionalInterface
  public static interface TwoLongInBoolOutLambda   { boolean apply(long    a, long    b); }
  @FunctionalInterface
  public static interface TwoFloatInBoolOutLambda  { boolean apply(float   a, float   b); }
  @FunctionalInterface
  public static interface TwoDoubleInBoolOutLambda { boolean apply(double  a, double  b); }
  @FunctionalInterface
  public static interface TwoStringInBoolOutLambda { boolean apply(double  a, double  b); }
  //Int Out
  @FunctionalInterface
  public static interface TwoBoolInIntOutLambda   { int apply(boolean a, boolean b); }
  @FunctionalInterface
  public static interface TwoIntInIntOutLambda    { int apply(int     a, int     b); }
  @FunctionalInterface
  public static interface TwoLongInIntOutLambda   { int apply(long    a, long    b); }
  @FunctionalInterface
  public static interface TwoFloatInIntOutLambda  { int apply(float   a, float   b); }
  @FunctionalInterface
  public static interface TwoDoubleInIntOutLambda { int apply(double  a, double  b); }
  @FunctionalInterface
  public static interface TwoStringInIntOutLambda { int apply(double  a, double  b); }
  //Long Out
  @FunctionalInterface
  public static interface TwoBoolInLongOutLambda   { long apply(boolean a, boolean b); }
  @FunctionalInterface
  public static interface TwoIntInLongOutLambda    { long apply(int     a, int     b); }
  @FunctionalInterface
  public static interface TwoLongInLongOutLambda   { long apply(long    a, long    b); }
  @FunctionalInterface
  public static interface TwoFloatInLongOutLambda  { long apply(float   a, float   b); }
  @FunctionalInterface
  public static interface TwoDoubleInLongOutLambda { long apply(double  a, double  b); }
  @FunctionalInterface
  public static interface TwoStringInLongOutLambda { long apply(double  a, double  b); }
  //Float Out
  @FunctionalInterface
  public static interface TwoBoolInFloatOutLambda   { float apply(boolean a, boolean b); }
  @FunctionalInterface
  public static interface TwoIntInFloatOutLambda    { float apply(int     a, int     b); }
  @FunctionalInterface
  public static interface TwoLongInFloatOutLambda   { float apply(long    a, long    b); }
  @FunctionalInterface
  public static interface TwoFloatInFloatOutLambda  { float apply(float   a, float   b); }
  @FunctionalInterface
  public static interface TwoDoubleInFloatOutLambda { float apply(double  a, double  b); }
  @FunctionalInterface
  public static interface TwoStringInFloatOutLambda { float apply(String  a, String  b); }
  //Double Out
  @FunctionalInterface
  public static interface TwoBoolInDoubleOutLambda   { double apply(boolean a, boolean b); }
  @FunctionalInterface
  public static interface TwoIntInDoubleOutLambda    { double apply(int     a, int     b); }
  @FunctionalInterface
  public static interface TwoLongInDoubleOutLambda   { double apply(long    a, long    b); }
  @FunctionalInterface
  public static interface TwoFloatInDoubleOutLambda  { double apply(float   a, float   b); }
  @FunctionalInterface
  public static interface TwoDoubleInDoubleOutLambda { double apply(double  a, double  b); }
  @FunctionalInterface
  public static interface TwoStringInDoubleOutLambda { double apply(String  a, String  b); }
  //String Out
  @FunctionalInterface
  public static interface TwoBoolInStringOutLambda   { String apply(boolean a, boolean b); }
  @FunctionalInterface
  public static interface TwoIntInStringOutLambda    { String apply(int     a, int     b); }
  @FunctionalInterface
  public static interface TwoLongInStringOutLambda   { String apply(long    a, long    b); }
  @FunctionalInterface
  public static interface TwoFloatInStringOutLambda  { String apply(float   a, float   b); }
  @FunctionalInterface
  public static interface TwoDoubleInStringOutLambda { String apply(double  a, double  b); }
  @FunctionalInterface
  public static interface TwoStringInStringOutLambda { String apply(String  a, String  b); }
}
class BooleanValueInBooleanValueOutFunction extends AbstractBooleanValue {
  private final BooleanValue param;
  private final BoolInBoolOutLambda lambda;
  private final String name;
  private final String exprStr;
  private final ExpressionType funcType;

  public BooleanValueInBooleanValueOutFunction(String name, BoolInBoolOutLambda lambda, BooleanValue param) {
    this.name = name;
    this.lambda = lambda;
    this.param = param;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,param);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,param);
  }

  private boolean exists = false;

  @Override
  public boolean getBoolean() {
    boolean value = lambda.apply(param.getBoolean());
    exists = param.exists();
    return value;
  }
  @Override
  public boolean exists() {
    return exists;
  }

  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getExpressionStr() {
    return exprStr;
  }
  @Override
  public ExpressionType getExpressionType() {
    return funcType;
  }
}
class BooleanStreamInBooleanStreamOutFunction extends AbstractBooleanValueStream {
  private final BooleanValueStream param;
  private final BoolInBoolOutLambda lambda;
  private final String name;
  private final String exprStr;
  private final ExpressionType funcType;

  public BooleanStreamInBooleanStreamOutFunction(String name, BoolInBoolOutLambda lambda, BooleanValueStream param) {
    this.name = name;
    this.lambda = lambda;
    this.param = param;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,param);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,param);
  }

  @Override
  public void streamBooleans(BooleanConsumer cons) {
    param.streamBooleans(value -> cons.accept(lambda.apply(value)));
  }

  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getExpressionStr() {
    return exprStr;
  }
  @Override
  public ExpressionType getExpressionType() {
    return funcType;
  }
}
class BooleanStreamInBooleanValueOutFunction extends AbstractBooleanValue implements BooleanConsumer {
  private final BooleanValueStream param;
  private final TwoBoolInBoolOutLambda lambda;
  private final String name;
  private final String exprStr;
  private final ExpressionType funcType;

  public BooleanStreamInBooleanValueOutFunction(String name, TwoBoolInBoolOutLambda lambda, BooleanValueStream param) {
    this.name = name;
    this.lambda = lambda;
    this.param = param;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,param);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,param);
  }

  private boolean exists = false;
  private boolean value;

  @Override
  public boolean getBoolean() {
    exists = false;
    param.streamBooleans(this);
    return value;
  }
  @Override
  public boolean exists() {
    return exists;
  }
  public void accept(boolean paramValue) {
    if (!exists) {
      exists = true;
      value = paramValue;
    } else {
      value = lambda.apply(value, paramValue);
    }
  }

  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getExpressionStr() {
    return exprStr;
  }
  @Override
  public ExpressionType getExpressionType() {
    return funcType;
  }
}
class TwoBooleanValueInBooleanValueOutFunction extends AbstractBooleanValue {
  private final BooleanValue param1;
  private final BooleanValue param2;
  private final TwoBoolInBoolOutLambda lambda;
  private final String name;
  private final String exprStr;
  private final ExpressionType funcType;

  public TwoBooleanValueInBooleanValueOutFunction(String name, TwoBoolInBoolOutLambda lambda, BooleanValue param1, BooleanValue param2) {
    this.name = name;
    this.lambda = lambda;
    this.param1 = param1;
    this.param2 = param2;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,param1,param2);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,param1,param2);
  }

  private boolean exists = false;

  @Override
  public boolean getBoolean() {
    boolean value = lambda.apply(param1.getBoolean(), param2.getBoolean());
    exists = param1.exists() && param2.exists();
    return value;
  }
  @Override
  public boolean exists() {
    return exists;
  }

  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getExpressionStr() {
    return exprStr;
  }
  @Override
  public ExpressionType getExpressionType() {
    return funcType;
  }
}
class BooleanValueBooleanStreamInBooleanStreamOutFunction extends AbstractBooleanValueStream {
  private final BooleanValue param1;
  private final BooleanValueStream param2;
  private final TwoBoolInBoolOutLambda lambda;
  private final String name;
  private final String exprStr;
  private final ExpressionType funcType;

  public BooleanValueBooleanStreamInBooleanStreamOutFunction(String name, TwoBoolInBoolOutLambda lambda, BooleanValue param1, BooleanValueStream param2) {
    this.name = name;
    this.lambda = lambda;
    this.param1 = param1;
    this.param2 = param2;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,param1,param2);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,param1,param2);
  }

  @Override
  public void streamBooleans(BooleanConsumer cons) {
    boolean value1 = param1.getBoolean();
    if (param1.exists()) {
      param2.streamBooleans(value2 -> cons.accept(lambda.apply(value1,value2)));
    }
  }

  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getExpressionStr() {
    return exprStr;
  }
  @Override
  public ExpressionType getExpressionType() {
    return funcType;
  }
}
class BooleanStreamBooleanValueInBooleanStreamOutFunction extends AbstractBooleanValueStream {
  private final BooleanValueStream param1;
  private final BooleanValue param2;
  private final TwoBoolInBoolOutLambda lambda;
  private final String name;
  private final String exprStr;
  private final ExpressionType funcType;

  public BooleanStreamBooleanValueInBooleanStreamOutFunction(String name, TwoBoolInBoolOutLambda lambda, BooleanValueStream param1, BooleanValue param2) {
    this.name = name;
    this.lambda = lambda;
    this.param1 = param1;
    this.param2 = param2;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,param1,param2);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,param1,param2);
  }

  @Override
  public void streamBooleans(BooleanConsumer cons) {
    boolean value2 = param2.getBoolean();
    if (param2.exists()) {
      param1.streamBooleans(value1 -> cons.accept(lambda.apply(value1,value2)));
    }
  }

  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getExpressionStr() {
    return exprStr;
  }
  @Override
  public ExpressionType getExpressionType() {
    return funcType;
  }
}
abstract class MultiBooleanValueInBooleanValueOutFunction extends AbstractBooleanValue {
  protected final BooleanValue[] params;
  protected final TwoBoolInBoolOutLambda lambda;
  private final String name;
  private final String exprStr;
  private final ExpressionType funcType;

  public MultiBooleanValueInBooleanValueOutFunction(String name, TwoBoolInBoolOutLambda lambda, BooleanValue[] params) {
    this.name = name;
    this.lambda = lambda;
    this.params = params;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,params);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,params);
  }

  protected boolean exists = false;
  protected boolean temp;
  @Override
  public boolean exists() {
    return exists;
  }

  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getExpressionStr() {
    return exprStr;
  }
  @Override
  public ExpressionType getExpressionType() {
    return funcType;
  }
}
class MultiBooleanValueInBooleanValueOutRequireAllFunction extends MultiBooleanValueInBooleanValueOutFunction {

  public MultiBooleanValueInBooleanValueOutRequireAllFunction(String name, TwoBoolInBoolOutLambda lambda, BooleanValue[] params) {
    super(name, lambda, params);
  }

  @Override
  public boolean getBoolean() {
    boolean value = params[0].getBoolean();
    exists = params[0].exists();
    for (int i = 1; i < params.length && exists; ++i) {
      value = lambda.apply(value, params[i].getBoolean());
      exists = params[i].exists();
    }
    return value;
  }
}
class MultiBooleanValueInBooleanValueOutRequireOneFunction extends MultiBooleanValueInBooleanValueOutFunction {

  public MultiBooleanValueInBooleanValueOutRequireOneFunction(String name, TwoBoolInBoolOutLambda lambda, BooleanValue[] params) {
    super(name, lambda, params);
  }

  @Override
  public boolean getBoolean() {
    int i = -1;
    boolean value = false;
    exists = false;
    while (++i < params.length) {
      value = params[i].getBoolean();
      exists = params[i].exists();
      if (exists) {
        break;
      }
    }
    while (++i < params.length) {
      temp = params[i].getBoolean();
      if (params[i].exists()) {
        value = lambda.apply(value, temp);
      }
    }
    return value;
  }
}
class IntValueInIntValueOutFunction extends AbstractIntValue {
  private final IntValue param;
  private final IntInIntOutLambda lambda;
  private final String name;
  private final String exprStr;
  private final ExpressionType funcType;

  public IntValueInIntValueOutFunction(String name, IntInIntOutLambda lambda, IntValue param) {
    this.name = name;
    this.lambda = lambda;
    this.param = param;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,param);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,param);
  }

  private boolean exists = false;

  @Override
  public int getInt() {
    int value = lambda.apply(param.getInt());
    exists = param.exists();
    return value;
  }
  @Override
  public boolean exists() {
    return exists;
  }

  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getExpressionStr() {
    return exprStr;
  }
  @Override
  public ExpressionType getExpressionType() {
    return funcType;
  }
}
class IntStreamInIntStreamOutFunction extends AbstractIntValueStream {
  private final IntValueStream param;
  private final IntInIntOutLambda lambda;
  private final String name;
  private final String exprStr;
  private final ExpressionType funcType;

  public IntStreamInIntStreamOutFunction(String name, IntInIntOutLambda lambda, IntValueStream param) {
    this.name = name;
    this.lambda = lambda;
    this.param = param;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,param);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,param);
  }

  @Override
  public void streamInts(IntConsumer cons) {
    param.streamInts(value -> cons.accept(lambda.apply(value)));
  }

  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getExpressionStr() {
    return exprStr;
  }
  @Override
  public ExpressionType getExpressionType() {
    return funcType;
  }
}
class IntStreamInIntValueOutFunction extends AbstractIntValue implements IntConsumer {
  private final IntValueStream param;
  private final TwoIntInIntOutLambda lambda;
  private final String name;
  private final String exprStr;
  private final ExpressionType funcType;

  public IntStreamInIntValueOutFunction(String name, TwoIntInIntOutLambda lambda, IntValueStream param) {
    this.name = name;
    this.lambda = lambda;
    this.param = param;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,param);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,param);
  }

  private boolean exists = false;
  private int value;

  @Override
  public int getInt() {
    exists = false;
    param.streamInts(this);
    return value;
  }
  @Override
  public boolean exists() {
    return exists;
  }
  public void accept(int paramValue) {
    if (!exists) {
      exists = true;
      value = paramValue;
    } else {
      value = lambda.apply(value, paramValue);
    }
  }

  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getExpressionStr() {
    return exprStr;
  }
  @Override
  public ExpressionType getExpressionType() {
    return funcType;
  }
}
class TwoIntValueInIntValueOutFunction extends AbstractIntValue {
  private final IntValue param1;
  private final IntValue param2;
  private final TwoIntInIntOutLambda lambda;
  private final String name;
  private final String exprStr;
  private final ExpressionType funcType;

  public TwoIntValueInIntValueOutFunction(String name, TwoIntInIntOutLambda lambda, IntValue param1, IntValue param2) {
    this.name = name;
    this.lambda = lambda;
    this.param1 = param1;
    this.param2 = param2;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,param1,param2);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,param1,param2);
  }

  private boolean exists = false;

  @Override
  public int getInt() {
    int value = lambda.apply(param1.getInt(), param2.getInt());
    exists = param1.exists() && param2.exists();
    return value;
  }
  @Override
  public boolean exists() {
    return exists;
  }

  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getExpressionStr() {
    return exprStr;
  }
  @Override
  public ExpressionType getExpressionType() {
    return funcType;
  }
}
class IntValueIntStreamInIntStreamOutFunction extends AbstractIntValueStream {
  private final IntValue param1;
  private final IntValueStream param2;
  private final TwoIntInIntOutLambda lambda;
  private final String name;
  private final String exprStr;
  private final ExpressionType funcType;

  public IntValueIntStreamInIntStreamOutFunction(String name, TwoIntInIntOutLambda lambda, IntValue param1, IntValueStream param2) {
    this.name = name;
    this.lambda = lambda;
    this.param1 = param1;
    this.param2 = param2;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,param1,param2);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,param1,param2);
  }

  @Override
  public void streamInts(IntConsumer cons) {
    int value1 = param1.getInt();
    if (param1.exists()) {
      param2.streamInts(value2 -> cons.accept(lambda.apply(value1,value2)));
    }
  }

  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getExpressionStr() {
    return exprStr;
  }
  @Override
  public ExpressionType getExpressionType() {
    return funcType;
  }
}
class IntStreamIntValueInIntStreamOutFunction extends AbstractIntValueStream {
  private final IntValueStream param1;
  private final IntValue param2;
  private final TwoIntInIntOutLambda lambda;
  private final String name;
  private final String exprStr;
  private final ExpressionType funcType;

  public IntStreamIntValueInIntStreamOutFunction(String name, TwoIntInIntOutLambda lambda, IntValueStream param1, IntValue param2) {
    this.name = name;
    this.lambda = lambda;
    this.param1 = param1;
    this.param2 = param2;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,param1,param2);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,param1,param2);
  }

  @Override
  public void streamInts(IntConsumer cons) {
    int value2 = param2.getInt();
    if (param2.exists()) {
      param1.streamInts(value1 -> cons.accept(lambda.apply(value1,value2)));
    }
  }

  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getExpressionStr() {
    return exprStr;
  }
  @Override
  public ExpressionType getExpressionType() {
    return funcType;
  }
}
abstract class MultiIntValueInIntValueOutFunction extends AbstractIntValue {
  protected final IntValue[] params;
  protected final TwoIntInIntOutLambda lambda;
  private final String name;
  private final String exprStr;
  private final ExpressionType funcType;

  public MultiIntValueInIntValueOutFunction(String name, TwoIntInIntOutLambda lambda, IntValue[] params) {
    this.name = name;
    this.lambda = lambda;
    this.params = params;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,params);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,params);
  }

  protected boolean exists = false;
  protected int temp;
  @Override
  public boolean exists() {
    return exists;
  }

  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getExpressionStr() {
    return exprStr;
  }
  @Override
  public ExpressionType getExpressionType() {
    return funcType;
  }
}
class MultiIntValueInIntValueOutRequireAllFunction extends MultiIntValueInIntValueOutFunction {

  public MultiIntValueInIntValueOutRequireAllFunction(String name, TwoIntInIntOutLambda lambda, IntValue[] params) {
    super(name, lambda, params);
  }

  @Override
  public int getInt() {
    int value = params[0].getInt();
    exists = params[0].exists();
    for (int i = 1; i < params.length && exists; ++i) {
      value = lambda.apply(value, params[i].getInt());
      exists = params[i].exists();
    }
    return value;
  }
}
class MultiIntValueInIntValueOutRequireOneFunction extends MultiIntValueInIntValueOutFunction {

  public MultiIntValueInIntValueOutRequireOneFunction(String name, TwoIntInIntOutLambda lambda, IntValue[] params) {
    super(name, lambda, params);
  }

  @Override
  public int getInt() {
    int i = -1;
    int value = 0;
    exists = false;
    while (++i < params.length) {
      value = params[i].getInt();
      exists = params[i].exists();
      if (exists) {
        break;
      }
    }
    while (++i < params.length) {
      temp = params[i].getInt();
      if (params[i].exists()) {
        value = lambda.apply(value, temp);
      }
    }
    return value;
  }
}
class LongValueInLongValueOutFunction extends AbstractLongValue {
  private final LongValue param;
  private final LongInLongOutLambda lambda;
  private final String name;
  private final String exprStr;
  private final ExpressionType funcType;

  public LongValueInLongValueOutFunction(String name, LongInLongOutLambda lambda, LongValue param) {
    this.name = name;
    this.lambda = lambda;
    this.param = param;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,param);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,param);
  }

  private boolean exists = false;

  @Override
  public long getLong() {
    long value = lambda.apply(param.getLong());
    exists = param.exists();
    return value;
  }
  @Override
  public boolean exists() {
    return exists;
  }

  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getExpressionStr() {
    return exprStr;
  }
  @Override
  public ExpressionType getExpressionType() {
    return funcType;
  }
}
class LongStreamInLongStreamOutFunction extends AbstractLongValueStream {
  private final LongValueStream param;
  private final LongInLongOutLambda lambda;
  private final String name;
  private final String exprStr;
  private final ExpressionType funcType;

  public LongStreamInLongStreamOutFunction(String name, LongInLongOutLambda lambda, LongValueStream param) {
    this.name = name;
    this.lambda = lambda;
    this.param = param;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,param);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,param);
  }

  @Override
  public void streamLongs(LongConsumer cons) {
    param.streamLongs(value -> cons.accept(lambda.apply(value)));
  }

  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getExpressionStr() {
    return exprStr;
  }
  @Override
  public ExpressionType getExpressionType() {
    return funcType;
  }
}
class LongStreamInLongValueOutFunction extends AbstractLongValue implements LongConsumer {
  private final LongValueStream param;
  private final TwoLongInLongOutLambda lambda;
  private final String name;
  private final String exprStr;
  private final ExpressionType funcType;

  public LongStreamInLongValueOutFunction(String name, TwoLongInLongOutLambda lambda, LongValueStream param) {
    this.name = name;
    this.lambda = lambda;
    this.param = param;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,param);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,param);
  }

  private boolean exists = false;
  private long value;

  @Override
  public long getLong() {
    exists = false;
    param.streamLongs(this);
    return value;
  }
  @Override
  public boolean exists() {
    return exists;
  }
  public void accept(long paramValue) {
    if (!exists) {
      exists = true;
      value = paramValue;
    } else {
      value = lambda.apply(value, paramValue);
    }
  }

  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getExpressionStr() {
    return exprStr;
  }
  @Override
  public ExpressionType getExpressionType() {
    return funcType;
  }
}
class TwoLongValueInLongValueOutFunction extends AbstractLongValue {
  private final LongValue param1;
  private final LongValue param2;
  private final TwoLongInLongOutLambda lambda;
  private final String name;
  private final String exprStr;
  private final ExpressionType funcType;

  public TwoLongValueInLongValueOutFunction(String name, TwoLongInLongOutLambda lambda, LongValue param1, LongValue param2) {
    this.name = name;
    this.lambda = lambda;
    this.param1 = param1;
    this.param2 = param2;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,param1,param2);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,param1,param2);
  }

  private boolean exists = false;

  @Override
  public long getLong() {
    long value = lambda.apply(param1.getLong(), param2.getLong());
    exists = param1.exists() && param2.exists();
    return value;
  }
  @Override
  public boolean exists() {
    return exists;
  }

  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getExpressionStr() {
    return exprStr;
  }
  @Override
  public ExpressionType getExpressionType() {
    return funcType;
  }
}
class LongValueLongStreamInLongStreamOutFunction extends AbstractLongValueStream {
  private final LongValue param1;
  private final LongValueStream param2;
  private final TwoLongInLongOutLambda lambda;
  private final String name;
  private final String exprStr;
  private final ExpressionType funcType;

  public LongValueLongStreamInLongStreamOutFunction(String name, TwoLongInLongOutLambda lambda, LongValue param1, LongValueStream param2) {
    this.name = name;
    this.lambda = lambda;
    this.param1 = param1;
    this.param2 = param2;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,param1,param2);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,param1,param2);
  }

  @Override
  public void streamLongs(LongConsumer cons) {
    long value1 = param1.getLong();
    if (param1.exists()) {
      param2.streamLongs(value2 -> cons.accept(lambda.apply(value1,value2)));
    }
  }

  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getExpressionStr() {
    return exprStr;
  }
  @Override
  public ExpressionType getExpressionType() {
    return funcType;
  }
}
class LongStreamLongValueInLongStreamOutFunction extends AbstractLongValueStream {
  private final LongValueStream param1;
  private final LongValue param2;
  private final TwoLongInLongOutLambda lambda;
  private final String name;
  private final String exprStr;
  private final ExpressionType funcType;

  public LongStreamLongValueInLongStreamOutFunction(String name, TwoLongInLongOutLambda lambda, LongValueStream param1, LongValue param2) {
    this.name = name;
    this.lambda = lambda;
    this.param1 = param1;
    this.param2 = param2;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,param1,param2);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,param1,param2);
  }

  @Override
  public void streamLongs(LongConsumer cons) {
    long value2 = param2.getLong();
    if (param2.exists()) {
      param1.streamLongs(value1 -> cons.accept(lambda.apply(value1,value2)));
    }
  }

  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getExpressionStr() {
    return exprStr;
  }
  @Override
  public ExpressionType getExpressionType() {
    return funcType;
  }
}
abstract class MultiLongValueInLongValueOutFunction extends AbstractLongValue {
  protected final LongValue[] params;
  protected final TwoLongInLongOutLambda lambda;
  private final String name;
  private final String exprStr;
  private final ExpressionType funcType;

  public MultiLongValueInLongValueOutFunction(String name, TwoLongInLongOutLambda lambda, LongValue[] params) {
    this.name = name;
    this.lambda = lambda;
    this.params = params;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,params);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,params);
  }

  protected boolean exists = false;
  protected long temp;
  @Override
  public boolean exists() {
    return exists;
  }

  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getExpressionStr() {
    return exprStr;
  }
  @Override
  public ExpressionType getExpressionType() {
    return funcType;
  }
}
class MultiLongValueInLongValueOutRequireAllFunction extends MultiLongValueInLongValueOutFunction {

  public MultiLongValueInLongValueOutRequireAllFunction(String name, TwoLongInLongOutLambda lambda, LongValue[] params) {
    super(name, lambda, params);
  }

  @Override
  public long getLong() {
    long value = params[0].getLong();
    exists = params[0].exists();
    for (int i = 1; i < params.length && exists; ++i) {
      value = lambda.apply(value, params[i].getLong());
      exists = params[i].exists();
    }
    return value;
  }
}
class MultiLongValueInLongValueOutRequireOneFunction extends MultiLongValueInLongValueOutFunction {

  public MultiLongValueInLongValueOutRequireOneFunction(String name, TwoLongInLongOutLambda lambda, LongValue[] params) {
    super(name, lambda, params);
  }

  @Override
  public long getLong() {
    int i = -1;
    long value = 0;
    exists = false;
    while (++i < params.length) {
      value = params[i].getLong();
      exists = params[i].exists();
      if (exists) {
        break;
      }
    }
    while (++i < params.length) {
      temp = params[i].getLong();
      if (params[i].exists()) {
        value = lambda.apply(value, temp);
      }
    }
    return value;
  }
}
class FloatValueInFloatValueOutFunction extends AbstractFloatValue {
  private final FloatValue param;
  private final FloatInFloatOutLambda lambda;
  private final String name;
  private final String exprStr;
  private final ExpressionType funcType;

  public FloatValueInFloatValueOutFunction(String name, FloatInFloatOutLambda lambda, FloatValue param) {
    this.name = name;
    this.lambda = lambda;
    this.param = param;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,param);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,param);
  }

  private boolean exists = false;

  @Override
  public float getFloat() {
    float value = lambda.apply(param.getFloat());
    exists = param.exists();
    return value;
  }
  @Override
  public boolean exists() {
    return exists;
  }

  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getExpressionStr() {
    return exprStr;
  }
  @Override
  public ExpressionType getExpressionType() {
    return funcType;
  }
}
class FloatStreamInFloatStreamOutFunction extends AbstractFloatValueStream {
  private final FloatValueStream param;
  private final FloatInFloatOutLambda lambda;
  private final String name;
  private final String exprStr;
  private final ExpressionType funcType;

  public FloatStreamInFloatStreamOutFunction(String name, FloatInFloatOutLambda lambda, FloatValueStream param) {
    this.name = name;
    this.lambda = lambda;
    this.param = param;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,param);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,param);
  }

  @Override
  public void streamFloats(FloatConsumer cons) {
    param.streamFloats(value -> cons.accept(lambda.apply(value)));
  }

  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getExpressionStr() {
    return exprStr;
  }
  @Override
  public ExpressionType getExpressionType() {
    return funcType;
  }
}
class FloatStreamInFloatValueOutFunction extends AbstractFloatValue implements FloatConsumer {
  private final FloatValueStream param;
  private final TwoFloatInFloatOutLambda lambda;
  private final String name;
  private final String exprStr;
  private final ExpressionType funcType;

  public FloatStreamInFloatValueOutFunction(String name, TwoFloatInFloatOutLambda lambda, FloatValueStream param) {
    this.name = name;
    this.lambda = lambda;
    this.param = param;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,param);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,param);
  }

  private boolean exists = false;
  private float value;

  @Override
  public float getFloat() {
    exists = false;
    param.streamFloats(this);
    return value;
  }
  @Override
  public boolean exists() {
    return exists;
  }
  public void accept(float paramValue) {
    if (!exists) {
      exists = true;
      value = paramValue;
    } else {
      value = lambda.apply(value, paramValue);
    }
  }

  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getExpressionStr() {
    return exprStr;
  }
  @Override
  public ExpressionType getExpressionType() {
    return funcType;
  }
}
class TwoFloatValueInFloatValueOutFunction extends AbstractFloatValue {
  private final FloatValue param1;
  private final FloatValue param2;
  private final TwoFloatInFloatOutLambda lambda;
  private final String name;
  private final String exprStr;
  private final ExpressionType funcType;

  public TwoFloatValueInFloatValueOutFunction(String name, TwoFloatInFloatOutLambda lambda, FloatValue param1, FloatValue param2) {
    this.name = name;
    this.lambda = lambda;
    this.param1 = param1;
    this.param2 = param2;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,param1,param2);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,param1,param2);
  }

  private boolean exists = false;

  @Override
  public float getFloat() {
    float value = lambda.apply(param1.getFloat(), param2.getFloat());
    exists = param1.exists() && param2.exists();
    return value;
  }
  @Override
  public boolean exists() {
    return exists;
  }

  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getExpressionStr() {
    return exprStr;
  }
  @Override
  public ExpressionType getExpressionType() {
    return funcType;
  }
}
class FloatValueFloatStreamInFloatStreamOutFunction extends AbstractFloatValueStream {
  private final FloatValue param1;
  private final FloatValueStream param2;
  private final TwoFloatInFloatOutLambda lambda;
  private final String name;
  private final String exprStr;
  private final ExpressionType funcType;

  public FloatValueFloatStreamInFloatStreamOutFunction(String name, TwoFloatInFloatOutLambda lambda, FloatValue param1, FloatValueStream param2) {
    this.name = name;
    this.lambda = lambda;
    this.param1 = param1;
    this.param2 = param2;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,param1,param2);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,param1,param2);
  }

  @Override
  public void streamFloats(FloatConsumer cons) {
    float value1 = param1.getFloat();
    if (param1.exists()) {
      param2.streamFloats(value2 -> cons.accept(lambda.apply(value1,value2)));
    }
  }

  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getExpressionStr() {
    return exprStr;
  }
  @Override
  public ExpressionType getExpressionType() {
    return funcType;
  }
}
class FloatStreamFloatValueInFloatStreamOutFunction extends AbstractFloatValueStream {
  private final FloatValueStream param1;
  private final FloatValue param2;
  private final TwoFloatInFloatOutLambda lambda;
  private final String name;
  private final String exprStr;
  private final ExpressionType funcType;

  public FloatStreamFloatValueInFloatStreamOutFunction(String name, TwoFloatInFloatOutLambda lambda, FloatValueStream param1, FloatValue param2) {
    this.name = name;
    this.lambda = lambda;
    this.param1 = param1;
    this.param2 = param2;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,param1,param2);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,param1,param2);
  }

  @Override
  public void streamFloats(FloatConsumer cons) {
    float value2 = param2.getFloat();
    if (param2.exists()) {
      param1.streamFloats(value1 -> cons.accept(lambda.apply(value1,value2)));
    }
  }

  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getExpressionStr() {
    return exprStr;
  }
  @Override
  public ExpressionType getExpressionType() {
    return funcType;
  }
}
abstract class MultiFloatValueInFloatValueOutFunction extends AbstractFloatValue {
  protected final FloatValue[] params;
  protected final TwoFloatInFloatOutLambda lambda;
  private final String name;
  private final String exprStr;
  private final ExpressionType funcType;

  public MultiFloatValueInFloatValueOutFunction(String name, TwoFloatInFloatOutLambda lambda, FloatValue[] params) {
    this.name = name;
    this.lambda = lambda;
    this.params = params;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,params);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,params);
  }

  protected boolean exists = false;
  protected float temp;
  @Override
  public boolean exists() {
    return exists;
  }

  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getExpressionStr() {
    return exprStr;
  }
  @Override
  public ExpressionType getExpressionType() {
    return funcType;
  }
}
class MultiFloatValueInFloatValueOutRequireAllFunction extends MultiFloatValueInFloatValueOutFunction {

  public MultiFloatValueInFloatValueOutRequireAllFunction(String name, TwoFloatInFloatOutLambda lambda, FloatValue[] params) {
    super(name, lambda, params);
  }

  @Override
  public float getFloat() {
    float value = params[0].getFloat();
    exists = params[0].exists();
    for (int i = 1; i < params.length && exists; ++i) {
      value = lambda.apply(value, params[i].getFloat());
      exists = params[i].exists();
    }
    return value;
  }
}
class MultiFloatValueInFloatValueOutRequireOneFunction extends MultiFloatValueInFloatValueOutFunction {

  public MultiFloatValueInFloatValueOutRequireOneFunction(String name, TwoFloatInFloatOutLambda lambda, FloatValue[] params) {
    super(name, lambda, params);
  }

  @Override
  public float getFloat() {
    int i = -1;
    float value = 0;
    exists = false;
    while (++i < params.length) {
      value = params[i].getFloat();
      exists = params[i].exists();
      if (exists) {
        break;
      }
    }
    while (++i < params.length) {
      temp = params[i].getFloat();
      if (params[i].exists()) {
        value = lambda.apply(value, temp);
      }
    }
    return value;
  }
}
class DoubleValueInDoubleValueOutFunction extends AbstractDoubleValue {
  private final DoubleValue param;
  private final DoubleInDoubleOutLambda lambda;
  private final String name;
  private final String exprStr;
  private final ExpressionType funcType;

  public DoubleValueInDoubleValueOutFunction(String name, DoubleInDoubleOutLambda lambda, DoubleValue param) {
    this.name = name;
    this.lambda = lambda;
    this.param = param;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,param);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,param);
  }

  private boolean exists = false;

  @Override
  public double getDouble() {
    double value = lambda.apply(param.getDouble());
    exists = param.exists();
    return value;
  }
  @Override
  public boolean exists() {
    return exists;
  }

  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getExpressionStr() {
    return exprStr;
  }
  @Override
  public ExpressionType getExpressionType() {
    return funcType;
  }
}
class DoubleStreamInDoubleStreamOutFunction extends AbstractDoubleValueStream {
  private final DoubleValueStream param;
  private final DoubleInDoubleOutLambda lambda;
  private final String name;
  private final String exprStr;
  private final ExpressionType funcType;

  public DoubleStreamInDoubleStreamOutFunction(String name, DoubleInDoubleOutLambda lambda, DoubleValueStream param) {
    this.name = name;
    this.lambda = lambda;
    this.param = param;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,param);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,param);
  }

  @Override
  public void streamDoubles(DoubleConsumer cons) {
    param.streamDoubles(value -> cons.accept(lambda.apply(value)));
  }

  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getExpressionStr() {
    return exprStr;
  }
  @Override
  public ExpressionType getExpressionType() {
    return funcType;
  }
}
class DoubleStreamInDoubleValueOutFunction extends AbstractDoubleValue implements DoubleConsumer {
  private final DoubleValueStream param;
  private final TwoDoubleInDoubleOutLambda lambda;
  private final String name;
  private final String exprStr;
  private final ExpressionType funcType;

  public DoubleStreamInDoubleValueOutFunction(String name, TwoDoubleInDoubleOutLambda lambda, DoubleValueStream param) {
    this.name = name;
    this.lambda = lambda;
    this.param = param;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,param);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,param);
  }

  private boolean exists = false;
  private double value;

  @Override
  public double getDouble() {
    exists = false;
    param.streamDoubles(this);
    return value;
  }
  @Override
  public boolean exists() {
    return exists;
  }
  public void accept(double paramValue) {
    if (!exists) {
      exists = true;
      value = paramValue;
    } else {
      value = lambda.apply(value, paramValue);
    }
  }

  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getExpressionStr() {
    return exprStr;
  }
  @Override
  public ExpressionType getExpressionType() {
    return funcType;
  }
}
class TwoDoubleValueInDoubleValueOutFunction extends AbstractDoubleValue {
  private final DoubleValue param1;
  private final DoubleValue param2;
  private final TwoDoubleInDoubleOutLambda lambda;
  private final String name;
  private final String exprStr;
  private final ExpressionType funcType;

  public TwoDoubleValueInDoubleValueOutFunction(String name, TwoDoubleInDoubleOutLambda lambda, DoubleValue param1, DoubleValue param2) {
    this.name = name;
    this.lambda = lambda;
    this.param1 = param1;
    this.param2 = param2;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,param1,param2);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,param1,param2);
  }

  private boolean exists = false;

  @Override
  public double getDouble() {
    double value = lambda.apply(param1.getDouble(), param2.getDouble());
    exists = param1.exists() && param2.exists();
    return value;
  }
  @Override
  public boolean exists() {
    return exists;
  }

  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getExpressionStr() {
    return exprStr;
  }
  @Override
  public ExpressionType getExpressionType() {
    return funcType;
  }
}
class DoubleValueDoubleStreamInDoubleStreamOutFunction extends AbstractDoubleValueStream {
  private final DoubleValue param1;
  private final DoubleValueStream param2;
  private final TwoDoubleInDoubleOutLambda lambda;
  private final String name;
  private final String exprStr;
  private final ExpressionType funcType;

  public DoubleValueDoubleStreamInDoubleStreamOutFunction(String name, TwoDoubleInDoubleOutLambda lambda, DoubleValue param1, DoubleValueStream param2) {
    this.name = name;
    this.lambda = lambda;
    this.param1 = param1;
    this.param2 = param2;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,param1,param2);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,param1,param2);
  }

  @Override
  public void streamDoubles(DoubleConsumer cons) {
    double value1 = param1.getDouble();
    if (param1.exists()) {
      param2.streamDoubles(value2 -> cons.accept(lambda.apply(value1,value2)));
    }
  }

  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getExpressionStr() {
    return exprStr;
  }
  @Override
  public ExpressionType getExpressionType() {
    return funcType;
  }
}
class DoubleStreamDoubleValueInDoubleStreamOutFunction extends AbstractDoubleValueStream {
  private final DoubleValueStream param1;
  private final DoubleValue param2;
  private final TwoDoubleInDoubleOutLambda lambda;
  private final String name;
  private final String exprStr;
  private final ExpressionType funcType;

  public DoubleStreamDoubleValueInDoubleStreamOutFunction(String name, TwoDoubleInDoubleOutLambda lambda, DoubleValueStream param1, DoubleValue param2) {
    this.name = name;
    this.lambda = lambda;
    this.param1 = param1;
    this.param2 = param2;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,param1,param2);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,param1,param2);
  }

  @Override
  public void streamDoubles(DoubleConsumer cons) {
    double value2 = param2.getDouble();
    if (param2.exists()) {
      param1.streamDoubles(value1 -> cons.accept(lambda.apply(value1,value2)));
    }
  }

  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getExpressionStr() {
    return exprStr;
  }
  @Override
  public ExpressionType getExpressionType() {
    return funcType;
  }
}
abstract class MultiDoubleValueInDoubleValueOutFunction extends AbstractDoubleValue {
  protected final DoubleValue[] params;
  protected final TwoDoubleInDoubleOutLambda lambda;
  private final String name;
  private final String exprStr;
  private final ExpressionType funcType;

  public MultiDoubleValueInDoubleValueOutFunction(String name, TwoDoubleInDoubleOutLambda lambda, DoubleValue[] params) {
    this.name = name;
    this.lambda = lambda;
    this.params = params;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,params);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,params);
  }

  protected boolean exists = false;
  protected double temp;
  @Override
  public boolean exists() {
    return exists;
  }

  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getExpressionStr() {
    return exprStr;
  }
  @Override
  public ExpressionType getExpressionType() {
    return funcType;
  }
}
class MultiDoubleValueInDoubleValueOutRequireAllFunction extends MultiDoubleValueInDoubleValueOutFunction {

  public MultiDoubleValueInDoubleValueOutRequireAllFunction(String name, TwoDoubleInDoubleOutLambda lambda, DoubleValue[] params) {
    super(name, lambda, params);
  }

  @Override
  public double getDouble() {
    double value = params[0].getDouble();
    exists = params[0].exists();
    for (int i = 1; i < params.length && exists; ++i) {
      value = lambda.apply(value, params[i].getDouble());
      exists = params[i].exists();
    }
    return value;
  }
}
class MultiDoubleValueInDoubleValueOutRequireOneFunction extends MultiDoubleValueInDoubleValueOutFunction {

  public MultiDoubleValueInDoubleValueOutRequireOneFunction(String name, TwoDoubleInDoubleOutLambda lambda, DoubleValue[] params) {
    super(name, lambda, params);
  }

  @Override
  public double getDouble() {
    int i = -1;
    double value = 0;
    exists = false;
    while (++i < params.length) {
      value = params[i].getDouble();
      exists = params[i].exists();
      if (exists) {
        break;
      }
    }
    while (++i < params.length) {
      temp = params[i].getDouble();
      if (params[i].exists()) {
        value = lambda.apply(value, temp);
      }
    }
    return value;
  }
}
class DateValueInDateValueOutFunction extends AbstractDateValue {
  private final DateValue param;
  private final LongInLongOutLambda lambda;
  private final String name;
  private final String exprStr;
  private final ExpressionType funcType;

  public DateValueInDateValueOutFunction(String name, LongInLongOutLambda lambda, DateValue param) {
    this.name = name;
    this.lambda = lambda;
    this.param = param;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,param);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,param);
  }

  private boolean exists = false;

  @Override
  public long getLong() {
    long value = lambda.apply(param.getLong());
    exists = param.exists();
    return value;
  }
  @Override
  public boolean exists() {
    return exists;
  }

  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getExpressionStr() {
    return exprStr;
  }
  @Override
  public ExpressionType getExpressionType() {
    return funcType;
  }
}
class DateStreamInDateStreamOutFunction extends AbstractDateValueStream {
  private final DateValueStream param;
  private final LongInLongOutLambda lambda;
  private final String name;
  private final String exprStr;
  private final ExpressionType funcType;

  public DateStreamInDateStreamOutFunction(String name, LongInLongOutLambda lambda, DateValueStream param) {
    this.name = name;
    this.lambda = lambda;
    this.param = param;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,param);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,param);
  }

  @Override
  public void streamLongs(LongConsumer cons) {
    param.streamLongs(value -> cons.accept(lambda.apply(value)));
  }

  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getExpressionStr() {
    return exprStr;
  }
  @Override
  public ExpressionType getExpressionType() {
    return funcType;
  }
}
class DateStreamInDateValueOutFunction extends AbstractDateValue implements LongConsumer {
  private final DateValueStream param;
  private final TwoLongInLongOutLambda lambda;
  private final String name;
  private final String exprStr;
  private final ExpressionType funcType;

  public DateStreamInDateValueOutFunction(String name, TwoLongInLongOutLambda lambda, DateValueStream param) {
    this.name = name;
    this.lambda = lambda;
    this.param = param;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,param);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,param);
  }

  private boolean exists = false;
  private long value;

  @Override
  public long getLong() {
    exists = false;
    param.streamLongs(this);
    return value;
  }
  @Override
  public boolean exists() {
    return exists;
  }
  public void accept(long paramValue) {
    if (!exists) {
      exists = true;
      value = paramValue;
    } else {
      value = lambda.apply(value, paramValue);
    }
  }

  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getExpressionStr() {
    return exprStr;
  }
  @Override
  public ExpressionType getExpressionType() {
    return funcType;
  }
}
class TwoDateValueInDateValueOutFunction extends AbstractDateValue {
  private final DateValue param1;
  private final DateValue param2;
  private final TwoLongInLongOutLambda lambda;
  private final String name;
  private final String exprStr;
  private final ExpressionType funcType;

  public TwoDateValueInDateValueOutFunction(String name, TwoLongInLongOutLambda lambda, DateValue param1, DateValue param2) {
    this.name = name;
    this.lambda = lambda;
    this.param1 = param1;
    this.param2 = param2;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,param1,param2);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,param1,param2);
  }

  private boolean exists = false;

  @Override
  public long getLong() {
    long value = lambda.apply(param1.getLong(), param2.getLong());
    exists = param1.exists() && param2.exists();
    return value;
  }
  @Override
  public boolean exists() {
    return exists;
  }

  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getExpressionStr() {
    return exprStr;
  }
  @Override
  public ExpressionType getExpressionType() {
    return funcType;
  }
}
class DateValueDateStreamInDateStreamOutFunction extends AbstractDateValueStream {
  private final DateValue param1;
  private final DateValueStream param2;
  private final TwoLongInLongOutLambda lambda;
  private final String name;
  private final String exprStr;
  private final ExpressionType funcType;

  public DateValueDateStreamInDateStreamOutFunction(String name, TwoLongInLongOutLambda lambda, DateValue param1, DateValueStream param2) {
    this.name = name;
    this.lambda = lambda;
    this.param1 = param1;
    this.param2 = param2;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,param1,param2);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,param1,param2);
  }

  @Override
  public void streamLongs(LongConsumer cons) {
    long value1 = param1.getLong();
    if (param1.exists()) {
      param2.streamLongs(value2 -> cons.accept(lambda.apply(value1,value2)));
    }
  }

  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getExpressionStr() {
    return exprStr;
  }
  @Override
  public ExpressionType getExpressionType() {
    return funcType;
  }
}
class DateStreamDateValueInDateStreamOutFunction extends AbstractDateValueStream {
  private final DateValueStream param1;
  private final DateValue param2;
  private final TwoLongInLongOutLambda lambda;
  private final String name;
  private final String exprStr;
  private final ExpressionType funcType;

  public DateStreamDateValueInDateStreamOutFunction(String name, TwoLongInLongOutLambda lambda, DateValueStream param1, DateValue param2) {
    this.name = name;
    this.lambda = lambda;
    this.param1 = param1;
    this.param2 = param2;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,param1,param2);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,param1,param2);
  }

  @Override
  public void streamLongs(LongConsumer cons) {
    long value2 = param2.getLong();
    if (param2.exists()) {
      param1.streamLongs(value1 -> cons.accept(lambda.apply(value1,value2)));
    }
  }

  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getExpressionStr() {
    return exprStr;
  }
  @Override
  public ExpressionType getExpressionType() {
    return funcType;
  }
}
abstract class MultiDateValueInDateValueOutFunction extends AbstractDateValue {
  protected final DateValue[] params;
  protected final TwoLongInLongOutLambda lambda;
  private final String name;
  private final String exprStr;
  private final ExpressionType funcType;

  public MultiDateValueInDateValueOutFunction(String name, TwoLongInLongOutLambda lambda, DateValue[] params) {
    this.name = name;
    this.lambda = lambda;
    this.params = params;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,params);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,params);
  }

  protected boolean exists = false;
  protected long temp;
  @Override
  public boolean exists() {
    return exists;
  }

  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getExpressionStr() {
    return exprStr;
  }
  @Override
  public ExpressionType getExpressionType() {
    return funcType;
  }
}
class MultiDateValueInDateValueOutRequireAllFunction extends MultiDateValueInDateValueOutFunction {

  public MultiDateValueInDateValueOutRequireAllFunction(String name, TwoLongInLongOutLambda lambda, DateValue[] params) {
    super(name, lambda, params);
  }

  @Override
  public long getLong() {
    long value = params[0].getLong();
    exists = params[0].exists();
    for (int i = 1; i < params.length && exists; ++i) {
      value = lambda.apply(value, params[i].getLong());
      exists = params[i].exists();
    }
    return value;
  }
}
class MultiDateValueInDateValueOutRequireOneFunction extends MultiDateValueInDateValueOutFunction {

  public MultiDateValueInDateValueOutRequireOneFunction(String name, TwoLongInLongOutLambda lambda, DateValue[] params) {
    super(name, lambda, params);
  }

  @Override
  public long getLong() {
    int i = -1;
    long value = 0;
    exists = false;
    while (++i < params.length) {
      value = params[i].getLong();
      exists = params[i].exists();
      if (exists) {
        break;
      }
    }
    while (++i < params.length) {
      temp = params[i].getLong();
      if (params[i].exists()) {
        value = lambda.apply(value, temp);
      }
    }
    return value;
  }
}
class StringValueInStringValueOutFunction extends AbstractStringValue {
  private final StringValue param;
  private final StringInStringOutLambda lambda;
  private final String name;
  private final String exprStr;
  private final ExpressionType funcType;

  public StringValueInStringValueOutFunction(String name, StringInStringOutLambda lambda, StringValue param) {
    this.name = name;
    this.lambda = lambda;
    this.param = param;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,param);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,param);
  }

  private boolean exists = false;

  @Override
  public String getString() {
    String value = lambda.apply(param.getString());
    exists = param.exists();
    return value;
  }
  @Override
  public boolean exists() {
    return exists;
  }

  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getExpressionStr() {
    return exprStr;
  }
  @Override
  public ExpressionType getExpressionType() {
    return funcType;
  }
}
class StringStreamInStringStreamOutFunction extends AbstractStringValueStream {
  private final StringValueStream param;
  private final StringInStringOutLambda lambda;
  private final String name;
  private final String exprStr;
  private final ExpressionType funcType;

  public StringStreamInStringStreamOutFunction(String name, StringInStringOutLambda lambda, StringValueStream param) {
    this.name = name;
    this.lambda = lambda;
    this.param = param;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,param);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,param);
  }

  @Override
  public void streamStrings(Consumer<String> cons) {
    param.streamStrings(value -> cons.accept(lambda.apply(value)));
  }

  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getExpressionStr() {
    return exprStr;
  }
  @Override
  public ExpressionType getExpressionType() {
    return funcType;
  }
}
class StringStreamInStringValueOutFunction extends AbstractStringValue implements Consumer<String> {
  private final StringValueStream param;
  private final TwoStringInStringOutLambda lambda;
  private final String name;
  private final String exprStr;
  private final ExpressionType funcType;

  public StringStreamInStringValueOutFunction(String name, TwoStringInStringOutLambda lambda, StringValueStream param) {
    this.name = name;
    this.lambda = lambda;
    this.param = param;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,param);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,param);
  }

  private boolean exists = false;
  private String value;

  @Override
  public String getString() {
    exists = false;
    param.streamStrings(this);
    return value;
  }
  @Override
  public boolean exists() {
    return exists;
  }
  public void accept(String paramValue) {
    if (!exists) {
      exists = true;
      value = paramValue;
    } else {
      value = lambda.apply(value, paramValue);
    }
  }

  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getExpressionStr() {
    return exprStr;
  }
  @Override
  public ExpressionType getExpressionType() {
    return funcType;
  }
}
class TwoStringValueInStringValueOutFunction extends AbstractStringValue {
  private final StringValue param1;
  private final StringValue param2;
  private final TwoStringInStringOutLambda lambda;
  private final String name;
  private final String exprStr;
  private final ExpressionType funcType;

  public TwoStringValueInStringValueOutFunction(String name, TwoStringInStringOutLambda lambda, StringValue param1, StringValue param2) {
    this.name = name;
    this.lambda = lambda;
    this.param1 = param1;
    this.param2 = param2;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,param1,param2);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,param1,param2);
  }

  private boolean exists = false;

  @Override
  public String getString() {
    String value = lambda.apply(param1.getString(), param2.getString());
    exists = param1.exists() && param2.exists();
    return value;
  }
  @Override
  public boolean exists() {
    return exists;
  }

  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getExpressionStr() {
    return exprStr;
  }
  @Override
  public ExpressionType getExpressionType() {
    return funcType;
  }
}
class StringValueStringStreamInStringStreamOutFunction extends AbstractStringValueStream {
  private final StringValue param1;
  private final StringValueStream param2;
  private final TwoStringInStringOutLambda lambda;
  private final String name;
  private final String exprStr;
  private final ExpressionType funcType;

  public StringValueStringStreamInStringStreamOutFunction(String name, TwoStringInStringOutLambda lambda, StringValue param1, StringValueStream param2) {
    this.name = name;
    this.lambda = lambda;
    this.param1 = param1;
    this.param2 = param2;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,param1,param2);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,param1,param2);
  }

  @Override
  public void streamStrings(Consumer<String> cons) {
    String value1 = param1.getString();
    if (param1.exists()) {
      param2.streamStrings(value2 -> cons.accept(lambda.apply(value1,value2)));
    }
  }

  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getExpressionStr() {
    return exprStr;
  }
  @Override
  public ExpressionType getExpressionType() {
    return funcType;
  }
}
class StringStreamStringValueInStringStreamOutFunction extends AbstractStringValueStream {
  private final StringValueStream param1;
  private final StringValue param2;
  private final TwoStringInStringOutLambda lambda;
  private final String name;
  private final String exprStr;
  private final ExpressionType funcType;

  public StringStreamStringValueInStringStreamOutFunction(String name, TwoStringInStringOutLambda lambda, StringValueStream param1, StringValue param2) {
    this.name = name;
    this.lambda = lambda;
    this.param1 = param1;
    this.param2 = param2;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,param1,param2);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,param1,param2);
  }

  @Override
  public void streamStrings(Consumer<String> cons) {
    String value2 = param2.getString();
    if (param2.exists()) {
      param1.streamStrings(value1 -> cons.accept(lambda.apply(value1,value2)));
    }
  }

  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getExpressionStr() {
    return exprStr;
  }
  @Override
  public ExpressionType getExpressionType() {
    return funcType;
  }
}
abstract class MultiStringValueInStringValueOutFunction extends AbstractStringValue {
  protected final StringValue[] params;
  protected final TwoStringInStringOutLambda lambda;
  private final String name;
  private final String exprStr;
  private final ExpressionType funcType;

  public MultiStringValueInStringValueOutFunction(String name, TwoStringInStringOutLambda lambda, StringValue[] params) {
    this.name = name;
    this.lambda = lambda;
    this.params = params;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,params);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,params);
  }

  protected boolean exists = false;
  protected String temp = null;
  @Override
  public boolean exists() {
    return exists;
  }

  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getExpressionStr() {
    return exprStr;
  }
  @Override
  public ExpressionType getExpressionType() {
    return funcType;
  }
}
class MultiStringValueInStringValueOutRequireAllFunction extends MultiStringValueInStringValueOutFunction {

  public MultiStringValueInStringValueOutRequireAllFunction(String name, TwoStringInStringOutLambda lambda, StringValue[] params) {
    super(name, lambda, params);
  }

  @Override
  public String getString() {
    String value = params[0].getString();
    exists = params[0].exists();
    for (int i = 1; i < params.length && exists; ++i) {
      temp = params[i].getString();
      if (params[i].exists()) {
        value = lambda.apply(value, temp);
      } else {
        exists = false;
        value = null;
      }
    }
    return value;
  }
}
class MultiStringValueInStringValueOutRequireOneFunction extends MultiStringValueInStringValueOutFunction {

  public MultiStringValueInStringValueOutRequireOneFunction(String name, TwoStringInStringOutLambda lambda, StringValue[] params) {
    super(name, lambda, params);
  }

  @Override
  public String getString() {
    int i = -1;
    String value = null;
    exists = false;
    while (++i < params.length) {
      value = params[i].getString();
      exists = params[i].exists();
      if (exists) {
        break;
      }
    }
    while (++i < params.length) {
      temp = params[i].getString();
      if (params[i].exists()) {
        value = lambda.apply(value, temp);
      }
    }
    return value;
  }
}