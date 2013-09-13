package org.apache.lucene.expressions.js;
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

import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.util.MathUtil;
import org.objectweb.asm.Type;

/**
 * A wrapper to delegate function calls from a javascript expression.
 */
class JavascriptFunction {
  private static Map<String, JavascriptFunction> methods = null;
  
  static {
    String mathClass  = Type.getInternalName(Math.class);
    String mathUtilClass = Type.getInternalName(MathUtil.class);
    
    JavascriptFunction abs    = new JavascriptFunction("abs",    1, mathClass,  "abs",      "(D)D" );
    JavascriptFunction acos   = new JavascriptFunction("acos",   1, mathClass,  "acos",     "(D)D" );
    JavascriptFunction acosh  = new JavascriptFunction("acosh",  1, mathUtilClass, "acosh",    "(D)D" );
    JavascriptFunction asin   = new JavascriptFunction("asin",   1, mathClass,  "asin",     "(D)D" );
    JavascriptFunction asinh  = new JavascriptFunction("asinh",  1, mathUtilClass, "asinh",    "(D)D" );
    JavascriptFunction atan   = new JavascriptFunction("atan",   1, mathClass,  "atan",     "(D)D" );
    JavascriptFunction atan2  = new JavascriptFunction("atan2",  1, mathClass,  "atan2",    "(DD)D");
    JavascriptFunction atanh  = new JavascriptFunction("atanh",  1, mathUtilClass, "atanh",    "(D)D" );
    JavascriptFunction ceil   = new JavascriptFunction("ceil",   1, mathClass,  "ceil",     "(D)D" );
    JavascriptFunction cos    = new JavascriptFunction("cos",    1, mathClass,  "cos",      "(D)D" );
    JavascriptFunction cosh   = new JavascriptFunction("cosh",   1, mathClass,  "cosh",     "(D)D" );
    JavascriptFunction exp    = new JavascriptFunction("exp",    1, mathClass,  "exp",      "(D)D" );
    JavascriptFunction floor  = new JavascriptFunction("floor",  1, mathClass,  "floor",    "(D)D" );
    JavascriptFunction ln     = new JavascriptFunction("ln",     1, mathClass,  "log",      "(D)D" );
    JavascriptFunction log10  = new JavascriptFunction("log10",  1, mathClass,  "log10",    "(D)D" );
    JavascriptFunction logn   = new JavascriptFunction("logn",   2, mathUtilClass, "log",      "(DD)D");
    JavascriptFunction pow    = new JavascriptFunction("pow",    2, mathClass,  "pow",      "(DD)D");
    JavascriptFunction sin    = new JavascriptFunction("sin",    1, mathClass,  "sin",      "(D)D" );
    JavascriptFunction sinh   = new JavascriptFunction("sinh",   1, mathClass,  "sinh",     "(D)D" );
    JavascriptFunction sqrt   = new JavascriptFunction("sqrt",   1, mathClass,  "sqrt",     "(D)D" );
    JavascriptFunction tan    = new JavascriptFunction("tan",    1, mathClass,  "tan",      "(D)D" );
    JavascriptFunction tanh   = new JavascriptFunction("tanh",   1, mathClass,  "tanh",     "(D)D" );
    
    JavascriptFunction min    = new JavascriptFunction("min",    2, mathClass,  "min",      "(DD)D");
    JavascriptFunction max    = new JavascriptFunction("max",    2, mathClass,  "max",      "(DD)D");
    
    methods  = new HashMap<String, JavascriptFunction>();
    methods.put( "abs",    abs   );
    methods.put( "acos",   acos  );
    methods.put( "acosh",  acosh );
    methods.put( "asin",   asin  );
    methods.put( "asinh",  asinh );
    methods.put( "atan",   atan  );
    methods.put( "atan2",  atan2 );
    methods.put( "atanh",  atanh );
    methods.put( "ceil",   ceil  );
    methods.put( "cos",    cos   );
    methods.put( "cosh",   cosh  );
    methods.put( "exp",    exp   );
    methods.put( "floor",  floor );
    methods.put( "ln",     ln    );
    methods.put( "log10",  log10 );
    methods.put( "logn",   logn  );
    methods.put( "max",    max   );
    methods.put( "min",    min   );
    methods.put( "pow",    pow   );
    methods.put( "sin",    sin   );
    methods.put( "sinh",   sinh  );
    methods.put( "sqrt",   sqrt  );
    methods.put( "tan",    tan   );
    methods.put( "tanh",   tanh  );
  }

  public static JavascriptFunction getMethod(String call, int arguments) {
    JavascriptFunction method = methods.get(call);

    if (method == null) {
      throw new IllegalArgumentException("Unrecognized method call (" + call + ").");
    }

    if (arguments != method.arguments && method.arguments != -1) {
      throw new IllegalArgumentException("Expected (" + method.arguments + ") arguments for method call (" +
          call + "), but found (" + arguments + ").");
    }

    return method;
  }
  
  public final String call;
  public final int arguments;
  public final String klass;
  public final String method;
  public final String signature;
  
  private JavascriptFunction(String call, int arguments, String klass, String method, String signature) {
    this.call = call;
    this.arguments = arguments;
    this.klass = klass;
    this.method = method;
    this.signature = signature;
  }
}
