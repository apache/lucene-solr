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
import java.lang.reflect.Method;

import org.apache.lucene.util.MathUtil;
import org.objectweb.asm.Type;

/**
 * A wrapper to delegate function calls from a javascript expression.
 */
class JavascriptFunction {

  private static final Map<String, JavascriptFunction> methods = new HashMap<String, JavascriptFunction>();
  static {
    try {
      addFunction("abs",    Math.class.getMethod("abs", double.class));
      addFunction("acos",   Math.class.getMethod("acos", double.class));
      addFunction("acosh",  MathUtil.class.getMethod("acosh", double.class));
      addFunction("asin",   Math.class.getMethod("asin", double.class));
      addFunction("asinh",  MathUtil.class.getMethod("asinh", double.class));
      addFunction("atan",   Math.class.getMethod("atan", double.class));
      addFunction("atan2",  Math.class.getMethod("atan2", double.class, double.class));
      addFunction("atanh",  MathUtil.class.getMethod("atanh", double.class));
      addFunction("ceil",   Math.class.getMethod("ceil", double.class));
      addFunction("cos",    Math.class.getMethod("cos", double.class));
      addFunction("cosh",   Math.class.getMethod("cosh", double.class));
      addFunction("exp",    Math.class.getMethod("exp", double.class));
      addFunction("floor",  Math.class.getMethod("floor", double.class));
      addFunction("ln",     Math.class.getMethod("log", double.class));
      addFunction("log10",  Math.class.getMethod("log10", double.class));
      addFunction("logn",   MathUtil.class.getMethod("log", double.class, double.class));
      addFunction("max",    Math.class.getMethod("max", double.class, double.class));
      addFunction("min",    Math.class.getMethod("min", double.class, double.class));
      addFunction("pow",    Math.class.getMethod("pow", double.class, double.class));
      addFunction("sin",    Math.class.getMethod("sin", double.class));
      addFunction("sinh",   Math.class.getMethod("sinh", double.class));
      addFunction("sqrt",   Math.class.getMethod("sqrt", double.class));
      addFunction("tan",    Math.class.getMethod("tan", double.class));
      addFunction("tanh",   Math.class.getMethod("tanh", double.class));
    } catch (NoSuchMethodException e) {
      throw new Error("Cannot resolve function", e);
    }
  }
  
  private static void addFunction(String call, Method method) {
    methods.put(call, new JavascriptFunction(call, method));
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
  
  private JavascriptFunction(String call, Method method) {
    this.call = call;
    this.arguments = method.getParameterTypes().length;
    this.klass = Type.getInternalName(method.getDeclaringClass());
    this.method = method.getName();
    this.signature = Type.getMethodDescriptor(method);
  }
}
