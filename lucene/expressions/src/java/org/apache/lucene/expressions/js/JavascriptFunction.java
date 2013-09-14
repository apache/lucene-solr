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

import java.io.IOException;
import java.io.Reader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.MathUtil;
import org.objectweb.asm.Type;

/**
 * A wrapper to delegate function calls from a javascript expression.
 */
class JavascriptFunction {

  private static final Map<String, JavascriptFunction> methods = new HashMap<String, JavascriptFunction>();
  static {
    try {
      final Properties props = new Properties();
      try (Reader in = IOUtils.getDecodingReader(JavascriptFunction.class,
        JavascriptFunction.class.getSimpleName() + ".properties", IOUtils.CHARSET_UTF_8)) {
        props.load(in);
      }
      for (final String call : props.stringPropertyNames()) {
        final String[] vals = props.getProperty(call).split(",");
        if (vals.length != 3) {
          throw new Error("Syntax error while reading Javascript functions from resource");
        }
        final Class<?> clazz = Class.forName(vals[0].trim());
        final String methodName = vals[1].trim();
        final int arity = Integer.parseInt(vals[2].trim());
        @SuppressWarnings({"rawtypes", "unchecked"}) Class[] args = new Class[arity];
        Arrays.fill(args, double.class);
        methods.put(call, new JavascriptFunction(call, clazz.getMethod(methodName, args)));
      }
    } catch (NoSuchMethodException | ClassNotFoundException | IOException e) {
      throw new Error("Cannot resolve function", e);
    }
  }
  
  public static JavascriptFunction getMethod(String call, int arity) {
    JavascriptFunction method = methods.get(call);

    if (method == null) {
      throw new IllegalArgumentException("Unrecognized method call (" + call + ").");
    }

    if (arity != method.arity && method.arity != -1) {
      throw new IllegalArgumentException("Expected (" + method.arity + ") arguments for method call (" +
          call + "), but found (" + arity + ").");
    }

    return method;
  }
  
  public final String call;
  public final int arity;
  public final String klass;
  public final String method;
  public final String descriptor;
  
  private JavascriptFunction(String call, Method method) {
    // do some checks if the signature is "compatible":
    if (!Modifier.isStatic(method.getModifiers())) {
      throw new IllegalArgumentException(method + " is not static.");
    }
    if (method.getReturnType() != double.class) {
      throw new IllegalArgumentException(method + " does not return a double.");
    }
    
    this.call = call;
    this.arity = method.getParameterTypes().length;
    this.klass = Type.getInternalName(method.getDeclaringClass());
    this.method = method.getName();
    this.descriptor = Type.getMethodDescriptor(method);
  }
}
