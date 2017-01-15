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
package org.apache.lucene.expressions.js;

import java.io.IOException;
import java.io.Reader;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.lucene.expressions.Expression;
import org.apache.lucene.expressions.js.JavascriptParser.ExpressionContext;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.util.IOUtils;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Label;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;

/**
 * An expression compiler for javascript expressions.
 * <p>
 * Example:
 * <pre class="prettyprint">
 *   Expression foo = JavascriptCompiler.compile("((0.3*popularity)/10.0)+(0.7*score)");
 * </pre>
 * <p>
 * See the {@link org.apache.lucene.expressions.js package documentation} for 
 * the supported syntax and default functions.
 * <p>
 * You can compile with an alternate set of functions via {@link #compile(String, Map, ClassLoader)}.
 * For example:
 * <pre class="prettyprint">
 *   Map&lt;String,Method&gt; functions = new HashMap&lt;&gt;();
 *   // add all the default functions
 *   functions.putAll(JavascriptCompiler.DEFAULT_FUNCTIONS);
 *   // add cbrt()
 *   functions.put("cbrt", Math.class.getMethod("cbrt", double.class));
 *   // call compile with customized function map
 *   Expression foo = JavascriptCompiler.compile("cbrt(score)+ln(popularity)", 
 *                                               functions, 
 *                                               getClass().getClassLoader());
 * </pre>
 * 
 * @lucene.experimental
 */
public final class JavascriptCompiler {
  static final class Loader extends ClassLoader {
    Loader(ClassLoader parent) {
      super(parent);
    }

    public Class<? extends Expression> define(String className, byte[] bytecode) {
      return defineClass(className, bytecode, 0, bytecode.length).asSubclass(Expression.class);
    }
  }
  
  private static final int CLASSFILE_VERSION = Opcodes.V1_8;
  
  // We use the same class name for all generated classes as they all have their own class loader.
  // The source code is displayed as "source file name" in stack trace.
  private static final String COMPILED_EXPRESSION_CLASS = JavascriptCompiler.class.getName() + "$CompiledExpression";
  private static final String COMPILED_EXPRESSION_INTERNAL = COMPILED_EXPRESSION_CLASS.replace('.', '/');
  
  static final Type EXPRESSION_TYPE = Type.getType(Expression.class);
  static final Type FUNCTION_VALUES_TYPE = Type.getType(DoubleValues.class);

  private static final org.objectweb.asm.commons.Method
    EXPRESSION_CTOR = getAsmMethod(void.class, "<init>", String.class, String[].class),
    EVALUATE_METHOD = getAsmMethod(double.class, "evaluate", DoubleValues[].class);

  static final org.objectweb.asm.commons.Method DOUBLE_VAL_METHOD = getAsmMethod(double.class, "doubleValue");
  
  /** create an ASM Method object from return type, method name, and parameters. */
  private static org.objectweb.asm.commons.Method getAsmMethod(Class<?> rtype, String name, Class<?>... ptypes) {
    return new org.objectweb.asm.commons.Method(name, MethodType.methodType(rtype, ptypes).toMethodDescriptorString());
  }
  
  // This maximum length is theoretically 65535 bytes, but as it's CESU-8 encoded we dont know how large it is in bytes, so be safe
  // rcmuir: "If your ranking function is that large you need to check yourself into a mental institution!"
  private static final int MAX_SOURCE_LENGTH = 16384;
  
  final String sourceText;
  final Map<String,Method> functions;
  
  /**
   * Compiles the given expression.
   *
   * @param sourceText The expression to compile
   * @return A new compiled expression
   * @throws ParseException on failure to compile
   */
  public static Expression compile(String sourceText) throws ParseException {
    return new JavascriptCompiler(sourceText).compileExpression(JavascriptCompiler.class.getClassLoader());
  }
  
  /**
   * Compiles the given expression with the supplied custom functions.
   * <p>
   * Functions must be {@code public static}, return {@code double} and 
   * can take from zero to 256 {@code double} parameters.
   *
   * @param sourceText The expression to compile
   * @param functions map of String names to functions
   * @param parent a {@code ClassLoader} that should be used as the parent of the loaded class.
   *   It must contain all classes referred to by the given {@code functions}.
   * @return A new compiled expression
   * @throws ParseException on failure to compile
   */
  public static Expression compile(String sourceText, Map<String,Method> functions, ClassLoader parent) throws ParseException {
    if (parent == null) {
      throw new NullPointerException("A parent ClassLoader must be given.");
    }
    for (Method m : functions.values()) {
      checkFunctionClassLoader(m, parent);
      checkFunction(m);
    }
    return new JavascriptCompiler(sourceText, functions).compileExpression(parent);
  }
  
  /**
   * This method is unused, it is just here to make sure that the function signatures don't change.
   * If this method fails to compile, you also have to change the byte code generator to correctly
   * use the FunctionValues class.
   */
  @SuppressWarnings({"unused", "null"})
  private static void unusedTestCompile() throws IOException {
    DoubleValues f = null;
    double ret = f.doubleValue();
  }
  
  /**
   * Constructs a compiler for expressions.
   * @param sourceText The expression to compile
   */
  private JavascriptCompiler(String sourceText) {
    this(sourceText, DEFAULT_FUNCTIONS);
  }
  
  /**
   * Constructs a compiler for expressions with specific set of functions
   * @param sourceText The expression to compile
   */
  private JavascriptCompiler(String sourceText, Map<String,Method> functions) {
    if (sourceText == null) {
      throw new NullPointerException();
    }
    this.sourceText = sourceText;
    this.functions = functions;
  }
  
  /**
   * Compiles the given expression with the specified parent classloader
   *
   * @return A new compiled expression
   * @throws ParseException on failure to compile
   */
  private Expression compileExpression(ClassLoader parent) throws ParseException {
    final Map<String, Integer> externalsMap = new LinkedHashMap<>();
    final ClassWriter classWriter = new ClassWriter(ClassWriter.COMPUTE_FRAMES | ClassWriter.COMPUTE_MAXS);
    
    generateClass(getAntlrParseTree(), classWriter, externalsMap);
    
    try {
      final Class<? extends Expression> evaluatorClass = new Loader(parent)
        .define(COMPILED_EXPRESSION_CLASS, classWriter.toByteArray());
      final Constructor<? extends Expression> constructor = evaluatorClass.getConstructor(String.class, String[].class);

      return constructor.newInstance(sourceText, externalsMap.keySet().toArray(new String[externalsMap.size()]));
    } catch (ReflectiveOperationException exception) {
      throw new IllegalStateException("An internal error occurred attempting to compile the expression (" + sourceText + ").", exception);
    }
  }

  /**
   * Parses the sourceText into an ANTLR 4 parse tree
   *
   * @return The ANTLR parse tree
   * @throws ParseException on failure to parse
   */
  private ParseTree getAntlrParseTree() throws ParseException {
    try {
      final ANTLRInputStream antlrInputStream = new ANTLRInputStream(sourceText);
      final JavascriptErrorHandlingLexer javascriptLexer = new JavascriptErrorHandlingLexer(antlrInputStream);
      javascriptLexer.removeErrorListeners();
      final JavascriptParser javascriptParser = new JavascriptParser(new CommonTokenStream(javascriptLexer));
      javascriptParser.removeErrorListeners();
      javascriptParser.setErrorHandler(new JavascriptParserErrorStrategy());
      return javascriptParser.compile();
    } catch (RuntimeException re) {
      if (re.getCause() instanceof ParseException) {
        throw (ParseException)re.getCause();
      }
      throw re;
    }
  }

  /**
   * Sends the bytecode of class file to {@link ClassWriter}.
   */
  private void generateClass(final ParseTree parseTree, final ClassWriter classWriter, final Map<String, Integer> externalsMap) throws ParseException {
    classWriter.visit(CLASSFILE_VERSION,
        Opcodes.ACC_PUBLIC | Opcodes.ACC_SUPER | Opcodes.ACC_FINAL,
        COMPILED_EXPRESSION_INTERNAL,
        null, EXPRESSION_TYPE.getInternalName(), null);
    final String clippedSourceText = (sourceText.length() <= MAX_SOURCE_LENGTH) ?
        sourceText : (sourceText.substring(0, MAX_SOURCE_LENGTH - 3) + "...");
    classWriter.visitSource(clippedSourceText, null);
    
    final GeneratorAdapter constructor = new GeneratorAdapter(Opcodes.ACC_PUBLIC,
        EXPRESSION_CTOR, null, null, classWriter);
    constructor.loadThis();
    constructor.loadArgs();
    constructor.invokeConstructor(EXPRESSION_TYPE, EXPRESSION_CTOR);
    constructor.returnValue();
    constructor.endMethod();
    
    final GeneratorAdapter gen = new GeneratorAdapter(Opcodes.ACC_PUBLIC,
        EVALUATE_METHOD, null, null, classWriter);
    
    // to completely hide the ANTLR visitor we use an anonymous impl:
    new JavascriptBaseVisitor<Void>() {
      private final Deque<Type> typeStack = new ArrayDeque<>();

      @Override
      public Void visitCompile(JavascriptParser.CompileContext ctx) {
        typeStack.push(Type.DOUBLE_TYPE);
        visit(ctx.expression());
        typeStack.pop();

        return null;
      }

      @Override
      public Void visitPrecedence(JavascriptParser.PrecedenceContext ctx) {
        visit(ctx.expression());

        return null;
      }

      @Override
      public Void visitNumeric(JavascriptParser.NumericContext ctx) {
        if (ctx.HEX() != null) {
          pushLong(Long.parseLong(ctx.HEX().getText().substring(2), 16));
        } else if (ctx.OCTAL() != null) {
          pushLong(Long.parseLong(ctx.OCTAL().getText().substring(1), 8));
        } else if (ctx.DECIMAL() != null) {
          gen.push(Double.parseDouble(ctx.DECIMAL().getText()));
          gen.cast(Type.DOUBLE_TYPE, typeStack.peek());
        } else {
          throw new IllegalStateException("Unknown operation specified: " + ctx.getText());
        }

        return null;
      }

      @Override
      public Void visitExternal(JavascriptParser.ExternalContext ctx) {
        String text = ctx.VARIABLE().getText();
        int arguments = ctx.expression().size();
        boolean parens = ctx.LP() != null && ctx.RP() != null;
        Method method = parens ? functions.get(text) : null;

        if (method != null) {
          int arity = method.getParameterTypes().length;

          if (arguments != arity) {
            throwChecked(new ParseException(
                "Invalid expression '" + sourceText + "': Expected (" + 
                arity + ") arguments for function call (" + text + "), but found (" + arguments + ").", 
                ctx.start.getStartIndex()));
          }

          typeStack.push(Type.DOUBLE_TYPE);

          for (int argument = 0; argument < arguments; ++argument) {
            visit(ctx.expression(argument));
          }

          typeStack.pop();

          gen.invokeStatic(Type.getType(method.getDeclaringClass()),
              org.objectweb.asm.commons.Method.getMethod(method));

          gen.cast(Type.DOUBLE_TYPE, typeStack.peek());
        } else if (!parens || arguments == 0 && text.contains(".")) {
          int index;

          text = normalizeQuotes(ctx.getText());

          if (externalsMap.containsKey(text)) {
            index = externalsMap.get(text);
          } else {
            index = externalsMap.size();
            externalsMap.put(text, index);
          }

          gen.loadArg(0);
          gen.push(index);
          gen.arrayLoad(FUNCTION_VALUES_TYPE);
          gen.invokeVirtual(FUNCTION_VALUES_TYPE, DOUBLE_VAL_METHOD);
          gen.cast(Type.DOUBLE_TYPE, typeStack.peek());
        } else {
          throwChecked(new ParseException("Invalid expression '" + sourceText + "': Unrecognized function call (" +
              text + ").", ctx.start.getStartIndex()));
        }

        return null;
      }

      @Override
      public Void visitUnary(JavascriptParser.UnaryContext ctx) {
        if (ctx.BOOLNOT() != null) {
          Label labelNotTrue = new Label();
          Label labelNotReturn = new Label();

          typeStack.push(Type.INT_TYPE);
          visit(ctx.expression());
          typeStack.pop();
          gen.visitJumpInsn(Opcodes.IFEQ, labelNotTrue);
          pushBoolean(false);
          gen.goTo(labelNotReturn);
          gen.visitLabel(labelNotTrue);
          pushBoolean(true);
          gen.visitLabel(labelNotReturn);

        } else if (ctx.BWNOT() != null) {
          typeStack.push(Type.LONG_TYPE);
          visit(ctx.expression());
          typeStack.pop();
          gen.push(-1L);
          gen.visitInsn(Opcodes.LXOR);
          gen.cast(Type.LONG_TYPE, typeStack.peek());

        } else if (ctx.ADD() != null) {
          visit(ctx.expression());

        } else if (ctx.SUB() != null) {
          typeStack.push(Type.DOUBLE_TYPE);
          visit(ctx.expression());
          typeStack.pop();
          gen.visitInsn(Opcodes.DNEG);
          gen.cast(Type.DOUBLE_TYPE, typeStack.peek());

        } else {
          throw new IllegalStateException("Unknown operation specified: " + ctx.getText());
        }

        return null;
      }

      @Override
      public Void visitMuldiv(JavascriptParser.MuldivContext ctx) {
        int opcode;

        if (ctx.MUL() != null) {
          opcode = Opcodes.DMUL;
        } else if (ctx.DIV() != null) {
          opcode = Opcodes.DDIV;
        } else if (ctx.REM() != null) {
          opcode = Opcodes.DREM;
        } else {
          throw new IllegalStateException("Unknown operation specified: " + ctx.getText());
        }

        pushArith(opcode, ctx.expression(0), ctx.expression(1));

        return null;
      }

      @Override
      public Void visitAddsub(JavascriptParser.AddsubContext ctx) {
        int opcode;

        if (ctx.ADD() != null) {
          opcode = Opcodes.DADD;
        } else if (ctx.SUB() != null) {
          opcode = Opcodes.DSUB;
        } else {
          throw new IllegalStateException("Unknown operation specified: " + ctx.getText());
        }

        pushArith(opcode, ctx.expression(0), ctx.expression(1));

        return null;
      }

      @Override
      public Void visitBwshift(JavascriptParser.BwshiftContext ctx) {
        int opcode;

        if (ctx.LSH() != null) {
          opcode = Opcodes.LSHL;
        } else if (ctx.RSH() != null) {
          opcode = Opcodes.LSHR;
        } else if (ctx.USH() != null) {
          opcode = Opcodes.LUSHR;
        } else {
          throw new IllegalStateException("Unknown operation specified: " + ctx.getText());
        }

        pushShift(opcode, ctx.expression(0), ctx.expression(1));

        return null;
      }

      @Override
      public Void visitBoolcomp(JavascriptParser.BoolcompContext ctx) {
        int opcode;

        if (ctx.LT() != null) {
          opcode = GeneratorAdapter.LT;
        } else if (ctx.LTE() != null) {
          opcode = GeneratorAdapter.LE;
        } else if (ctx.GT() != null) {
          opcode = GeneratorAdapter.GT;
        } else if (ctx.GTE() != null) {
          opcode = GeneratorAdapter.GE;
        } else {
          throw new IllegalStateException("Unknown operation specified: " + ctx.getText());
        }

        pushCond(opcode, ctx.expression(0), ctx.expression(1));

        return null;
      }

      @Override
      public Void visitBooleqne(JavascriptParser.BooleqneContext ctx) {
        int opcode;

        if (ctx.EQ() != null) {
          opcode = GeneratorAdapter.EQ;
        } else if (ctx.NE() != null) {
          opcode = GeneratorAdapter.NE;
        } else {
          throw new IllegalStateException("Unknown operation specified: " + ctx.getText());
        }

        pushCond(opcode, ctx.expression(0), ctx.expression(1));

        return null;
      }

      @Override
      public Void visitBwand(JavascriptParser.BwandContext ctx) {
        pushBitwise(Opcodes.LAND, ctx.expression(0), ctx.expression(1));

        return null;
      }

      @Override
      public Void visitBwxor(JavascriptParser.BwxorContext ctx) {
        pushBitwise(Opcodes.LXOR, ctx.expression(0), ctx.expression(1));

        return null;
      }

      @Override
      public Void visitBwor(JavascriptParser.BworContext ctx) {
        pushBitwise(Opcodes.LOR, ctx.expression(0), ctx.expression(1));

        return null;
      }

      @Override
      public Void visitBooland(JavascriptParser.BoolandContext ctx) {
        Label andFalse = new Label();
        Label andEnd = new Label();

        typeStack.push(Type.INT_TYPE);
        visit(ctx.expression(0));
        gen.visitJumpInsn(Opcodes.IFEQ, andFalse);
        visit(ctx.expression(1));
        gen.visitJumpInsn(Opcodes.IFEQ, andFalse);
        typeStack.pop();
        pushBoolean(true);
        gen.goTo(andEnd);
        gen.visitLabel(andFalse);
        pushBoolean(false);
        gen.visitLabel(andEnd);

        return null;
      }

      @Override
      public Void visitBoolor(JavascriptParser.BoolorContext ctx) {
        Label orTrue = new Label();
        Label orEnd = new Label();

        typeStack.push(Type.INT_TYPE);
        visit(ctx.expression(0));
        gen.visitJumpInsn(Opcodes.IFNE, orTrue);
        visit(ctx.expression(1));
        gen.visitJumpInsn(Opcodes.IFNE, orTrue);
        typeStack.pop();
        pushBoolean(false);
        gen.goTo(orEnd);
        gen.visitLabel(orTrue);
        pushBoolean(true);
        gen.visitLabel(orEnd);

        return null;
      }

      @Override
      public Void visitConditional(JavascriptParser.ConditionalContext ctx) {
        Label condFalse = new Label();
        Label condEnd = new Label();

        typeStack.push(Type.INT_TYPE);
        visit(ctx.expression(0));
        typeStack.pop();
        gen.visitJumpInsn(Opcodes.IFEQ, condFalse);
        visit(ctx.expression(1));
        gen.goTo(condEnd);
        gen.visitLabel(condFalse);
        visit(ctx.expression(2));
        gen.visitLabel(condEnd);

        return null;
      }

      private void pushArith(int operator, ExpressionContext left, ExpressionContext right) {
        pushBinaryOp(operator, left, right, Type.DOUBLE_TYPE, Type.DOUBLE_TYPE, Type.DOUBLE_TYPE);
      }

      private void pushShift(int operator, ExpressionContext left, ExpressionContext right) {
        pushBinaryOp(operator, left, right, Type.LONG_TYPE, Type.INT_TYPE, Type.LONG_TYPE);
      }

      private void pushBitwise(int operator, ExpressionContext left, ExpressionContext right) {
        pushBinaryOp(operator, left, right, Type.LONG_TYPE, Type.LONG_TYPE, Type.LONG_TYPE);
      }

      private void pushBinaryOp(int operator, ExpressionContext left, ExpressionContext right,
                                Type leftType, Type rightType, Type returnType) {
        typeStack.push(leftType);
        visit(left);
        typeStack.pop();
        typeStack.push(rightType);
        visit(right);
        typeStack.pop();
        gen.visitInsn(operator);
        gen.cast(returnType, typeStack.peek());
      }

      private void pushCond(int operator, ExpressionContext left, ExpressionContext right) {
        Label labelTrue = new Label();
        Label labelReturn = new Label();

        typeStack.push(Type.DOUBLE_TYPE);
        visit(left);
        visit(right);
        typeStack.pop();

        gen.ifCmp(Type.DOUBLE_TYPE, operator, labelTrue);
        pushBoolean(false);
        gen.goTo(labelReturn);
        gen.visitLabel(labelTrue);
        pushBoolean(true);
        gen.visitLabel(labelReturn);
      }

      private void pushBoolean(boolean truth) {
        switch (typeStack.peek().getSort()) {
          case Type.INT:
            gen.push(truth);
            break;
          case Type.LONG:
            gen.push(truth ? 1L : 0L);
            break;
          case Type.DOUBLE:
            gen.push(truth ? 1. : 0.);
            break;
          default:
            throw new IllegalStateException("Invalid expected type: " + typeStack.peek());
        }
      }

      private void pushLong(long i) {
        switch (typeStack.peek().getSort()) {
          case Type.INT:
            gen.push((int) i);
            break;
          case Type.LONG:
            gen.push(i);
            break;
          case Type.DOUBLE:
            gen.push((double) i);
            break;
          default:
            throw new IllegalStateException("Invalid expected type: " + typeStack.peek());
        }
      }
      
      /** Needed to throw checked ParseException in this visitor (that does not allow it). */
      private void throwChecked(Throwable t) {
        this.<Error>throwChecked0(t);
      }
      
      @SuppressWarnings("unchecked")
      private <T extends Throwable> void throwChecked0(Throwable t) throws T {
        throw (T) t;
      }
    }.visit(parseTree);
    
    gen.returnValue();
    gen.endMethod();
    
    classWriter.visitEnd();
  }

  static String normalizeQuotes(String text) {
    StringBuilder out = new StringBuilder(text.length());
    boolean inDoubleQuotes = false;
    for (int i = 0; i < text.length(); ++i) {
      char c = text.charAt(i);
      if (c == '\\') {
        c = text.charAt(++i);
        if (c == '\\') {
          out.append('\\'); // re-escape the backslash
        }
        // no escape for double quote
      } else if (c == '\'') {
        if (inDoubleQuotes) {
          // escape in output
          out.append('\\');
        } else {
          int j = findSingleQuoteStringEnd(text, i);
          out.append(text, i, j); // copy up to end quote (leave end for append below)
          i = j;
        }
      } else if (c == '"') {
        c = '\''; // change beginning/ending doubles to singles
        inDoubleQuotes = !inDoubleQuotes;
      }
      out.append(c);
    }
    return out.toString();
  }

  static int findSingleQuoteStringEnd(String text, int start) {
    ++start; // skip beginning
    while (text.charAt(start) != '\'') {
      if (text.charAt(start) == '\\') {
        ++start; // blindly consume escape value
      }
      ++start;
    }
    return start;
  }
  
  /** 
   * The default set of functions available to expressions.
   * <p>
   * See the {@link org.apache.lucene.expressions.js package documentation}
   * for a list.
   */
  public static final Map<String,Method> DEFAULT_FUNCTIONS;
  static {
    Map<String,Method> map = new HashMap<>();
    try {
      final Properties props = new Properties();
      try (Reader in = IOUtils.getDecodingReader(JavascriptCompiler.class,
        JavascriptCompiler.class.getSimpleName() + ".properties", StandardCharsets.UTF_8)) {
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
        Method method = clazz.getMethod(methodName, args);
        checkFunction(method);
        map.put(call, method);
      }
    } catch (ReflectiveOperationException | IOException e) {
      throw new Error("Cannot resolve function", e);
    }
    DEFAULT_FUNCTIONS = Collections.unmodifiableMap(map);
  }
  
  /** Check Method signature for compatibility. */
  private static void checkFunction(Method method) {
    // check that the Method is public in some public reachable class:
    final MethodType type;
    try {
      type = MethodHandles.publicLookup().unreflect(method).type();
    } catch (IllegalAccessException iae) {
      throw new IllegalArgumentException(method + " is not accessible (declaring class or method not public).");
    }
    // do some checks if the signature is "compatible":
    if (!Modifier.isStatic(method.getModifiers())) {
      throw new IllegalArgumentException(method + " is not static.");
    }
    for (int arg = 0, arity = type.parameterCount(); arg < arity; arg++) {
      if (type.parameterType(arg) != double.class) {
        throw new IllegalArgumentException(method + " must take only double parameters.");
      }
    }
    if (type.returnType() != double.class) {
      throw new IllegalArgumentException(method + " does not return a double.");
    }
  }
  
  /** Cross check if declaring class of given method is the same as
   * returned by the given parent {@link ClassLoader} on string lookup.
   * This prevents {@link NoClassDefFoundError}.
   */
  private static void checkFunctionClassLoader(Method method, ClassLoader parent) {
    boolean ok = false;
    try {
      final Class<?> clazz = method.getDeclaringClass();
      ok = Class.forName(clazz.getName(), false, parent) == clazz;
    } catch (ClassNotFoundException e) {
      ok = false;
    }
    if (!ok) {
      throw new IllegalArgumentException(method + " is not declared by a class which is accessible by the given parent ClassLoader.");
    }
  }
}
