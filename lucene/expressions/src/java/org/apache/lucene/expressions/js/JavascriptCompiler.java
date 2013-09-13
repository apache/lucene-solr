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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CharStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.tree.Tree;
import org.apache.lucene.expressions.Expression;
import org.apache.lucene.queries.function.FunctionValues;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import static org.objectweb.asm.Opcodes.AALOAD;
import static org.objectweb.asm.Opcodes.ACC_FINAL;
import static org.objectweb.asm.Opcodes.ACC_PUBLIC;
import static org.objectweb.asm.Opcodes.ACC_SUPER;
import static org.objectweb.asm.Opcodes.ALOAD;
import static org.objectweb.asm.Opcodes.BIPUSH;
import static org.objectweb.asm.Opcodes.D2I;
import static org.objectweb.asm.Opcodes.D2L;
import static org.objectweb.asm.Opcodes.DADD;
import static org.objectweb.asm.Opcodes.DCMPG;
import static org.objectweb.asm.Opcodes.DCMPL;
import static org.objectweb.asm.Opcodes.DCONST_0;
import static org.objectweb.asm.Opcodes.DCONST_1;
import static org.objectweb.asm.Opcodes.DDIV;
import static org.objectweb.asm.Opcodes.DNEG;
import static org.objectweb.asm.Opcodes.DREM;
import static org.objectweb.asm.Opcodes.DRETURN;
import static org.objectweb.asm.Opcodes.DSUB;
import static org.objectweb.asm.Opcodes.GOTO;
import static org.objectweb.asm.Opcodes.I2D;
import static org.objectweb.asm.Opcodes.I2L;
import static org.objectweb.asm.Opcodes.ICONST_0;
import static org.objectweb.asm.Opcodes.ICONST_1;
import static org.objectweb.asm.Opcodes.ICONST_2;
import static org.objectweb.asm.Opcodes.ICONST_3;
import static org.objectweb.asm.Opcodes.ICONST_4;
import static org.objectweb.asm.Opcodes.ICONST_5;
import static org.objectweb.asm.Opcodes.IFEQ;
import static org.objectweb.asm.Opcodes.IFGE;
import static org.objectweb.asm.Opcodes.IFGT;
import static org.objectweb.asm.Opcodes.IFLE;
import static org.objectweb.asm.Opcodes.IFLT;
import static org.objectweb.asm.Opcodes.IFNE;
import static org.objectweb.asm.Opcodes.ILOAD;
import static org.objectweb.asm.Opcodes.INVOKESPECIAL;
import static org.objectweb.asm.Opcodes.INVOKESTATIC;
import static org.objectweb.asm.Opcodes.INVOKEVIRTUAL;
import static org.objectweb.asm.Opcodes.L2D;
import static org.objectweb.asm.Opcodes.L2I;
import static org.objectweb.asm.Opcodes.LAND;
import static org.objectweb.asm.Opcodes.LCONST_0;
import static org.objectweb.asm.Opcodes.LCONST_1;
import static org.objectweb.asm.Opcodes.LOR;
import static org.objectweb.asm.Opcodes.LSHL;
import static org.objectweb.asm.Opcodes.LSHR;
import static org.objectweb.asm.Opcodes.LUSHR;
import static org.objectweb.asm.Opcodes.LXOR;
import static org.objectweb.asm.Opcodes.RETURN;
import static org.objectweb.asm.Opcodes.SIPUSH;
import static org.objectweb.asm.Opcodes.V1_7;

/**
 * An expression compiler for javascript expressions.
 * <p>
 * Example:
 * <pre class="prettyprint">
 *   Expression foo = JavascriptCompiler.compile("((0.3*popularity)/10.0)+(0.7*score)");
 * </pre>
 * <p>
 * See the {@link org.apache.lucene.expressions.js package documentation} for 
 * the supported syntax and functions.
 * 
 * @lucene.experimental
 */
public class JavascriptCompiler {
  private static enum ComputedType {
    INT, LONG, DOUBLE
  }

  static class Loader extends ClassLoader {
    Loader(ClassLoader parent) {
      super(parent);
    }

    public Class<? extends Expression> define(String className, byte[] bytecode) {
      return super.defineClass(className, bytecode, 0, bytecode.length).asSubclass(Expression.class);
    }
  }
  
  private static final String EXPRESSION_CLASS_PREFIX = JavascriptCompiler.class.getPackage().getName() + ".Expr_";
  private static final String COMPILED_EXPRESSION_INTERNAL = Type.getInternalName(Expression.class);
  
  private static final Type FUNCTION_VALUES_TYPE = Type.getType(FunctionValues.class);
  private static final Type FUNCTION_VALUES_ARRAY_TYPE = Type.getType(FunctionValues[].class);
  private static final Type STRING_TYPE = Type.getType(String.class);
  private static final Type STRING_ARRAY_TYPE = Type.getType(String[].class);
  
  private static final String CONSTRUCTOR_DESC = Type.getMethodDescriptor(Type.VOID_TYPE, STRING_TYPE, STRING_ARRAY_TYPE);
  private static final String EVALUATE_METHOD_DESC = Type.getMethodDescriptor(Type.DOUBLE_TYPE, Type.INT_TYPE, FUNCTION_VALUES_ARRAY_TYPE);
  private static final String DOUBLE_VAL_METHOD_DESC = Type.getMethodDescriptor(Type.DOUBLE_TYPE, Type.INT_TYPE);
  
  private final Loader loader = new Loader(getClass().getClassLoader());
  
  private ClassWriter classWriter;
  private MethodVisitor methodVisitor;
  private Map<String, Integer> externalsMap;
  private List<String> externalsList;
  
  /**
   * Compiles the given expression.
   *
   * @param sourceText The expression to compile
   * @return A new compiled expression
   * @throws ParseException on failure to compile
   */
  public static Expression compile(String sourceText) throws ParseException {
    return new JavascriptCompiler().compileExpression(sourceText);
  }
  
  /**
   * Constructs a compiler for expressions.
   */
  private JavascriptCompiler() {
  }
  
  /**
   * Compiles the given expression.
   *
   * @param sourceText The expression to compile
   * @return A new compiled expression
   * @throws ParseException on failure to compile
   */
  private Expression compileExpression(String sourceText) throws ParseException {
    if (sourceText == null) {
      throw new NullPointerException();
    }
    final String className = EXPRESSION_CLASS_PREFIX + createClassName(sourceText);
    // System.out.println(sourceText + "|" + className);
    try {
      externalsMap = new HashMap<String, Integer>();
      externalsList = new ArrayList<String>();
      
      Tree antlrTree = getAntlrComputedExpressionTree(sourceText);
      
      beginCompile(className);
      recursiveCompile(antlrTree, ComputedType.DOUBLE);
      endCompile();
      
      Class<? extends Expression> evaluatorClass = loader.define(className, classWriter.toByteArray());
      Constructor<? extends Expression> constructor = evaluatorClass.getConstructor(String.class, String[].class);
      return constructor.newInstance(sourceText, externalsList.toArray(new String[externalsList.size()]));
    } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException exception) {
      throw new IllegalStateException("An internal error occurred attempting to compile the expression (" + className + ").", exception);
    }
  }
  
  private String createClassName(String sourceText) {
    final StringBuilder sb = new StringBuilder(sourceText.length() / 2);
    boolean wasIdentifierPart = true;
    for (int i = 0, c = sourceText.length(); i < c; i++) {
      final char ch = sourceText.charAt(i);
      if (Character.isJavaIdentifierPart(ch)) {
        sb.append(ch);
        wasIdentifierPart = true;
      } else if (wasIdentifierPart) {
        sb.append('_');
        wasIdentifierPart = false;
      }
    }
    for (int i = sb.length() - 1; i >= 0; i--) {
      if (sb.charAt(i) == '_') {
        sb.setLength(i);
      } else {
        break;
      }
    }
    return sb.toString();
  }

  private void beginCompile(String className) {
    classWriter = new ClassWriter(ClassWriter.COMPUTE_FRAMES);
    classWriter.visit(V1_7, ACC_PUBLIC + ACC_SUPER + ACC_FINAL, className.replace('.', '/'),
        null, COMPILED_EXPRESSION_INTERNAL, null);
    MethodVisitor constructor = classWriter.visitMethod(ACC_PUBLIC, "<init>", CONSTRUCTOR_DESC, null, null);
    constructor.visitCode();
    constructor.visitVarInsn(ALOAD, 0);
    constructor.visitVarInsn(ALOAD, 1);
    constructor.visitVarInsn(ALOAD, 2);
    constructor.visitMethodInsn(INVOKESPECIAL, COMPILED_EXPRESSION_INTERNAL, "<init>", CONSTRUCTOR_DESC);
    constructor.visitInsn(RETURN);
    constructor.visitMaxs(0, 0);
    constructor.visitEnd();
    
    methodVisitor = classWriter.visitMethod(ACC_PUBLIC, "evaluate", EVALUATE_METHOD_DESC, null, null);
    methodVisitor.visitCode();
  }
  
  private void recursiveCompile(Tree current, ComputedType expected) {
    int type = current.getType();
    String text = current.getText();
    
    switch (type) {
      case JavascriptParser.AT_CALL:
        Tree identifier = current.getChild(0);
        String call = identifier.getText();
        int arguments = current.getChildCount() - 1;
        
        JavascriptFunction method = JavascriptFunction.getMethod(call, arguments);
        
        for (int argument = 1; argument <= arguments; ++argument) {
          recursiveCompile(current.getChild(argument), ComputedType.DOUBLE);
        }
        
        methodVisitor.visitMethodInsn(INVOKESTATIC, method.klass, method.method, method.signature);
        
        typeCompile(expected, ComputedType.DOUBLE);
        break;
      case JavascriptParser.ID:
        int index;
        
        if (externalsMap.containsKey(text)) {
          index = externalsMap.get(text);
        } else {
          index = externalsList.size();
          externalsList.add(text);
          externalsMap.put(text, index);
        }
        
        methodVisitor.visitVarInsn(ALOAD, 2);
        
        switch (index) {
          case 0:
            methodVisitor.visitInsn(ICONST_0);
            break;
          case 1:
            methodVisitor.visitInsn(ICONST_1);
            break;
          case 2:
            methodVisitor.visitInsn(ICONST_2);
            break;
          case 3:
            methodVisitor.visitInsn(ICONST_3);
            break;
          case 4:
            methodVisitor.visitInsn(ICONST_4);
            break;
          case 5:
            methodVisitor.visitInsn(ICONST_5);
            break;
          default:
            if (index < 128) {
              methodVisitor.visitIntInsn(BIPUSH, index);
            } else if (index < 16384) {
              methodVisitor.visitIntInsn(SIPUSH, index);
            } else {
              methodVisitor.visitLdcInsn(index);
            }
            
            break;
        }
        
        methodVisitor.visitInsn(AALOAD);
        methodVisitor.visitVarInsn(ILOAD, 1);
        methodVisitor.visitMethodInsn(INVOKEVIRTUAL, FUNCTION_VALUES_TYPE.getInternalName(), "doubleVal", DOUBLE_VAL_METHOD_DESC);
        
        typeCompile(expected, ComputedType.DOUBLE);
        break;
      case JavascriptParser.HEX:
        long hex = Long.parseLong(text.substring(2), 16);
        
        if (expected == ComputedType.INT) {
          methodVisitor.visitLdcInsn((int)hex);
        } else if (expected == ComputedType.LONG) {
          methodVisitor.visitLdcInsn(hex);
        } else {
          methodVisitor.visitLdcInsn((double)hex);
        }
        
        break;
      case JavascriptParser.OCTAL:
        long octal = Long.parseLong(text.substring(1), 8);
        
        if (expected == ComputedType.INT) {
          methodVisitor.visitLdcInsn((int)octal);
        } else if (expected == ComputedType.LONG) {
          methodVisitor.visitLdcInsn(octal);
        } else {
          methodVisitor.visitLdcInsn((double)octal);
        }
        
        break;
      case JavascriptParser.DECIMAL:
        double decimal = Double.parseDouble(text);
        methodVisitor.visitLdcInsn(decimal);
        
        typeCompile(expected, ComputedType.DOUBLE);
        break;
      case JavascriptParser.AT_NEGATE:
        recursiveCompile(current.getChild(0), ComputedType.DOUBLE);
        methodVisitor.visitInsn(DNEG);
        
        typeCompile(expected, ComputedType.DOUBLE);
        break;
      case JavascriptParser.AT_ADD:
        recursiveCompile(current.getChild(0), ComputedType.DOUBLE);
        recursiveCompile(current.getChild(1), ComputedType.DOUBLE);
        methodVisitor.visitInsn(DADD);
        
        typeCompile(expected, ComputedType.DOUBLE);
        break;
      case JavascriptParser.AT_SUBTRACT:
        recursiveCompile(current.getChild(0), ComputedType.DOUBLE);
        recursiveCompile(current.getChild(1), ComputedType.DOUBLE);
        methodVisitor.visitInsn(DSUB);
        
        typeCompile(expected, ComputedType.DOUBLE);
        break;
      case JavascriptParser.AT_MULTIPLY:
        recursiveCompile(current.getChild(0), ComputedType.DOUBLE);
        recursiveCompile(current.getChild(1), ComputedType.DOUBLE);
        methodVisitor.visitInsn(Opcodes.DMUL);
        
        typeCompile(expected, ComputedType.DOUBLE);
        break;
      case JavascriptParser.AT_DIVIDE:
        recursiveCompile(current.getChild(0), ComputedType.DOUBLE);
        recursiveCompile(current.getChild(1), ComputedType.DOUBLE);
        methodVisitor.visitInsn(DDIV);
        
        typeCompile(expected, ComputedType.DOUBLE);
        break;
      case JavascriptParser.AT_MODULO:
        recursiveCompile(current.getChild(0), ComputedType.DOUBLE);
        recursiveCompile(current.getChild(1), ComputedType.DOUBLE);
        methodVisitor.visitInsn(DREM);
        
        typeCompile(expected, ComputedType.DOUBLE);
        break;
      case JavascriptParser.AT_BIT_SHL:
        recursiveCompile(current.getChild(0), ComputedType.LONG);
        recursiveCompile(current.getChild(1), ComputedType.INT);
        methodVisitor.visitInsn(LSHL);
        
        typeCompile(expected, ComputedType.LONG);
        break;
      case JavascriptParser.AT_BIT_SHR:
        recursiveCompile(current.getChild(0), ComputedType.LONG);
        recursiveCompile(current.getChild(1), ComputedType.INT);
        methodVisitor.visitInsn(LSHR);
        
        typeCompile(expected, ComputedType.LONG);
        break;
      case JavascriptParser.AT_BIT_SHU:
        recursiveCompile(current.getChild(0), ComputedType.LONG);
        recursiveCompile(current.getChild(1), ComputedType.INT);
        methodVisitor.visitInsn(LUSHR);
        
        typeCompile(expected, ComputedType.LONG);
        break;
      case JavascriptParser.AT_BIT_AND:
        recursiveCompile(current.getChild(0), ComputedType.LONG);
        recursiveCompile(current.getChild(1), ComputedType.LONG);
        methodVisitor.visitInsn(LAND);
        
        typeCompile(expected, ComputedType.LONG);
        break;
      case JavascriptParser.AT_BIT_OR:
        recursiveCompile(current.getChild(0), ComputedType.LONG);
        recursiveCompile(current.getChild(1), ComputedType.LONG);
        methodVisitor.visitInsn(LOR);
        
        typeCompile(expected, ComputedType.LONG);            
        break;
      case JavascriptParser.AT_BIT_XOR:
        recursiveCompile(current.getChild(0), ComputedType.LONG);
        recursiveCompile(current.getChild(1), ComputedType.LONG);
        methodVisitor.visitInsn(LXOR);
        
        typeCompile(expected, ComputedType.LONG);            
        break;
      case JavascriptParser.AT_BIT_NOT:
        recursiveCompile(current.getChild(0), ComputedType.LONG);
        methodVisitor.visitLdcInsn(new Long(-1));
        methodVisitor.visitInsn(LXOR);
        
        typeCompile(expected, ComputedType.LONG);
        break;
      case JavascriptParser.AT_COMP_EQ:
        Label labelEqTrue = new Label();
        Label labelEqReturn = new Label();
        
        recursiveCompile(current.getChild(0), ComputedType.DOUBLE);
        recursiveCompile(current.getChild(1), ComputedType.DOUBLE);
        methodVisitor.visitInsn(DCMPL);
        
        methodVisitor.visitJumpInsn(IFEQ, labelEqTrue);
        truthCompile(expected, false);
        methodVisitor.visitJumpInsn(GOTO, labelEqReturn);
        methodVisitor.visitLabel(labelEqTrue);
        truthCompile(expected, true);
        methodVisitor.visitLabel(labelEqReturn);
        
        break;
      case JavascriptParser.AT_COMP_NEQ:
        Label labelNeqTrue = new Label();
        Label labelNeqReturn = new Label();
        
        recursiveCompile(current.getChild(0), ComputedType.DOUBLE);
        recursiveCompile(current.getChild(1), ComputedType.DOUBLE);
        methodVisitor.visitInsn(DCMPL);
        
        methodVisitor.visitJumpInsn(IFNE, labelNeqTrue);
        truthCompile(expected, false);
        methodVisitor.visitJumpInsn(GOTO, labelNeqReturn);
        methodVisitor.visitLabel(labelNeqTrue);
        truthCompile(expected, true);
        methodVisitor.visitLabel(labelNeqReturn);
        
        break;
      case JavascriptParser.AT_COMP_LT:
        Label labelLtTrue = new Label();
        Label labelLtReturn = new Label();
        
        recursiveCompile(current.getChild(0), ComputedType.DOUBLE);
        recursiveCompile(current.getChild(1), ComputedType.DOUBLE);
        methodVisitor.visitInsn(DCMPG);
        
        methodVisitor.visitJumpInsn(IFLT, labelLtTrue);
        truthCompile(expected, false);
        methodVisitor.visitJumpInsn(GOTO, labelLtReturn);
        methodVisitor.visitLabel(labelLtTrue);
        truthCompile(expected, true);
        methodVisitor.visitLabel(labelLtReturn);
        
        break;
      case JavascriptParser.AT_COMP_GT:
        Label labelGtTrue = new Label();
        Label labelGtReturn = new Label();
        
        recursiveCompile(current.getChild(0), ComputedType.DOUBLE);
        recursiveCompile(current.getChild(1), ComputedType.DOUBLE);
        methodVisitor.visitInsn(DCMPL);
        
        methodVisitor.visitJumpInsn(IFGT, labelGtTrue);
        truthCompile(expected, false);
        methodVisitor.visitJumpInsn(GOTO, labelGtReturn);
        methodVisitor.visitLabel(labelGtTrue);
        truthCompile(expected, true);
        methodVisitor.visitLabel(labelGtReturn);
        
        break;
      case JavascriptParser.AT_COMP_LTE:
        Label labelLteTrue = new Label();
        Label labelLteReturn = new Label();
        
        recursiveCompile(current.getChild(0), ComputedType.DOUBLE);
        recursiveCompile(current.getChild(1), ComputedType.DOUBLE);
        methodVisitor.visitInsn(DCMPG);
        
        methodVisitor.visitJumpInsn(IFLE, labelLteTrue);
        truthCompile(expected, false);
        methodVisitor.visitJumpInsn(GOTO, labelLteReturn);
        methodVisitor.visitLabel(labelLteTrue);
        truthCompile(expected, true);
        methodVisitor.visitLabel(labelLteReturn);
        
        break;
      case JavascriptParser.AT_COMP_GTE:
        Label labelGteTrue = new Label();
        Label labelGteReturn = new Label();
        
        recursiveCompile(current.getChild(0), ComputedType.DOUBLE);
        recursiveCompile(current.getChild(1), ComputedType.DOUBLE);
        methodVisitor.visitInsn(DCMPL);
        
        methodVisitor.visitJumpInsn(IFGE, labelGteTrue);
        truthCompile(expected, false);
        methodVisitor.visitJumpInsn(GOTO, labelGteReturn);
        methodVisitor.visitLabel(labelGteTrue);
        truthCompile(expected, true);
        methodVisitor.visitLabel(labelGteReturn);
        
        break;
      case JavascriptParser.AT_BOOL_NOT:
        Label labelNotTrue = new Label();
        Label labelNotReturn = new Label();
        
        recursiveCompile(current.getChild(0), ComputedType.INT);
        methodVisitor.visitJumpInsn(IFEQ, labelNotTrue);
        truthCompile(expected, false);
        methodVisitor.visitJumpInsn(GOTO, labelNotReturn);
        methodVisitor.visitLabel(labelNotTrue);
        truthCompile(expected, true);
        methodVisitor.visitLabel(labelNotReturn);
        
        break;
      case JavascriptParser.AT_BOOL_AND:
        Label andFalse = new Label();
        Label andEnd = new Label();
        
        recursiveCompile(current.getChild(0), ComputedType.INT);
        methodVisitor.visitJumpInsn(IFEQ, andFalse);
        recursiveCompile(current.getChild(1), ComputedType.INT);
        methodVisitor.visitJumpInsn(IFEQ, andFalse);
        truthCompile(expected, true);
        methodVisitor.visitJumpInsn(GOTO, andEnd);
        methodVisitor.visitLabel(andFalse);
        truthCompile(expected, false);
        methodVisitor.visitLabel(andEnd);
        
        break;
      case JavascriptParser.AT_BOOL_OR:
        Label orTrue = new Label();
        Label orEnd = new Label();
        
        recursiveCompile(current.getChild(0), ComputedType.INT);
        methodVisitor.visitJumpInsn(IFNE, orTrue);
        recursiveCompile(current.getChild(1), ComputedType.INT);
        methodVisitor.visitJumpInsn(IFNE, orTrue);
        truthCompile(expected, false);
        methodVisitor.visitJumpInsn(GOTO, orEnd);
        methodVisitor.visitLabel(orTrue);
        truthCompile(expected, true);
        methodVisitor.visitLabel(orEnd);
        
        break;
      case JavascriptParser.AT_COND_QUE:
        Label condFalse = new Label();
        Label condEnd = new Label();
        
        recursiveCompile(current.getChild(0), ComputedType.INT);
        methodVisitor.visitJumpInsn(IFEQ, condFalse);
        recursiveCompile(current.getChild(1), expected);
        methodVisitor.visitJumpInsn(GOTO, condEnd);
        methodVisitor.visitLabel(condFalse);
        recursiveCompile(current.getChild(2), expected);
        methodVisitor.visitLabel(condEnd);
        
        break;
      default:
        throw new IllegalStateException("Unknown operation specified: (" + current.getText() + ").");
    }
  }
  
  private void typeCompile(ComputedType expected, ComputedType actual) {
    if (expected == actual) {
      return;
    }
    
    switch (expected) {
      case INT:
        if (actual == ComputedType.LONG) {
          methodVisitor.visitInsn(L2I);
        } else {
          methodVisitor.visitInsn(D2I);
        }
        
        break;
      case LONG:
        if (actual == ComputedType.INT) {
          methodVisitor.visitInsn(I2L);
        } else {
          methodVisitor.visitInsn(D2L);
        }
        
        break;
      default:
        if (actual == ComputedType.INT) {
          methodVisitor.visitInsn(I2D);
        } else {
          methodVisitor.visitInsn(L2D);
        }
        
        break;
    }
  }
  
  private void truthCompile(ComputedType expected, boolean truth) {
    switch (expected) {
      case INT:
        methodVisitor.visitInsn(truth ? ICONST_1 : ICONST_0);
        
        break;
      case LONG:
        methodVisitor.visitInsn(truth ? LCONST_1 : LCONST_0);
        
        break;
      default:
        methodVisitor.visitInsn(truth ? DCONST_1 : DCONST_0);
        
        break;
    }
  }
  
  private void endCompile() {
    methodVisitor.visitInsn(DRETURN);
    methodVisitor.visitMaxs(0, 0);
    methodVisitor.visitEnd();
    
    classWriter.visitEnd();
  }

  private static Tree getAntlrComputedExpressionTree(String expression) throws ParseException {
    CharStream input = new ANTLRStringStream(expression);
    JavascriptLexer lexer = new JavascriptLexer(input);
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    JavascriptParser parser = new JavascriptParser(tokens);

    try {
      return parser.expression().tree;

    } catch (RecognitionException exception) {
      throw new IllegalArgumentException(exception);
    } catch (RuntimeException exception) {
      if (exception.getCause() instanceof ParseException) {
        throw (ParseException)exception.getCause();
      }
      throw exception;
    }
  }
}
