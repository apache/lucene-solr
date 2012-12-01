package org.apache.lucene.validation;

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

import org.objectweb.asm.ClassReader;
import org.objectweb.asm.Label;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.Method;

import org.apache.tools.ant.AntClassLoader;
import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.Task;
import org.apache.tools.ant.types.Path;
import org.apache.tools.ant.types.FileSet;
import org.apache.tools.ant.types.Reference;
import org.apache.tools.ant.types.Resource;
import org.apache.tools.ant.types.ResourceCollection;
import org.apache.tools.ant.types.resources.FileResource;
import org.apache.tools.ant.types.resources.Resources;
import org.apache.tools.ant.types.resources.FileResource;
import org.apache.tools.ant.types.resources.StringResource;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.Reader;
import java.io.File;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.HashSet;
import java.util.Set;

/**
 * Task to check if a set of class files contains calls to forbidden APIs
 * from a given classpath and list of API signatures (either inline or as pointer to files).
 * In contrast to other ANT tasks, this tool does only visit the given classpath
 * and the system classloader. It uses the local classpath in preference to the system classpath
 * (which violates the spec).
 */
public class ForbiddenApisCheckTask extends Task {

  private final Resources classFiles = new Resources();
  private final Resources apiSignatures = new Resources();
  private Path classpath = null;
  
  private boolean failOnUnsupportedJava = false;
  
  ClassLoader loader = null;
  
  final Map<String,ClassSignatureLookup> classesToCheck = new HashMap<String,ClassSignatureLookup>();
  final Map<String,ClassSignatureLookup> classpathClassCache = new HashMap<String,ClassSignatureLookup>();
  
  final Map<String,String> forbiddenFields = new HashMap<String,String>();
  final Map<String,String> forbiddenMethods = new HashMap<String,String>();
  final Map<String,String> forbiddenClasses = new HashMap<String,String>();
  
  /** Reads a class (binary name) from the given {@link ClassLoader}. */
  ClassSignatureLookup getClassFromClassLoader(final String clazz) throws BuildException {
    ClassSignatureLookup c = classpathClassCache.get(clazz);
    if (c == null) {
      try {
        final InputStream in = loader.getResourceAsStream(clazz.replace('.', '/') + ".class");
        if (in == null) {
          throw new BuildException("Loading of class " + clazz + " failed: Not found");
        }
        try {
          classpathClassCache.put(clazz, c = new ClassSignatureLookup(new ClassReader(in)));
        } finally {
          in.close();
        }
      } catch (IOException ioe) {
        throw new BuildException("Loading of class " + clazz + " failed.", ioe);
      }
    }
    return c;
  }
 
  /** Adds the method signature to the list of disallowed methods. The Signature is checked against the given ClassLoader. */
  private void addSignature(final String signature) throws BuildException {
    final String clazz, field;
    final Method method;
    int p = signature.indexOf('#');
    if (p >= 0) {
      clazz = signature.substring(0, p);
      final String s = signature.substring(p + 1);
      p = s.indexOf('(');
      if (p >= 0) {
        if (p == 0) {
          throw new BuildException("Invalid method signature (method name missing): " + signature);
        }
        // we ignore the return type, its just to match easier (so return type is void):
        try {
          method = Method.getMethod("void " + s, true);
        } catch (IllegalArgumentException iae) {
          throw new BuildException("Invalid method signature: " + signature);
        }
        field = null;
      } else {
        field = s;
        method = null;
      }
    } else {
      clazz = signature;
      method = null;
      field = null;
    }
    // check class & method/field signature, if it is really existent (in classpath), but we don't really load the class into JVM:
    final ClassSignatureLookup c = getClassFromClassLoader(clazz);
    if (method != null) {
      assert field == null;
      // list all methods with this signature:
      boolean found = false;
      for (final Method m : c.methods) {
        if (m.getName().equals(method.getName()) && Arrays.equals(m.getArgumentTypes(), method.getArgumentTypes())) {
          found = true;
          forbiddenMethods.put(c.reader.getClassName() + '\000' + m, signature);
          // don't break when found, as there may be more covariant overrides!
        }
      }
      if (!found) {
        throw new BuildException("No method found with following signature: " + signature);
      }
    } else if (field != null) {
      assert method == null;
      if (!c.fields.contains(field)) {
        throw new BuildException("No field found with following name: " + signature);
      }
      forbiddenFields.put(c.reader.getClassName() + '\000' + field, signature);
    } else {
      assert field == null && method == null;
      // only add the signature as class name
      forbiddenClasses.put(c.reader.getClassName(), signature);
    }
  }

  /** Reads a list of API signatures. Closes the Reader when done (on Exception, too)! */
  private void parseApiFile(Reader reader) throws IOException {
    final BufferedReader r = new BufferedReader(reader);
    try {
      String line;
      while ((line = r.readLine()) != null) {
        line = line.trim();
        if (line.length() == 0 || line.startsWith("#"))
          continue;
        addSignature(line);
      }
    } finally {
      r.close();
    }
  }
  
  /** Parses a class given as (FileSet) Resource */
  private ClassReader loadClassFromResource(final Resource res) throws BuildException {
    try {
      final InputStream stream = res.getInputStream();
      try {
        return new ClassReader(stream);
      } finally {
        stream.close();
      }
    } catch (IOException ioe) {
      throw new BuildException("IO problem while reading class file " + res, ioe);
    }
  }
  
  /** Parses a class given as Resource and checks for valid method invocations */
  private int checkClass(final ClassReader reader) {
    final int[] violations = new int[1];
    reader.accept(new ClassVisitor(Opcodes.ASM4) {
      final String className = Type.getObjectType(reader.getClassName()).getClassName();
      String source = null;
      
      @Override
      public void visitSource(String source, String debug) {
        this.source = source;
      }
      
      @Override
      public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
        return new MethodVisitor(Opcodes.ASM4) {
          private int lineNo = -1;
          
          private ClassSignatureLookup lookupRelatedClass(String internalName) {
            ClassSignatureLookup c = classesToCheck.get(internalName);
            if (c == null) try {
              c = getClassFromClassLoader(internalName);
            } catch (BuildException be) {
              // we ignore lookup errors and simply ignore this related class
              c = null;
            }
            return c;
          }
          
          private boolean checkClassUse(String owner) {
            final String printout = forbiddenClasses.get(owner);
            if (printout != null) {
              log("Forbidden class use: " + printout, Project.MSG_ERR);
              return true;
            }
            return false;
          }
          
          private boolean checkMethodAccess(String owner, Method method) {
            if (checkClassUse(owner)) {
              return true;
            }
            final String printout = forbiddenMethods.get(owner + '\000' + method);
            if (printout != null) {
              log("Forbidden method invocation: " + printout, Project.MSG_ERR);
              return true;
            }
            final ClassSignatureLookup c = lookupRelatedClass(owner);
            if (c != null && !c.methods.contains(method)) {
              final String superName = c.reader.getSuperName();
              if (superName != null && checkMethodAccess(superName, method)) {
                return true;
              }
              final String[] interfaces = c.reader.getInterfaces();
              if (interfaces != null) {
                for (String intf : interfaces) {
                  if (intf != null && checkMethodAccess(intf, method)) {
                    return true;
                  }
                }
              }
            }
            return false;
          }
          
          private boolean checkFieldAccess(String owner, String field) {
            if (checkClassUse(owner)) {
              return true;
            }
            final String printout = forbiddenFields.get(owner + '\000' + field);
            if (printout != null) {
              log("Forbidden field access: " + printout, Project.MSG_ERR);
              return true;
            }
            final ClassSignatureLookup c = lookupRelatedClass(owner);
            if (c != null && !c.fields.contains(field)) {
              final String superName = c.reader.getSuperName();
              if (superName != null && checkFieldAccess(superName, field)) {
                return true;
              }
              final String[] interfaces = c.reader.getInterfaces();
              if (interfaces != null) {
                for (String intf : interfaces) {
                  if (intf != null && checkFieldAccess(intf, field)) {
                    return true;
                  }
                }
              }
            }
            return false;
          }

          @Override
          public void visitMethodInsn(int opcode, String owner, String name, String desc) {
            if (checkMethodAccess(owner, new Method(name, desc))) {
              violations[0]++;
              reportSourceAndLine();
            }
          }
          
          @Override
          public void visitFieldInsn(int opcode, String owner, String name, String desc) {
            if (checkFieldAccess(owner, name)) {
             violations[0]++;
             reportSourceAndLine();
            }
          }

          private void reportSourceAndLine() {
            final StringBuilder sb = new StringBuilder("  in ").append(className);
            if (source != null && lineNo >= 0) {
              new Formatter(sb, Locale.ROOT).format(" (%s:%d)", source, lineNo).flush();
            }
            log(sb.toString(), Project.MSG_ERR);
          }
          
          @Override
          public void visitLineNumber(int lineNo, Label start) {
            this.lineNo = lineNo;
          }
        };
      }
    }, ClassReader.SKIP_FRAMES);
    return violations[0];
  }
  
  @Override
  public void execute() throws BuildException {
    AntClassLoader antLoader = null;
    try {
      if (classpath != null) {
        classpath.setProject(getProject());
        this.loader = antLoader = getProject().createClassLoader(ClassLoader.getSystemClassLoader(), classpath);
        // force that loading from this class loader is done first, then parent is asked.
        // This violates spec, but prevents classes in any system classpath to be used if a local one is available:
        antLoader.setParentFirst(false);
      } else {
        this.loader = ClassLoader.getSystemClassLoader();
      }
      classFiles.setProject(getProject());
      apiSignatures.setProject(getProject());
      
      final long start = System.currentTimeMillis();
      
      // check if we can load runtime classes (e.g. java.lang.String).
      // If this fails, we have a newer Java version than ASM supports:
      try {
        getClassFromClassLoader(String.class.getName());
      } catch (IllegalArgumentException iae) {
        final String msg = String.format(Locale.ROOT, 
          "Your Java version (%s) is not supported by <%s/>. Please run the checks with a supported JDK!",
          System.getProperty("java.version"), getTaskName());
        if (failOnUnsupportedJava) {
          throw new BuildException(msg);
        } else {
          log("WARNING: " + msg, Project.MSG_WARN);
          return;
        }
      }

      try {
        @SuppressWarnings("unchecked")
        Iterator<Resource> iter = (Iterator<Resource>) apiSignatures.iterator();
        if (!iter.hasNext()) {
          throw new BuildException("You need to supply at least one API signature definition through apiFile=, <apiFileSet/>, or inner text.");
        }
        while (iter.hasNext()) {
          final Resource r = iter.next();
          if (!r.isExists()) { 
            throw new BuildException("Resource does not exist: " + r);
          }
          if (r instanceof StringResource) {
            final String s = ((StringResource) r).getValue();
            if (s != null && s.trim().length() > 0) {
              log("Reading inline API signatures...", Project.MSG_INFO);
              parseApiFile(new StringReader(s));
            }
          } else {
            log("Reading API signatures: " + r, Project.MSG_INFO);
            parseApiFile(new InputStreamReader(r.getInputStream(), "UTF-8"));
          }
        }
      } catch (IOException ioe) {
        throw new BuildException("IO problem while reading files with API signatures.", ioe);
      }
      if (forbiddenMethods.isEmpty() && forbiddenClasses.isEmpty()) {
        throw new BuildException("No API signatures found; use apiFile=, <apiFileSet/>, or inner text to define those!");
      }

      log("Loading classes to check...", Project.MSG_INFO);
            
      @SuppressWarnings("unchecked")
      Iterator<Resource> iter = (Iterator<Resource>) classFiles.iterator();
      if (!iter.hasNext()) {
        throw new BuildException("There is no <fileset/> given or the fileset does not contain any class files to check.");
      }
      while (iter.hasNext()) {
        final Resource r = iter.next();
        if (!r.isExists()) { 
          throw new BuildException("Class file does not exist: " + r);
        }

        ClassReader reader = loadClassFromResource(r);
        classesToCheck.put(reader.getClassName(), new ClassSignatureLookup(reader));
      }

      log("Scanning for API signatures and dependencies...", Project.MSG_INFO);

      int errors = 0;
      for (final ClassSignatureLookup c : classesToCheck.values()) {
        errors += checkClass(c.reader);
      }

      log(String.format(Locale.ROOT, 
          "Scanned %d (and %d related) class file(s) for forbidden API invocations (in %.2fs), %d error(s).",
          classesToCheck.size(), classpathClassCache.size(), (System.currentTimeMillis() - start) / 1000.0, errors),
          errors > 0 ? Project.MSG_ERR : Project.MSG_INFO);

      if (errors > 0) {
        throw new BuildException("Check for forbidden API calls failed, see log.");
      }
    } finally {
      this.loader = null;
      if (antLoader != null) antLoader.cleanup();
      antLoader = null;
      classesToCheck.clear();
      classpathClassCache.clear();
      forbiddenFields.clear();
      forbiddenMethods.clear();
      forbiddenClasses.clear();
    }
  }
  
  /** Set of class files to check */
  public void add(ResourceCollection rc) {
    classFiles.add(rc);
  }
  
  /** A file with API signatures apiFile= attribute */
  public void setApiFile(File file) {
    apiSignatures.add(new FileResource(getProject(), file));
  }
  
  /** Set of files with API signatures as <apiFileSet/> nested element */
  public FileSet createApiFileSet() {
    final FileSet fs = new FileSet();
    fs.setProject(getProject());
    apiSignatures.add(fs);
    return fs;
  }

  /** Support for API signatures list as nested text */
  public void addText(String text) {
    apiSignatures.add(new StringResource(getProject(), text));
  }

  /** Classpath as classpath= attribute */
  public void setClasspath(Path classpath) {
    createClasspath().append(classpath);
  }

  /** Classpath as classpathRef= attribute */
  public void setClasspathRef(Reference r) {
    createClasspath().setRefid(r);
  }

  /** Classpath as <classpath/> nested element */
  public Path createClasspath() {
    if (this.classpath == null) {
        this.classpath = new Path(getProject());
    }
    return this.classpath.createPath();
  }
  
  public void setFailOnUnsupportedJava(boolean failOnUnsupportedJava) {
    this.failOnUnsupportedJava = failOnUnsupportedJava;
  }

  static final class ClassSignatureLookup {
    public final ClassReader reader;
    public final Set<Method> methods;
    public final Set<String> fields;
    
    public ClassSignatureLookup(final ClassReader reader) {
      this.reader = reader;
      final Set<Method> methods = new HashSet<Method>();
      final Set<String> fields = new HashSet<String>();
      reader.accept(new ClassVisitor(Opcodes.ASM4) {
        @Override
        public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
          final Method m = new Method(name, desc);
          methods.add(m);
          return null;
        }
        
        @Override
        public FieldVisitor visitField(int access, String name, String desc, String signature, Object value) {
          fields.add(name);
          return null;
        }
      }, ClassReader.SKIP_CODE | ClassReader.SKIP_DEBUG | ClassReader.SKIP_FRAMES);
      this.methods = Collections.unmodifiableSet(methods);
      this.fields = Collections.unmodifiableSet(fields);
    }
  }

}
