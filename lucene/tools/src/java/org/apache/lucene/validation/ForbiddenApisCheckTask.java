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
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.Method;
import org.objectweb.asm.tree.ClassNode;
import org.objectweb.asm.tree.FieldNode;
import org.objectweb.asm.tree.MethodNode;

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
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

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

  private final Map<String,ClassNode> classCache = new HashMap<String,ClassNode>();
  private final Map<String,String> forbiddenFields = new HashMap<String,String>();
  private final Map<String,String> forbiddenMethods = new HashMap<String,String>();
  private final Map<String,String> forbiddenClasses = new HashMap<String,String>();
  
  /** Reads a class (binary name) from the given {@link ClassLoader}.
   */
  private ClassReader readClass(final ClassLoader loader, final String clazz) throws BuildException {
    try {
      final InputStream in = loader.getResourceAsStream(clazz.replace('.', '/') + ".class");
      if (in == null) {
        throw new BuildException("Loading of class " + clazz + " failed: Not found");
      }
      try {
        return new ClassReader(in);
      } finally {
        in.close();
      }
    } catch (IOException ioe) {
      throw new BuildException("Loading of class " + clazz + " failed.", ioe);
    }
  }
 
  /** Adds the method signature to the list of disallowed methods. The Signature is checked against the given ClassLoader. */
  private void addSignature(final ClassLoader loader, final String signature) throws BuildException {
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
    ClassNode c = classCache.get(clazz);
    if (c == null) {
      readClass(loader, clazz).accept(c = new ClassNode(Opcodes.ASM4), ClassReader.SKIP_CODE | ClassReader.SKIP_DEBUG | ClassReader.SKIP_FRAMES);
      classCache.put(clazz, c);
    }
    if (method != null) {
      assert field == null;
      // list all methods with this signature:
      boolean found = false;
      for (final MethodNode mn : c.methods) {
        if (mn.name.equals(method.getName()) && Arrays.equals(Type.getArgumentTypes(mn.desc), method.getArgumentTypes())) {
          found = true;
          forbiddenMethods.put(c.name + '\000' + new Method(mn.name, mn.desc), signature);
          // don't break when found, as there may be more covariant overrides!
        }
      }
      if (!found) {
        throw new BuildException("No method found with following signature: " + signature);
      }
    } else if (field != null) {
      assert method == null;
      // list all fields to find the right one:
      boolean found = false;
      for (final FieldNode fld : c.fields) {
        if (fld.name.equals(field)) {
          found = true;
          forbiddenFields.put(c.name + '\000' + fld.name, signature);
          break;
        }
      }
      if (!found) {
        throw new BuildException("No field found with following name: " + signature);
      }
    } else {
      assert field == null && method == null;
      // only add the signature as class name
      forbiddenClasses.put(c.name, signature);
    }
  }

  /** Reads a list of API signatures. Closes the Reader when done (on Exception, too)! */
  private void parseApiFile(ClassLoader loader, Reader reader) throws IOException {
    final BufferedReader r = new BufferedReader(reader);
    try {
      String line;
      while ((line = r.readLine()) != null) {
        line = line.trim();
        if (line.length() == 0 || line.startsWith("#"))
          continue;
        addSignature(loader, line);
      }
    } finally {
      r.close();
    }
  }
  
  /** Parses a class given as Resource and checks for valid method invocations */
  private int checkClass(final Resource res) throws IOException {
    final InputStream stream = res.getInputStream();
    try {
      final int[] violations = new int[1];
      new ClassReader(stream).accept(new ClassVisitor(Opcodes.ASM4) {
        String className = null, source = null;
        
        @Override
        public void visit(int version, int access, String name, String signature, String superName, String[] interfaces) {
          // save class name in source code format:
          this.className = Type.getObjectType(name).getClassName();
        }
        
        @Override
        public void visitSource(String source, String debug) {
          this.source = source;
        }
        
        @Override
        public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
          return new MethodVisitor(Opcodes.ASM4) {
            private int lineNo = -1;
            
            private boolean checkClassUse(String owner) {
              final String printout = forbiddenClasses.get(owner);
              if (printout != null) {
                log("Forbidden class use: " + printout, Project.MSG_ERR);
                return true;
              }
              return false;
            }
            
            private void reportSourceAndLine() {
              final StringBuilder sb = new StringBuilder("  in ").append(className);
              if (source != null && lineNo >= 0) {
                new Formatter(sb, Locale.ROOT).format(" (%s:%d)", source, lineNo).flush();
              }
              log(sb.toString(), Project.MSG_ERR);
            }

            @Override
            public void visitMethodInsn(int opcode, String owner, String name, String desc) {
              boolean found = checkClassUse(owner);
              if (!found) {
                final String printout = forbiddenMethods.get(owner + '\000' + new Method(name, desc));
                if (printout != null) {
                  found = true;
                  log("Forbidden method invocation: " + printout, Project.MSG_ERR);
                }
              }
              if (found) {
                violations[0]++;
                reportSourceAndLine();
              }
            }
            
            @Override
            public void visitFieldInsn(int opcode, String owner, String name, String desc) {
              boolean found = checkClassUse(owner);
              if (!found) {
                final String printout = forbiddenFields.get(owner + '\000' + name);
                if (printout != null) {
                  found = true;
                  log("Forbidden field access: " + printout, Project.MSG_ERR);
                }
              }
              if (found) {
               violations[0]++;
               reportSourceAndLine();
              }
            }

            @Override
            public void visitLineNumber(int lineNo, Label start) {
              this.lineNo = lineNo;
            }
          };
        }
      }, ClassReader.SKIP_FRAMES);
      return violations[0];
    } finally {
      stream.close();
    }
  }
  
  @Override
  public void execute() throws BuildException {
    AntClassLoader antLoader = null;
    try {
      final ClassLoader loader;
      if (classpath != null) {
        classpath.setProject(getProject());
        loader = antLoader = getProject().createClassLoader(ClassLoader.getSystemClassLoader(), classpath);
        // force that loading from this class loader is done first, then parent is asked.
        // This violates spec, but prevents classes in any system classpath to be used if a local one is available:
        antLoader.setParentFirst(false);
      } else {
        loader = ClassLoader.getSystemClassLoader();
      }
      classFiles.setProject(getProject());
      apiSignatures.setProject(getProject());
      
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
            log("Reading inline API signatures...", Project.MSG_INFO);
            parseApiFile(loader, new StringReader(((StringResource) r).getValue()));
          } else {
            log("Reading API signatures: " + r, Project.MSG_INFO);
            parseApiFile(loader, new InputStreamReader(r.getInputStream(), "UTF-8"));
          }
        }
      } catch (IOException ioe) {
        throw new BuildException("IO problem while reading files with API signatures.", ioe);
      }
      if (forbiddenMethods.isEmpty() && forbiddenClasses.isEmpty()) {
        throw new BuildException("No API signatures found; use apiFile=, <apiFileSet/>, or inner text to define those!");
      }
    } finally {
      if (antLoader != null) antLoader.cleanup();
      antLoader = null;
    }

    long start = System.currentTimeMillis();
    
    int checked = 0;
    int errors = 0;
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

      try {
        errors += checkClass(r);
      } catch (IOException ioe) {
        throw new BuildException("IO problem while reading class file " + r, ioe);
      }
      checked++;
    }

    log(String.format(Locale.ROOT, 
        "Scanned %d class file(s) for forbidden API invocations (in %.2fs), %d error(s).",
        checked, (System.currentTimeMillis() - start) / 1000.0, errors),
        errors > 0 ? Project.MSG_ERR : Project.MSG_INFO);

    if (errors > 0) {
      throw new BuildException("Check for forbidden API calls failed, see log.");
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

}
