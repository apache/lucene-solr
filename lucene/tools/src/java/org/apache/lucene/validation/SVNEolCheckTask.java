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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Task;
import org.apache.tools.ant.types.Resource;
import org.apache.tools.ant.types.ResourceCollection;
import org.apache.tools.ant.types.resources.FileResource;
import org.apache.tools.ant.types.resources.Resources;

/**
 * Checks all files to ensure they have svn:eol-style, or
 * have a binary svn:mime-type.
 * <p>
 * TODO: check that this value is actually correct, not just present.
 * <p>
 * WARNING: slow!
 */
public class SVNEolCheckTask extends Task {
  
  private final Resources files = new Resources();
  
  private String svnExecutable;
  
  /** Set of files to check */
  public void add(ResourceCollection rc) {
    files.add(rc);
  }
  
  /** svn.exe executable */
  public void setSvnExecutable(String svnExecutable) {
    this.svnExecutable = svnExecutable;
  }

  @Override
  public void execute() throws BuildException {
    if (svnExecutable == null) {
      throw new BuildException("svnExecutable parameter must be set!");
    }
    boolean success = true;
    files.setProject(getProject());
    Iterator<Resource> iter = (Iterator<Resource>) files.iterator();
    while (iter.hasNext()) {
      Resource r = iter.next();
      if (!(r instanceof FileResource)) {
        throw new BuildException("Only filesystem resource are supported: " + r.getName()
            + ", was: " + r.getClass().getName());
      }

      File f = ((FileResource) r).getFile();
      List<String> cmd = new ArrayList<String>();
      cmd.add(svnExecutable);
      cmd.add("pget");
      cmd.add("svn:eol-style");
      cmd.add(f.getAbsolutePath());
      String eolStyle = exec(cmd);
      if (eolStyle.isEmpty()) {
        cmd.clear();
        cmd.add(svnExecutable);
        cmd.add("pget");
        cmd.add("svn:mime-type");
        cmd.add(f.getAbsolutePath());
        String binProp = exec(cmd);
        if (!binProp.startsWith("application/") && !binProp.startsWith("image/")) {
          success = false;
          log(r.getName() + " missing svn:eol-style (or binary svn:mime-type).");
        }
      }
    }
    if (!success) {
      throw new BuildException("Some svn properties are missing");
    }
  }
  
  private String exec(List<String> cmd) throws BuildException {
    ProcessBuilder pb = new ProcessBuilder(cmd);
    pb.redirectErrorStream(true);
    BufferedReader r = null;
    StringBuilder sb = new StringBuilder();
    try {
      Process p = pb.start();
      InputStream is = p.getInputStream();
      r = new BufferedReader(new InputStreamReader(is, Charset.defaultCharset()));
      int ch;
      while ((ch = r.read()) > 0) {
        sb.append((char)ch);
      }
      p.waitFor();
      return sb.toString();
    } catch (Exception e) {
      throw new BuildException(e);
    } finally {
      if (r != null) {
        try {
          r.close();
        } catch (IOException e) {}
      }
    }
  }
}
