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
package org.apache.solr.hadoop;

import java.io.IOException;

import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.ArgumentType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;

/**
 * ArgumentType subclass for HDFS Path type, using fluent style API.
 */
public class PathArgumentType implements ArgumentType<Path> {
  
  private final Configuration conf;
  private FileSystem fs;
  private boolean acceptSystemIn = false;
  private boolean verifyExists = false;
  private boolean verifyNotExists = false;
  private boolean verifyIsFile = false;
  private boolean verifyIsDirectory = false;
  private boolean verifyCanRead = false;
  private boolean verifyCanWrite = false;
  private boolean verifyCanWriteParent = false;
  private boolean verifyCanExecute = false;
  private boolean verifyIsAbsolute = false;
  private boolean verifyHasScheme = false;
  private String verifyScheme = null;

  public PathArgumentType(Configuration conf) {
    this.conf = conf;
  }
  
  public PathArgumentType acceptSystemIn() {
    acceptSystemIn = true;
    return this;
  }
  
  public PathArgumentType verifyExists() {
    verifyExists = true;
    return this;
  }
  
  public PathArgumentType verifyNotExists() {
    verifyNotExists = true;
    return this;
  }
  
  public PathArgumentType verifyIsFile() {
    verifyIsFile = true;
    return this;
  }
  
  public PathArgumentType verifyIsDirectory() {
    verifyIsDirectory = true;
    return this;
  }
  
  public PathArgumentType verifyCanRead() {
    verifyCanRead = true;
    return this;
  }
  
  public PathArgumentType verifyCanWrite() {
    verifyCanWrite = true;
    return this;
  }
  
  public PathArgumentType verifyCanWriteParent() {
    verifyCanWriteParent = true;
    return this;
  }
  
  public PathArgumentType verifyCanExecute() {
    verifyCanExecute = true;
    return this;
  }
  
  public PathArgumentType verifyIsAbsolute() {
    verifyIsAbsolute = true;
    return this;
  }
  
  public PathArgumentType verifyHasScheme() {
    verifyHasScheme = true;
    return this;
  }
  
  public PathArgumentType verifyScheme(String scheme) {
    verifyScheme = scheme;
    return this;
  }
  
  @Override
  public Path convert(ArgumentParser parser, Argument arg, String value) throws ArgumentParserException {
    Path file = new Path(value);
    try {
      fs = file.getFileSystem(conf);
      if (verifyHasScheme && !isSystemIn(file)) {
        verifyHasScheme(parser, file);
      }        
      if (verifyScheme != null && !isSystemIn(file)) {
        verifyScheme(parser, file);
      }        
      if (verifyIsAbsolute && !isSystemIn(file)) {
        verifyIsAbsolute(parser, file);
      }
      if (verifyExists && !isSystemIn(file)) {
        verifyExists(parser, file);
      }
      if (verifyNotExists && !isSystemIn(file)) {
        verifyNotExists(parser, file);
      }
      if (verifyIsFile && !isSystemIn(file)) {
        verifyIsFile(parser, file);
      }
      if (verifyIsDirectory && !isSystemIn(file)) {
        verifyIsDirectory(parser, file);
      }
      if (verifyCanRead && !isSystemIn(file)) {
        verifyCanRead(parser, file);
      }
      if (verifyCanWrite && !isSystemIn(file)) {
        verifyCanWrite(parser, file);
      }
      if (verifyCanWriteParent && !isSystemIn(file)) {
        verifyCanWriteParent(parser, file);
      }
      if (verifyCanExecute && !isSystemIn(file)) {
        verifyCanExecute(parser, file);
      }
    } catch (IOException e) {
      throw new ArgumentParserException(e, parser);
    }
    return file;
  }
  
  private void verifyExists(ArgumentParser parser, Path file) throws ArgumentParserException, IOException {
    if (!fs.exists(file)) {
      throw new ArgumentParserException("File not found: " + file, parser);
    }
  }    
  
  private void verifyNotExists(ArgumentParser parser, Path file) throws ArgumentParserException, IOException {
    if (fs.exists(file)) {
      throw new ArgumentParserException("File found: " + file, parser);
    }
  }    
  
  private void verifyIsFile(ArgumentParser parser, Path file) throws ArgumentParserException, IOException {
    if (!fs.isFile(file)) {
      throw new ArgumentParserException("Not a file: " + file, parser);
    }
  }    
  
  private void verifyIsDirectory(ArgumentParser parser, Path file) throws ArgumentParserException, IOException {
    if (!fs.isDirectory(file)) {
      throw new ArgumentParserException("Not a directory: " + file, parser);
    }
  }    
  
  private void verifyCanRead(ArgumentParser parser, Path file) throws ArgumentParserException, IOException {
    verifyExists(parser, file);
    if (!fs.getFileStatus(file).getPermission().getUserAction().implies(FsAction.READ)) {
      throw new ArgumentParserException("Insufficient permissions to read file: " + file, parser);
    }
  }    
  
  private void verifyCanWrite(ArgumentParser parser, Path file) throws ArgumentParserException, IOException {
    verifyExists(parser, file);
    if (!fs.getFileStatus(file).getPermission().getUserAction().implies(FsAction.WRITE)) {
      throw new ArgumentParserException("Insufficient permissions to write file: " + file, parser);
    }
  }    
  
  private void verifyCanWriteParent(ArgumentParser parser, Path file) throws ArgumentParserException, IOException {
    Path parent = file.getParent();
    if (parent == null || !fs.exists(parent) || !fs.getFileStatus(parent).getPermission().getUserAction().implies(FsAction.WRITE)) {
      throw new ArgumentParserException("Cannot write parent of file: " + file, parser);
    }
  }    
  
  private void verifyCanExecute(ArgumentParser parser, Path file) throws ArgumentParserException, IOException {
    verifyExists(parser, file);
    if (!fs.getFileStatus(file).getPermission().getUserAction().implies(FsAction.EXECUTE)) {
      throw new ArgumentParserException("Insufficient permissions to execute file: " + file, parser);
    }
  }    
  
  private void verifyIsAbsolute(ArgumentParser parser, Path file) throws ArgumentParserException {
    if (!file.isAbsolute()) {
      throw new ArgumentParserException("Not an absolute file: " + file, parser);
    }
  }    
  
  private void verifyHasScheme(ArgumentParser parser, Path file) throws ArgumentParserException {
    if (file.toUri().getScheme() == null) {
      throw new ArgumentParserException("URI scheme is missing in path: " + file, parser);
    }
  }

  private void verifyScheme(ArgumentParser parser, Path file) throws ArgumentParserException {
    if (!verifyScheme.equals(file.toUri().getScheme())) {
      throw new ArgumentParserException("Scheme of path: " + file + " must be: " + verifyScheme, parser);
    }
  }

  private boolean isSystemIn(Path file) {
    return acceptSystemIn && file.toString().equals("-");
  }
  
}
