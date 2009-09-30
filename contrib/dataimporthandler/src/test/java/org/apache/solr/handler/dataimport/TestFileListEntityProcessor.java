package org.apache.solr.handler.dataimport;
/**
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

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;

/**
 * <p>
 * Test for FileListEntityProcessor
 * </p>
 *
 * @version $Id$
 * @since solr 1.3
 */
public class TestFileListEntityProcessor {

  @Test
  @SuppressWarnings("unchecked")
  public void testSimple() throws IOException {
    long time = System.currentTimeMillis();
    File tmpdir = new File("." + time);
    tmpdir.mkdir();
    tmpdir.deleteOnExit();
    createFile(tmpdir, "a.xml", "a.xml".getBytes(), false);
    createFile(tmpdir, "b.xml", "b.xml".getBytes(), false);
    createFile(tmpdir, "c.props", "c.props".getBytes(), false);
    Map attrs = AbstractDataImportHandlerTest.createMap(
            FileListEntityProcessor.FILE_NAME, "xml$",
            FileListEntityProcessor.BASE_DIR, tmpdir.getAbsolutePath());
    Context c = AbstractDataImportHandlerTest.getContext(null,
            new VariableResolverImpl(), null, Context.FULL_DUMP, Collections.EMPTY_LIST, attrs);
    FileListEntityProcessor fileListEntityProcessor = new FileListEntityProcessor();
    fileListEntityProcessor.init(c);
    List<String> fList = new ArrayList<String>();
    while (true) {
      Map<String, Object> f = fileListEntityProcessor.nextRow();
      if (f == null)
        break;
      fList.add((String) f.get(FileListEntityProcessor.ABSOLUTE_FILE));
    }
    Assert.assertEquals(2, fList.size());
  }
  
  @Test
  public void testBiggerSmallerFiles() throws IOException {
    long time = System.currentTimeMillis();
    File tmpdir = new File("." + time);
    tmpdir.mkdir();
    tmpdir.deleteOnExit();
    long minLength = Long.MAX_VALUE;
    String smallestFile = "";
    byte[] content = "abcdefgij".getBytes("UTF-8");
    createFile(tmpdir, "a.xml", content, false);
    if (minLength > content.length) {
      minLength = content.length;
      smallestFile = "a.xml";
    }
    content = "abcdefgij".getBytes("UTF-8");
    createFile(tmpdir, "b.xml", content, false);
    if (minLength > content.length) {
      minLength = content.length;
      smallestFile = "b.xml";
    }
    content = "abc".getBytes("UTF-8");
    createFile(tmpdir, "c.props", content, false);
    if (minLength > content.length) {
      minLength = content.length;
      smallestFile = "c.props";
    }
    Map attrs = AbstractDataImportHandlerTest.createMap(
            FileListEntityProcessor.FILE_NAME, ".*",
            FileListEntityProcessor.BASE_DIR, tmpdir.getAbsolutePath(),
            FileListEntityProcessor.BIGGER_THAN, String.valueOf(minLength));
    List<String> fList = getFiles(null, attrs);
    Assert.assertEquals(2, fList.size());
    Set<String> l = new HashSet<String>();
    l.add(new File(tmpdir, "a.xml").getAbsolutePath());
    l.add(new File(tmpdir, "b.xml").getAbsolutePath());
    Assert.assertEquals(l, new HashSet<String>(fList));
    attrs = AbstractDataImportHandlerTest.createMap(
            FileListEntityProcessor.FILE_NAME, ".*",
            FileListEntityProcessor.BASE_DIR, tmpdir.getAbsolutePath(),
            FileListEntityProcessor.SMALLER_THAN, String.valueOf(minLength+1));
    fList = getFiles(null, attrs);
    l.clear();
    l.add(new File(tmpdir, smallestFile).getAbsolutePath());
    Assert.assertEquals(l, new HashSet<String>(fList));
    attrs = AbstractDataImportHandlerTest.createMap(
            FileListEntityProcessor.FILE_NAME, ".*",
            FileListEntityProcessor.BASE_DIR, tmpdir.getAbsolutePath(),
            FileListEntityProcessor.SMALLER_THAN, "${a.x}");
    VariableResolverImpl resolver = new VariableResolverImpl();
    resolver.addNamespace("a", AbstractDataImportHandlerTest.createMap("x", "4"));
    fList = getFiles(resolver, attrs);
    Assert.assertEquals(l, new HashSet<String>(fList));
  }

  @SuppressWarnings("unchecked")
  static List<String> getFiles(VariableResolverImpl resolver, Map attrs) {
    Context c = AbstractDataImportHandlerTest.getContext(null,
            resolver, null, Context.FULL_DUMP, Collections.EMPTY_LIST, attrs);
    FileListEntityProcessor fileListEntityProcessor = new FileListEntityProcessor();
    fileListEntityProcessor.init(c);
    List<String> fList = new ArrayList<String>();
    while (true) {
      Map<String, Object> f = fileListEntityProcessor.nextRow();
      if (f == null)
        break;
      fList.add((String) f.get(FileListEntityProcessor.ABSOLUTE_FILE));
    }
    return fList;
  }

  @Test
  public void testNTOT() throws IOException {
    long time = System.currentTimeMillis();
    File tmpdir = new File("." + time);
    tmpdir.mkdir();
    tmpdir.deleteOnExit();
    createFile(tmpdir, "a.xml", "a.xml".getBytes(), true);
    createFile(tmpdir, "b.xml", "b.xml".getBytes(), true);
    createFile(tmpdir, "c.props", "c.props".getBytes(), true);
    Map attrs = AbstractDataImportHandlerTest.createMap(
            FileListEntityProcessor.FILE_NAME, "xml$",
            FileListEntityProcessor.BASE_DIR, tmpdir.getAbsolutePath(),
            FileListEntityProcessor.OLDER_THAN, "'NOW'");
    List<String> fList = getFiles(null, attrs);
    Assert.assertEquals(2, fList.size());
    attrs = AbstractDataImportHandlerTest.createMap(
            FileListEntityProcessor.FILE_NAME, ".xml$",
            FileListEntityProcessor.BASE_DIR, tmpdir.getAbsolutePath(),
            FileListEntityProcessor.NEWER_THAN, "'NOW-2HOURS'");
    fList = getFiles(null, attrs);
    Assert.assertEquals(2, fList.size());

    // Use a variable for newerThan
    attrs = AbstractDataImportHandlerTest.createMap(
            FileListEntityProcessor.FILE_NAME, ".xml$",
            FileListEntityProcessor.BASE_DIR, tmpdir.getAbsolutePath(),
            FileListEntityProcessor.NEWER_THAN, "${a.x}");
    VariableResolverImpl resolver = new VariableResolverImpl();
    String lastMod = DataImporter.DATE_TIME_FORMAT.get().format(new Date(System.currentTimeMillis() - 50000));
    resolver.addNamespace("a", AbstractDataImportHandlerTest.createMap("x", lastMod));
    createFile(tmpdir, "t.xml", "t.xml".getBytes(), false);
    fList = getFiles(resolver, attrs);
    Assert.assertEquals(1, fList.size());
    Assert.assertEquals("File name must be t.xml", new File(tmpdir, "t.xml").getAbsolutePath(), fList.get(0));
  }

  @Test
  public void testRECURSION() throws IOException {
    long time = System.currentTimeMillis();
    File tmpdir = new File("." + time);
    tmpdir.mkdir();
    tmpdir.deleteOnExit();
    File childdir = new File(tmpdir + "/child" );
    childdir.mkdirs();
    childdir.deleteOnExit();
    createFile(childdir, "a.xml", "a.xml".getBytes(), true);
    createFile(childdir, "b.xml", "b.xml".getBytes(), true);
    createFile(childdir, "c.props", "c.props".getBytes(), true);
    Map attrs = AbstractDataImportHandlerTest.createMap(
            FileListEntityProcessor.FILE_NAME, "^.*\\.xml$",
            FileListEntityProcessor.BASE_DIR, childdir.getAbsolutePath(),
            FileListEntityProcessor.RECURSIVE, "true");
    List<String> fList = getFiles(null, attrs);
    Assert.assertEquals(2, fList.size());
  }

  public static File createFile(File tmpdir, String name, byte[] content,
                                boolean changeModifiedTime) throws IOException {
    File file = new File(tmpdir.getAbsolutePath() + File.separator + name);
    file.deleteOnExit();
    FileOutputStream f = new FileOutputStream(file);
    f.write(content);
    f.close();
    if (changeModifiedTime)
      file.setLastModified(System.currentTimeMillis() - 3600000);
    return file;
  }
}
