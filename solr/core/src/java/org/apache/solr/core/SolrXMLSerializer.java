package org.apache.solr.core;

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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.XML;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolrXMLSerializer {
  protected static Logger log = LoggerFactory
      .getLogger(SolrXMLSerializer.class);
  
  private final static String INDENT = "  ";
  
  
  /**
   * @param w
   *          Writer to use
   * @param defaultCoreName
   *          to use for cores with name ""
   * @param coreDescriptors
   *          to persist
   * @param rootSolrAttribs
   *          solrxml solr attribs
   * @param containerProperties
   *          to persist
   * @param coresAttribs
   *          solrxml cores attribs
   * @throws IOException
   */
  void persist(Writer w, SolrXMLDef solrXMLDef) throws IOException {
    w.write("<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n");
    w.write("<solr");
    Map<String,String> rootSolrAttribs = solrXMLDef.solrAttribs;
    Set<String> solrAttribKeys = rootSolrAttribs.keySet();
    for (String key : solrAttribKeys) {
      String value = rootSolrAttribs.get(key);
      writeAttribute(w, key, value);
    }
    
    w.write(">\n");
    Properties containerProperties = solrXMLDef.containerProperties;
    if (containerProperties != null && !containerProperties.isEmpty()) {
      writeProperties(w, containerProperties, "  ");
    }
    w.write(INDENT + "<cores");
    Map<String,String> coresAttribs = solrXMLDef.coresAttribs;
    Set<String> coreAttribKeys = coresAttribs.keySet();
    for (String key : coreAttribKeys) {
      String value = coresAttribs.get(key);
      writeAttribute(w, key, value);
    }
    w.write(">\n");
    
    for (SolrCoreXMLDef coreDef : solrXMLDef.coresDefs) {
      persist(w, coreDef);
    }

    w.write(INDENT + "</cores>\n");
    w.write("</solr>\n");
  }
  
  /** Writes the cores configuration node for a given core. */
  private void persist(Writer w, SolrCoreXMLDef coreDef) throws IOException {
    w.write(INDENT + INDENT + "<core");
    Set<String> keys = coreDef.coreAttribs.keySet();
    for (String key : keys) {
      writeAttribute(w, key, coreDef.coreAttribs.get(key));
    }
    Properties properties = coreDef.coreProperties;
    if (properties == null || properties.isEmpty()) w.write("/>\n"); // core
    else {
      w.write(">\n");
      writeProperties(w, properties, "      ");
      w.write(INDENT + INDENT + "</core>\n");
    }
  }
  
  private void writeProperties(Writer w, Properties props, String indent)
      throws IOException {
    for (Map.Entry<Object,Object> entry : props.entrySet()) {
      w.write(indent + "<property");
      writeAttribute(w, "name", entry.getKey());
      writeAttribute(w, "value", entry.getValue());
      w.write("/>\n");
    }
  }
  
  private void writeAttribute(Writer w, String name, Object value)
      throws IOException {
    if (value == null) return;
    w.write(" ");
    w.write(name);
    w.write("=\"");
    XML.escapeAttributeValue(value.toString(), w);
    w.write("\"");
  }
  
  void persistFile(File file, SolrXMLDef solrXMLDef) {
    log.info("Persisting cores config to " + file);
    
    File tmpFile = null;
    try {
      // write in temp first
      tmpFile = File.createTempFile("solr", ".xml", file.getParentFile());
      
      java.io.FileOutputStream out = new java.io.FileOutputStream(tmpFile);
      Writer writer = new BufferedWriter(new OutputStreamWriter(out, "UTF-8"));
      try {
        persist(writer, solrXMLDef);
      } finally {
        writer.close();
        out.close();
      }
      // rename over origin or copy if this fails
      if (tmpFile != null) {
        if (tmpFile.renameTo(file)) tmpFile = null;
        else fileCopy(tmpFile, file);
      }
    } catch (java.io.FileNotFoundException xnf) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, xnf);
    } catch (java.io.IOException xio) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, xio);
    } finally {
      if (tmpFile != null) {
        if (!tmpFile.delete()) tmpFile.deleteOnExit();
      }
    }
  }
  
  /**
   * Copies a src file to a dest file: used to circumvent the platform
   * discrepancies regarding renaming files.
   */
  private static void fileCopy(File src, File dest) throws IOException {
    IOException xforward = null;
    FileInputStream fis = null;
    FileOutputStream fos = null;
    FileChannel fcin = null;
    FileChannel fcout = null;
    try {
      fis = new FileInputStream(src);
      fos = new FileOutputStream(dest);
      fcin = fis.getChannel();
      fcout = fos.getChannel();
      // do the file copy 32Mb at a time
      final int MB32 = 32 * 1024 * 1024;
      long size = fcin.size();
      long position = 0;
      while (position < size) {
        position += fcin.transferTo(position, MB32, fcout);
      }
    } catch (IOException xio) {
      xforward = xio;
    } finally {
      if (fis != null) try {
        fis.close();
        fis = null;
      } catch (IOException xio) {}
      if (fos != null) try {
        fos.close();
        fos = null;
      } catch (IOException xio) {}
      if (fcin != null && fcin.isOpen()) try {
        fcin.close();
        fcin = null;
      } catch (IOException xio) {}
      if (fcout != null && fcout.isOpen()) try {
        fcout.close();
        fcout = null;
      } catch (IOException xio) {}
    }
    if (xforward != null) {
      throw xforward;
    }
  }
  
  static public class SolrXMLDef {
    Properties containerProperties;
    Map<String,String> solrAttribs;
    Map<String,String> coresAttribs;
    List<SolrCoreXMLDef> coresDefs;
  }
  
  static public class SolrCoreXMLDef {
    Properties coreProperties;
    Map<String,String> coreAttribs;
  }
}