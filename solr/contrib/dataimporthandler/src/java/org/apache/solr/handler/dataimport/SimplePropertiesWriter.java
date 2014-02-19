package org.apache.solr.handler.dataimport;

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

import static org.apache.solr.handler.dataimport.DataImportHandlerException.SEVERE;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

import org.apache.lucene.util.IOUtils;
import org.apache.solr.core.SolrCore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * <p>
 *  Writes properties using {@link Properties#store} .
 *  The special property "last_index_time" is converted to a formatted date.
 *  Users can configure the location, filename, locale and date format to use.
 * </p> 
 */
public class SimplePropertiesWriter extends DIHProperties {
  private static final Logger log = LoggerFactory
      .getLogger(SimplePropertiesWriter.class);
  
  static final String LAST_INDEX_KEY = "last_index_time";
  
  protected String filename = null;
  
  protected String configDir = null;
  
  protected Locale locale = null;
  
  protected SimpleDateFormat dateFormat = null;
  
  /**
   * The locale to use when writing the properties file.  Default is {@link Locale#ROOT}
   */
  public static final String LOCALE = "locale";
  /**
   * The date format to use when writing values for "last_index_time" to the properties file.
   * See {@link SimpleDateFormat} for patterns.  Default is yyyy-MM-dd HH:mm:ss .
   */
  public static final String DATE_FORMAT = "dateFormat";
  /**
   * The directory to save the properties file in. Default is the current core's "config" directory.
   */
  public static final String DIRECTORY = "directory";
  /**
   * The filename to save the properties file to.  Default is this Handler's name from solrconfig.xml.
   */
  public static final String FILENAME = "filename";
  
  @Override
  public void init(DataImporter dataImporter, Map<String, String> params) {
    if(params.get(FILENAME) != null) {
      filename = params.get(FILENAME);
    } else if(dataImporter.getHandlerName()!=null) {
      filename = dataImporter.getHandlerName() +  ".properties";
    } else {
      filename = "dataimport.properties";
    }
    findDirectory(dataImporter, params);
    if(params.get(LOCALE) != null) {
      String localeStr = params.get(LOCALE);
      for (Locale l : Locale.getAvailableLocales()) {
        if(localeStr.equals(l.getDisplayName())) {
          locale = l;
          break;
        }
      }
      if(locale==null) {
        throw new DataImportHandlerException(SEVERE, "Unsupported locale for PropertWriter: " + localeStr);
      }
    } else {
      locale = Locale.ROOT;
    }    
    if(params.get(DATE_FORMAT) != null) {
      dateFormat = new SimpleDateFormat(params.get(DATE_FORMAT), locale);
    } else {
      dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", locale);
    }    
  }  
  protected void findDirectory(DataImporter dataImporter, Map<String, String> params) {
    if(params.get(DIRECTORY) != null) {
      configDir = params.get(DIRECTORY);
    } else {
      SolrCore core = dataImporter.getCore();
      configDir = (core == null ? "." : core.getResourceLoader().getConfigDir());
    }
  }
  
  private File getPersistFile() {
    String filePath = configDir;
    if (configDir != null && !configDir.endsWith(File.separator)) filePath += File.separator;
    filePath += filename;
    return new File(filePath);
  }
  @Override
  public boolean isWritable() {
    File persistFile = getPersistFile();
    return persistFile.exists() ? persistFile.canWrite() : persistFile
        .getParentFile().canWrite();
    
  }
  
  @Override
  public String convertDateToString(Date d) {
    return dateFormat.format(d);
  }
  protected Date convertStringToDate(String s) {
    try {
      return dateFormat.parse(s);
    } catch (ParseException e) {
      throw new DataImportHandlerException(SEVERE, "Value for "
          + LAST_INDEX_KEY + " is invalid for date format "
          + dateFormat.toLocalizedPattern() + " : " + s);
    }
  }
  /**
   * {@link DocBuilder} sends the date as an Object because 
   * this class knows how to convert it to a String
   */
  protected Properties mapToProperties(Map<String,Object> propObjs) {
    Properties p = new Properties();
    for(Map.Entry<String,Object> entry : propObjs.entrySet()) {
      String key = entry.getKey();
      String val = null;
      String lastKeyPart = key;
      int lastDotPos = key.lastIndexOf('.');
      if(lastDotPos!=-1 && key.length() > lastDotPos+1) {
        lastKeyPart = key.substring(lastDotPos + 1);
      }
      if(LAST_INDEX_KEY.equals(lastKeyPart) && entry.getValue() instanceof Date) {
        val = convertDateToString((Date) entry.getValue());
      } else {
        val = entry.getValue().toString();
      }
      p.put(key, val);
    }
    return p;
  }
  /**
   * We'll send everything back as Strings as this class has
   * already converted them.
   */
  protected Map<String,Object> propertiesToMap(Properties p) {
    Map<String,Object> theMap = new HashMap<String,Object>();
    for(Map.Entry<Object,Object> entry : p.entrySet()) {
      String key = entry.getKey().toString();
      Object val = entry.getValue().toString();
      theMap.put(key, val);
    }
    return theMap;
  }
  
  @Override
  public void persist(Map<String, Object> propObjs) {
    Writer propOutput = null;    
    Properties existingProps = mapToProperties(readIndexerProperties());    
    Properties newProps = mapToProperties(propObjs);
    try {
      existingProps.putAll(newProps);
      String filePath = configDir;
      if (configDir != null && !configDir.endsWith(File.separator)) {
        filePath += File.separator;
      }
      filePath += filename;
      propOutput = new OutputStreamWriter(new FileOutputStream(filePath), IOUtils.CHARSET_UTF_8);
      existingProps.store(propOutput, null);
      log.info("Wrote last indexed time to " + filename);
    } catch (Exception e) {
      throw new DataImportHandlerException(DataImportHandlerException.SEVERE,
          "Unable to persist Index Start Time", e);
    } finally {
      IOUtils.closeWhileHandlingException(propOutput);
    }
  }
  
  @Override
  public Map<String, Object> readIndexerProperties() {
    Properties props = new Properties();
    InputStream propInput = null;    
    try {
      String filePath = configDir;
      if (configDir != null && !configDir.endsWith(File.separator)) {
        filePath += File.separator;
      }
      filePath += filename;
      propInput = new FileInputStream(filePath);
      props.load(new InputStreamReader(propInput, IOUtils.CHARSET_UTF_8));
      log.info("Read " + filename);
    } catch (Exception e) {
      log.warn("Unable to read: " + filename);
    } finally {
      IOUtils.closeWhileHandlingException(propInput);
    }    
    return propertiesToMap(props);
  }
  
}
