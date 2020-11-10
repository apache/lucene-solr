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
package org.apache.solr.handler.extraction;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map.Entry;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.lucene.util.IOUtils;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaMetadataKeys;
import org.apache.tika.parser.PasswordProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Password provider for Extracting request handler which finds correct
 * password based on file name matching against a list of regular expressions. 
 * The list of passwords is supplied in an optional Map.
 * If an explicit password is set, it will be used.
 */
public class RegexRulesPasswordProvider implements PasswordProvider {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  private LinkedHashMap<Pattern,String> passwordMap = new LinkedHashMap<>();
  private String explicitPassword; 
  
  @Override
  public String getPassword(Metadata meta) {
    if(getExplicitPassword() != null) {
      return getExplicitPassword();
    }
    
    if(passwordMap.size() > 0)
      return lookupPasswordFromMap(meta.get(TikaMetadataKeys.RESOURCE_NAME_KEY));
    
    return null;
  }

  private String lookupPasswordFromMap(String fileName) {
    if(fileName != null && fileName.length() > 0) {
      for(Entry<Pattern,String> e : passwordMap.entrySet()) {
        if(e.getKey().matcher(fileName).matches()) {
          return e.getValue();
        }
      }
    }
    return null;
  }
  
  /**
   * Parses rule file from stream and returns a Map of all rules found
   * @param is input stream for the file
   */
  public static LinkedHashMap<Pattern,String> parseRulesFile(InputStream is) {
    LinkedHashMap<Pattern,String> rules = new LinkedHashMap<>();
    BufferedReader br = new BufferedReader(IOUtils.getDecodingReader(is, StandardCharsets.UTF_8));
    String line;
    try {
      int linenum = 0;
      while ((line = br.readLine()) != null)   {
        linenum++;
        // Remove comments
        String[] arr = line.split("#");
        if(arr.length > 0)
          line = arr[0].trim();
        if(line.length() == 0) 
          continue;
        int sep = line.indexOf("=");
        if(sep <= 0) {
          log.warn("Wrong format of password line {}", linenum);
          continue;
        }
        String pass = line.substring(sep+1).trim();
        String regex = line.substring(0, sep).trim();
        try {
          Pattern pattern = Pattern.compile(regex);
          rules.put(pattern,  pass);
        } catch(PatternSyntaxException pse) {
          log.warn("Key of line {} was not a valid regex pattern{}", linenum, pse);
          continue;
        }
      }
      is.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return rules;
  }

  /**
   * Initialize rules through file input stream. This is a convenience for first calling
   * setPasswordMap(parseRulesFile(is)).
   * @param is the input stream with rules file, one line per rule on format regex=password
   */
  public void parse(InputStream is) {
    setPasswordMap(parseRulesFile(is));
  }
  
  public LinkedHashMap<Pattern,String> getPasswordMap() {
    return passwordMap;
  }

  public void setPasswordMap(LinkedHashMap<Pattern,String> linkedHashMap) {
    this.passwordMap = linkedHashMap;
  }

  /**
   * Gets the explicit password, if set
   * @return the password, or null if not set
   */
  public String getExplicitPassword() {
    return explicitPassword;
  }

  /**
   * Sets an explicit password which will be used instead of password map
   * @param explicitPassword the password to use
   */
  public void setExplicitPassword(String explicitPassword) {
    this.explicitPassword = explicitPassword;
  }
  
  /**
   * Resets explicit password, so that map will be used for lookups
   */
  public void resetExplicitPassword() {
    this.explicitPassword = null;
  }

}
