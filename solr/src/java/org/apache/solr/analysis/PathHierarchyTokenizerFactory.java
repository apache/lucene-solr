package org.apache.solr.analysis;
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

import java.io.Reader;
import java.util.Map;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.path.PathHierarchyTokenizer;


/**
 * @version $Id$
 */
public class PathHierarchyTokenizerFactory extends BaseTokenizerFactory {
  
  private char delimiter;
  private char replacement;
  
  /**
   * Require a configured pattern
   */
  @Override
  public void init(Map<String,String> args){
    super.init( args );
    
    String v = args.get( "delimiter" );
    if( v != null ){
      if( v.length() != 1 ){
        throw new IllegalArgumentException( "delimiter should be a char. \"" + v + "\" is invalid" );
      }
      else{
        delimiter = v.charAt(0);
      }
    }
    else{
      delimiter = PathHierarchyTokenizer.DEFAULT_DELIMITER;
    }
    
    v = args.get( "replace" );
    if( v != null ){
      if( v.length() != 1 ){
        throw new IllegalArgumentException( "replace should be a char. \"" + v + "\" is invalid" );
      }
      else{
        replacement = v.charAt(0);
      }
    }
    else{
      replacement = delimiter;
    }
  }

  public Tokenizer create(Reader input) {
    return new PathHierarchyTokenizer(input, delimiter, replacement);
  }
}


