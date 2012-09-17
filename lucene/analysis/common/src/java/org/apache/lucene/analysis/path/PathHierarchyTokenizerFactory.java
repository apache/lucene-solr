package org.apache.lucene.analysis.path;

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

import java.io.Reader;
import java.util.Map;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.path.PathHierarchyTokenizer;
import org.apache.lucene.analysis.path.ReversePathHierarchyTokenizer;
import org.apache.lucene.analysis.util.TokenizerFactory;

/**
 * Factory for {@link PathHierarchyTokenizer}. 
 * <p>
 * This factory is typically configured for use only in the <code>index</code> 
 * Analyzer (or only in the <code>query</code> Analyzer, but never both).
 * </p>
 * <p>
 * For example, in the configuration below a query for 
 * <code>Books/NonFic</code> will match documents indexed with values like 
 * <code>Books/NonFic</code>, <code>Books/NonFic/Law</code>, 
 * <code>Books/NonFic/Science/Physics</code>, etc. But it will not match 
 * documents indexed with values like <code>Books</code>, or 
 * <code>Books/Fic</code>...
 * </p>
 *
 * <pre class="prettyprint" >
 * &lt;fieldType name="descendent_path" class="solr.TextField"&gt;
 *   &lt;analyzer type="index"&gt;
 *     &lt;tokenizer class="solr.PathHierarchyTokenizerFactory" delimiter="/" /&gt;
 *   &lt;/analyzer&gt;
 *   &lt;analyzer type="query"&gt;
 *     &lt;tokenizer class="solr.KeywordTokenizerFactory" /&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;
 * </pre>
 * <p>
 * In this example however we see the oposite configuration, so that a query 
 * for <code>Books/NonFic/Science/Physics</code> would match documents 
 * containing <code>Books/NonFic</code>, <code>Books/NonFic/Science</code>, 
 * or <code>Books/NonFic/Science/Physics</code>, but not 
 * <code>Books/NonFic/Science/Physics/Theory</code> or 
 * <code>Books/NonFic/Law</code>.
 * </p>
 * <pre class="prettyprint" >
 * &lt;fieldType name="descendent_path" class="solr.TextField"&gt;
 *   &lt;analyzer type="index"&gt;
 *     &lt;tokenizer class="solr.KeywordTokenizerFactory" /&gt;
 *   &lt;/analyzer&gt;
 *   &lt;analyzer type="query"&gt;
 *     &lt;tokenizer class="solr.PathHierarchyTokenizerFactory" delimiter="/" /&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;
 * </pre>
 */
public class PathHierarchyTokenizerFactory extends TokenizerFactory {
  
  private char delimiter;
  private char replacement;
  private boolean reverse = false;
  private int skip =  PathHierarchyTokenizer.DEFAULT_SKIP;
  
  /**
   * Require a configured pattern
   */
  @Override
  public void init(Map<String,String> args){
    super.init( args );
    
    String v = args.get( "delimiter" );
    if( v != null ){
      if( v.length() != 1 ){
        throw new IllegalArgumentException("delimiter should be a char. \"" + v + "\" is invalid");
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
        throw new IllegalArgumentException("replace should be a char. \"" + v + "\" is invalid");
      }
      else{
        replacement = v.charAt(0);
      }
    }
    else{
      replacement = delimiter;
    }
    
    v = args.get( "reverse" );
    if( v != null ){
      reverse = "true".equals( v );
    }

    v = args.get( "skip" );
    if( v != null ){
      skip = Integer.parseInt( v );
    }
  }

  public Tokenizer create(Reader input) {
    if( reverse ) {
      return new ReversePathHierarchyTokenizer(input, delimiter, replacement, skip);
    }
    return new PathHierarchyTokenizer(input, delimiter, replacement, skip);
  }
}


