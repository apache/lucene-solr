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

package org.apache.solr.analysis;

import org.apache.lucene.analysis.TokenStream;
import java.util.Map;

/**
 * A <code>TokenFilterFactory</code> creates a 
 * <code>TokenFilter</code> to transform one <code>TokenStream</code> 
 * into another.
 *
 * <p>
 * TokenFilterFactories are registered for <code>FieldType</code>s with the
 * <code>IndexSchema</code> through the <code>schema.xml</code> file.
 * </p>
 * <p>
 * Example <code>schema.xml</code> entry to register a TokenFilterFactory 
 * implementation to transform tokens in a field of type "cool"
 * </p>
 * <pre>
 *  &lt;fieldtype name="cool" class="solr.TextField"&gt;
 *      &lt;analyzer&gt;
 *      ...
 *      &lt;filter class="foo.MyTokenFilterFactory"/&gt;
 *      ...
 * </pre>
 * <p>
 * A single instance of any registered TokenFilterFactory is created
 * via the default constructor and is reused for each FieldType.
 * </p>
 *
 */

public interface TokenFilterFactory {
  /** <code>init</code> will be called just once, immediately after creation.
   * <p>The args are user-level initialization parameters that
   * may be specified when declaring the factory in the
   * schema.xml
   */
  public void init(Map<String,String> args);
  
  /**
   * Accessor method for reporting the args used to initialize this factory.
   * <p>
   * Implementations are <strong>strongly</strong> encouraged to return 
   * the contents of the Map passed to to the init method
   * </p>
   */
  public Map<String,String> getArgs();
  
  /** Transform the specified input TokenStream */
  public TokenStream create(TokenStream input);
}

