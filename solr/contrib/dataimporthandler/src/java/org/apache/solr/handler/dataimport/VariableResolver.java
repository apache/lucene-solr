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
package org.apache.solr.handler.dataimport;

/**
 * <p>
 * This class is more or less like a Map. But has more intelligence to resolve
 * namespaces. Namespaces are delimited with '.' (period)
 * </p>
 * <p/>
 * <p>
 * Refer to <a
 * href="http://wiki.apache.org/solr/DataImportHandler">http://wiki.apache.org/solr/DataImportHandler</a>
 * for more details.
 * </p>
 * <p/>
 * <b>This API is experimental and may change in the future.</b>
 *
 *
 * @since solr 1.3
 */
public abstract class VariableResolver {

  /**
   * Resolves a given value with a name
   *
   * @param name the String to be resolved
   * @return an Object which is the result of evaluation of given name
   */
  public abstract Object resolve(String name);

  /**
   * Given a String with place holders, replace them with the value tokens.
   *
   * @param template
   * @return the string with the placeholders replaced with their values
   */
  public abstract String replaceTokens(String template);
}
