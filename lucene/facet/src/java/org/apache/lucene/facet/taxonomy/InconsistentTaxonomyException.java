package org.apache.lucene.facet.taxonomy;

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

/**
 * Exception indicating that a certain operation could not be performed 
 * on a taxonomy related object because of an inconsistency.
 * <p>
 * For example, trying to refresh a taxonomy reader might fail in case 
 * the underlying taxonomy was meanwhile modified in a manner which 
 * does not allow to perform such a refresh. (See {@link TaxonomyReader#refresh()}.)
 *   
 * @lucene.experimental
 */
public class InconsistentTaxonomyException extends Exception {
  
  public InconsistentTaxonomyException(String message) {
    super(message);
  }
  
  public InconsistentTaxonomyException() {
    super();
  }
  
}
