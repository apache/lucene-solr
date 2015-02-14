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
package org.apache.solr.client.solrj.beans;

import static java.lang.annotation.ElementType.*;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static org.apache.solr.client.solrj.beans.DocumentObjectBinder.DEFAULT;

import java.lang.annotation.Target;
import java.lang.annotation.Retention;


/**
 * This class can be used to annotate a field or a setter an any class
 * and SlrJ would help you convert to SolrInputDocument and from SolrDocument
 *
 * @since solr 1.3
 */
@Target({FIELD, METHOD})
@Retention(RUNTIME)
public @interface Field {
  boolean child() default false;
  String value() default DEFAULT;
}
