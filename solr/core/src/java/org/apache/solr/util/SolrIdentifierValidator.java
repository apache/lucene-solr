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
package org.apache.solr.util;

import java.lang.invoke.MethodHandles;
import java.util.regex.Pattern;

import org.apache.solr.common.SolrException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ensures that provided identifiers align with Solr's recommendations/requirements for choosing
 * collection, core, etc identifiers.
 *  
 * Identifiers are allowed to contain underscores, periods, and alphanumeric characters. 
 */
public class SolrIdentifierValidator {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  final static Pattern identifierPattern = Pattern.compile("^[\\._A-Za-z0-9]*$");
  
  public static void validateCollectionName(String collectionName) throws SolrException {
    validateCoreName(collectionName);
  }
  
  public static void validateCoreName(String name) throws SolrException {
    if (name == null || !identifierPattern.matcher(name).matches()) {
      log.info("Validation failed on the invalid identifier [{}].  Throwing SolrException to indicate a BAD REQUEST.", name);
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "Invalid name: '" + name + "' Identifiers must consist entirely of periods, underscores and alphanumerics");
    }
  }
}
