package org.apache.lucene.queryParser.standard.config;

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

import org.apache.lucene.queryParser.core.config.QueryConfigHandler;
import org.apache.lucene.queryParser.standard.processors.StandardQueryNodeProcessorPipeline;

/**
 * This query configuration handler is used for almost every processor defined
 * in the {@link StandardQueryNodeProcessorPipeline} processor pipeline. It holds
 * attributes that reproduces the configuration that could be set on the old
 * lucene 2.4 QueryParser class. <br/>
 * 
 * @see StandardQueryNodeProcessorPipeline
 */
public class StandardQueryConfigHandler extends QueryConfigHandler {



  public StandardQueryConfigHandler() {
    // Add listener that will build the FieldConfig attributes.
    addFieldConfigListener(new FieldBoostMapFCListener(this));
    addFieldConfigListener(new FieldDateResolutionFCListener(this));

    // Default Values
    addAttribute(DefaultOperatorAttribute.class);
    addAttribute(AnalyzerAttribute.class);
    addAttribute(FuzzyAttribute.class);
    addAttribute(LowercaseExpandedTermsAttribute.class);
    addAttribute(MultiTermRewriteMethodAttribute.class);
    addAttribute(AllowLeadingWildcardAttribute.class);
    addAttribute(PositionIncrementsAttribute.class);
    addAttribute(LocaleAttribute.class);
    addAttribute(DefaultPhraseSlopAttribute.class);
    addAttribute(MultiTermRewriteMethodAttribute.class);   
    
  }

}
