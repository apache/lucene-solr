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
package org.apache.lucene.queryparser.flexible.precedence.processors;

import org.apache.lucene.queryparser.flexible.precedence.PrecedenceQueryParser;
import org.apache.lucene.queryparser.flexible.standard.processors.BooleanQuery2ModifierNodeProcessor;
import org.apache.lucene.queryparser.flexible.standard.processors.StandardQueryNodeProcessorPipeline;
import org.apache.lucene.queryparser.flexible.core.config.QueryConfigHandler;

/**
 * <p>
 * This processor pipeline extends {@link StandardQueryNodeProcessorPipeline} and enables
 * boolean precedence on it.
 * </p>
 * <p>
 * EXPERT: the precedence is enabled by removing {@link BooleanQuery2ModifierNodeProcessor} from the
 * {@link StandardQueryNodeProcessorPipeline} and appending {@link BooleanModifiersQueryNodeProcessor}
 * to the pipeline.
 * </p>
 * 
 * @see PrecedenceQueryParser
 *  @see StandardQueryNodeProcessorPipeline
 */
public class PrecedenceQueryNodeProcessorPipeline extends StandardQueryNodeProcessorPipeline {

  /**
   * @see StandardQueryNodeProcessorPipeline#StandardQueryNodeProcessorPipeline(QueryConfigHandler)
   */
  public PrecedenceQueryNodeProcessorPipeline(QueryConfigHandler queryConfig) {
    super(queryConfig);
    
    for (int i = 0 ; i < size() ; i++) {
      
      if (get(i).getClass().equals(BooleanQuery2ModifierNodeProcessor.class)) {
        remove(i--);
      }
      
    }
    
    add(new BooleanModifiersQueryNodeProcessor());
    
  }

}
