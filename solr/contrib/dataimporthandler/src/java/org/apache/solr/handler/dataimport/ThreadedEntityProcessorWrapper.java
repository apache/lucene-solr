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

import static org.apache.solr.handler.dataimport.EntityProcessorBase.ON_ERROR;
import static org.apache.solr.handler.dataimport.EntityProcessorBase.ABORT;
import static org.apache.solr.handler.dataimport.DataImportHandlerException.wrapAndThrow;
import static org.apache.solr.handler.dataimport.DataImportHandlerException.SEVERE;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Each Entity may have only a single EntityProcessor .  But the same entity can be run by
 * multiple EntityProcessorWrapper (1 per thread) . this helps running transformations in multiple threads
 * @since Solr 3.1
 */

public class ThreadedEntityProcessorWrapper extends EntityProcessorWrapper {
  private static final Logger LOG = LoggerFactory.getLogger(ThreadedEntityProcessorWrapper.class);

  final DocBuilder.EntityRunner entityRunner;
  /** single EntityRunner per children entity */
  final Map<DataConfig.Entity ,DocBuilder.EntityRunner> children;
  
  //final protected AtomicBoolean entityEnded = new AtomicBoolean(false);

   final private int number;

  public ThreadedEntityProcessorWrapper(EntityProcessor delegate, DocBuilder docBuilder,
                                  DocBuilder.EntityRunner entityRunner,
                                  VariableResolverImpl resolver,
                                  Map<DataConfig.Entity ,DocBuilder.EntityRunner> childrenRunners,
                                  int num) {
    super(delegate, docBuilder);
    this.entityRunner = entityRunner;
    this.resolver = resolver;
    this.children = childrenRunners;
    this.number = num;
  }

  void threadedInit(Context context){
    rowcache = null;
    this.context = context;
    resolver = (VariableResolverImpl) context.getVariableResolver();
    //context has to be set correctly . keep the copy of the old one so that it can be restored in destroy
    if (entityName == null) {
      onError = resolver.replaceTokens(context.getEntityAttribute(ON_ERROR));
      if (onError == null) onError = ABORT;
      entityName = context.getEntityAttribute(DataConfig.NAME);
    }    
  }

  public void init(DocBuilder.EntityRow rows) {
    for (DocBuilder.EntityRow row = rows; row != null; row = row.tail) resolver.addNamespace(row.name, row.row);
  }

  public int getNumber() {
    return number;
  }


 
}
