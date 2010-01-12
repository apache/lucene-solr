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


/**Context implementation used when run multi threaded.
 *
 * @since Solr 1.5
 * 
 */
public class ThreadedContext extends ContextImpl{
  private DocBuilder.EntityRunner entityRunner;
  private boolean limitedContext = false;

  public ThreadedContext(DocBuilder.EntityRunner entityRunner, DocBuilder docBuilder) {
    super(entityRunner.entity,
            null,//to be fethed realtime
            null,
            null,
            docBuilder.session,
            null,
            docBuilder);
    this.entityRunner = entityRunner;
  }

  @Override
  public VariableResolver getVariableResolver() {
    checkLimited();
    return entityRunner.currentEntityProcWrapper.get().resolver;
  }

  @Override
  public Context getParentContext() {
    ThreadedContext ctx = new ThreadedContext(entityRunner.parent, docBuilder);
    ctx.limitedContext =  true;
    return ctx;
  }

  @Override
  public String currentProcess() {
    return entityRunner.currentProcess;
  }

  @Override
  public EntityProcessor getEntityProcessor() {
    return entityRunner.currentEntityProcWrapper.get().delegate;    
  }

  @Override
  public DataSource getDataSource() {
    checkLimited();
    return super.getDataSource();    
  }



  private void checkLimited() {
    if(limitedContext) throw new RuntimeException("parentContext does not support this method");
  }

  @Override
  public String getResolvedEntityAttribute(String name) {
    checkLimited();
    return super.getResolvedEntityAttribute(name);
  }

  @Override
  public void setSessionAttribute(String name, Object val, String scope) {
    checkLimited();
    super.setSessionAttribute(name, val, scope);
  }

  @Override
  public Object resolve(String var) {
    return getVariableResolver().resolve(var);
  }

  @Override
  public String replaceTokens(String template) {
    return getVariableResolver().replaceTokens(template);    
  }
}
