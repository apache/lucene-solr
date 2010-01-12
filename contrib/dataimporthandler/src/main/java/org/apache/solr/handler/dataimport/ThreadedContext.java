package org.apache.solr.handler.dataimport;



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
