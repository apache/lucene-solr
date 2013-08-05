package org.apache.solr.rest;


import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Provider;
import com.google.inject.multibindings.Multibinder;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryRequestDecoder;

/**
 *
 *
 **/
public abstract class APIModule extends AbstractModule {

  @Inject
  protected Injector injector;



  /**
   * Classes who extend and who's API implementation does not conform to the convention
   * of being of the same name as the APIModule implementing class or is not in the same
   * package as the APIModule implementing should override this class.  For instance
   * if your module is FooAPIModule, then by convention this method will look for FooAPI as the API class.
   *
   * @return The {@link org.apache.solr.rest.API} class tied to this Module
   */
  public Class<? extends API> getAPIClass() {
    String className = this.getClass().getName();
    className = className.replace("Module", "");
    try {
      return Class.forName(className).asSubclass(API.class);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  protected abstract void defineBindings();



  @Override
  protected void configure() {
    //TODO

    Multibinder< API > apiBinder = Multibinder.newSetBinder(binder(), API.class);
    apiBinder.addBinding().to(getAPIClass());

    defineBindings();
  }

  /**
   * This method is called when all modules are already configured, Injector has
   * been created and injected locally, but before the APIServer starts.
   */
  public void initInjectorDependent() {

  }
}

