package org.apache.solr.rest;


import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import org.restlet.Request;
import org.restlet.Response;
import org.restlet.resource.Finder;
import org.restlet.resource.ServerResource;

import java.lang.reflect.Type;

/**
 *
 *
 **/
public class ResourceFinder {

  private volatile Injector injector;

  @Inject
  public ResourceFinder(Injector injector) {
    this.injector = injector;
  }

  public Finder finderOf(Key<? extends ServerResource> key) {
    return new ServerResourceKeyFinder(key);
  }

  public Finder finderOf(Class<? extends ServerResource> cls) {
    return new ServerResourceKeyFinder(Key.get(cls));
  }


  public class KeyFinder extends Finder {
    private final Class<? extends ServerResource> targetClass;

    KeyFinder(Type type) {
      this.targetClass = (Class<? extends ServerResource>) type;
    }

    @Override
    public final Class<? extends ServerResource> getTargetClass() {
      return this.targetClass;
    }
  }

  public class ServerResourceKeyFinder extends KeyFinder {
    private final Key<? extends ServerResource> serverResourceKey;

    ServerResourceKeyFinder(Key<? extends ServerResource> serverResourceKey) {
      super(serverResourceKey.getTypeLiteral().getType());
      this.serverResourceKey = serverResourceKey;
    }

    @Override
    public ServerResource create(Request request, Response response) {
      return injector.getInstance(serverResourceKey);
    }
  }
}