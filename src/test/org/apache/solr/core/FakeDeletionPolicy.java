package org.apache.solr.core;

import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.util.plugin.NamedListInitializedPlugin;

import java.io.IOException;
import java.util.List;

/**
 * @version $Id$
 */
public class FakeDeletionPolicy implements IndexDeletionPolicy, NamedListInitializedPlugin {

  private String var1;
  private String var2;

  //@Override
  public void init(NamedList args) {
    var1 = (String) args.get("var1");
    var2 = (String) args.get("var2");
  }

  public String getVar1() {
    return var1;
  }

  public String getVar2() {
    return var2;
  }

  //  @Override
  public void onCommit(List arg0) throws IOException {
    System.setProperty("onCommit", "test.org.apache.solr.core.FakeDeletionPolicy.onCommit");
  }

  //  @Override
  public void onInit(List arg0) throws IOException {
    System.setProperty("onInit", "test.org.apache.solr.core.FakeDeletionPolicy.onInit");
  }
}
