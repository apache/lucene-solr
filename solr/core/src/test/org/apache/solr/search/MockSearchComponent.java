package org.apache.solr.search;

import java.io.IOException;

import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;

public class MockSearchComponent extends SearchComponent {
  
  private String testParam = null;
  
  @Override
  public void init(NamedList args) {
    super.init(args);
    testParam = (String) args.get("testParam");
  }

  @Override
  public void prepare(ResponseBuilder rb) throws IOException {
    
  }

  @Override
  public void process(ResponseBuilder rb) throws IOException {
    rb.rsp.add(this.getName(), this.testParam);
  }

  @Override
  public String getDescription() {
    return "Mock search component for tests";
  }

  @Override
  public String getSource() {
    return "";
  }
  
}
