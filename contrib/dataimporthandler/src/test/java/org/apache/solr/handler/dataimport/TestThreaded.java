package org.apache.solr.handler.dataimport;

import org.junit.Test;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;


public class TestThreaded extends AbstractDataImportHandlerTest {
  @Test
  @SuppressWarnings("unchecked")
  public void testCompositePk_FullImport() throws Exception {
    List parentRow = new ArrayList();
//    parentRow.add(createMap("id", "1"));
    parentRow.add(createMap("id", "2"));
    parentRow.add(createMap("id", "3"));
    parentRow.add(createMap("id", "4"));
    parentRow.add(createMap("id", "1"));
    MockDataSource.setIterator("select * from x", parentRow.iterator());

    List childRow = new ArrayList();
    Map map = createMap("desc", "hello");
    childRow.add(map);

    MockDataSource.setIterator("select * from y where y.A=1", childRow.iterator());
    MockDataSource.setIterator("select * from y where y.A=2", childRow.iterator());
    MockDataSource.setIterator("select * from y where y.A=3", childRow.iterator());
    MockDataSource.setIterator("select * from y where y.A=4", childRow.iterator());

    super.runFullImport(dataConfig);

    assertQ(req("id:1"), "//*[@numFound='1']");
    assertQ(req("*:*"), "//*[@numFound='4']");
    assertQ(req("desc:hello"), "//*[@numFound='4']");
  }

    @Override
  public String getSchemaFile() {
    return "dataimport-schema.xml";
  }

  @Override
  public String getSolrConfigFile() {
    return "dataimport-solrconfig.xml";
  }
   private static String dataConfig = "<dataConfig>\n"
          +"<dataSource  type=\"MockDataSource\"/>\n"
          + "       <document>\n"
          + "               <entity name=\"x\" threads=\"2\" query=\"select * from x\" deletedPkQuery=\"select id from x where last_modified > NOW AND deleted='true'\" deltaQuery=\"select id from x where last_modified > NOW\">\n"
          + "                       <field column=\"id\" />\n"
          + "                       <entity name=\"y\" query=\"select * from y where y.A=${x.id}\">\n"
          + "                               <field column=\"desc\" />\n"
          + "                       </entity>\n" + "               </entity>\n"
          + "       </document>\n" + "</dataConfig>";
}
