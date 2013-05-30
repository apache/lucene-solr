var Assert = Packages.org.junit.Assert;

function processAdd(cmd) {
    functionMessages.add("processAdd0");
    Assert.assertNotNull(req);
    Assert.assertNotNull(rsp);
    Assert.assertNotNull(logger);
    Assert.assertNotNull(cmd);
    Assert.assertNotNull(params);
    Assert.assertTrue(1 == params.get('intValue').intValue());  // had issues with assertTrue(1, params.get('intValue').intValue()) casting to wrong variant
    Assert.assertTrue(params.get('boolValue').booleanValue());

    // Integer.valueOf is needed here to get a tru java object, because 
    // all javascript numbers are floating point (ie: java.lang.Double)
    cmd.getSolrInputDocument().addField("script_added_i", 
                                        java.lang.Integer.valueOf(42));
    cmd.getSolrInputDocument().addField("script_added_d", 42.3);
    
}

function processDelete(cmd) {
    functionMessages.add("processDelete0");
    Assert.assertNotNull(req);
    Assert.assertNotNull(rsp);
    Assert.assertNotNull(logger);
    Assert.assertNotNull(cmd);
}

function processMergeIndexes(cmd) {
    functionMessages.add("processMergeIndexes0");
    Assert.assertNotNull(req);
    Assert.assertNotNull(rsp);
    Assert.assertNotNull(logger);
    Assert.assertNotNull(cmd);
}

function processCommit(cmd) {
    functionMessages.add("processCommit0");
    Assert.assertNotNull(req);
    Assert.assertNotNull(rsp);
    Assert.assertNotNull(logger);
    Assert.assertNotNull(cmd);
}

function processRollback(cmd) {
    functionMessages.add("processRollback0");
    Assert.assertNotNull(req);
    Assert.assertNotNull(rsp);
    Assert.assertNotNull(logger);
    Assert.assertNotNull(cmd);
}

function finish() {
    functionMessages.add("finish0");
    Assert.assertNotNull(req);
    Assert.assertNotNull(rsp);
    Assert.assertNotNull(logger);
}

