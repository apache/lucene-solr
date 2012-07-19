function processAdd(cmd) {
    functionMessages.add("processAdd0");
    testCase.assertNotNull(req);
    testCase.assertNotNull(rsp);
    testCase.assertNotNull(logger);
    testCase.assertNotNull(cmd);
    testCase.assertNotNull(params);
    testCase.assertTrue(1 == params.get('intValue').intValue());  // had issues with assertTrue(1, params.get('intValue').intValue()) casting to wrong variant
    testCase.assertTrue(params.get('boolValue').booleanValue());

    // Integer.valueOf is needed here to get a tru java object, because 
    // all javascript numbers are floating point (ie: java.lang.Double)
    cmd.getSolrInputDocument().addField("script_added_i", 
                                        java.lang.Integer.valueOf(42));
    cmd.getSolrInputDocument().addField("script_added_d", 42.3);
    
}

function processDelete(cmd) {
    functionMessages.add("processDelete0");
    testCase.assertNotNull(req);
    testCase.assertNotNull(rsp);
    testCase.assertNotNull(logger);
    testCase.assertNotNull(cmd);
}

function processMergeIndexes(cmd) {
    functionMessages.add("processMergeIndexes0");
    testCase.assertNotNull(req);
    testCase.assertNotNull(rsp);
    testCase.assertNotNull(logger);
    testCase.assertNotNull(cmd);
}

function processCommit(cmd) {
    functionMessages.add("processCommit0");
    testCase.assertNotNull(req);
    testCase.assertNotNull(rsp);
    testCase.assertNotNull(logger);
    testCase.assertNotNull(cmd);
}

function processRollback(cmd) {
    functionMessages.add("processRollback0");
    testCase.assertNotNull(req);
    testCase.assertNotNull(rsp);
    testCase.assertNotNull(logger);
    testCase.assertNotNull(cmd);
}

function finish() {
    functionMessages.add("finish0");
    testCase.assertNotNull(req);
    testCase.assertNotNull(rsp);
    testCase.assertNotNull(logger);
}

