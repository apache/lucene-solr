function processAdd(cmd) {
    // Integer.valueOf is needed here to get a tru java object, because 
    // all javascript numbers are floating point (ie: java.lang.Double)
    cmd.getSolrInputDocument().addField("script_added_i", 
                                        java.lang.Integer.valueOf(42));
    cmd.getSolrInputDocument().addField("script_added_d", 42.3);
    
}

// // // 

function processDelete() {
    // NOOP
}
function processCommit() { 
    // NOOP
}
function processRollback() {
    // NOOP
}
function processMergeIndexes() {
    // NOOP
}
function finish() { 
    // NOOP
}
