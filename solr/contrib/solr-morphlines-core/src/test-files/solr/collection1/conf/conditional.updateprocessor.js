function processAdd(cmd) {
  if (req.getParams().getBool("go-for-it",false)) {
    cmd.getSolrInputDocument().addField("script_added_s", "i went for it");
    return true;
  }
  return false;
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
