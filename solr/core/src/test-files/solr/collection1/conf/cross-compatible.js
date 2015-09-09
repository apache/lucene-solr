function get_class(name) {
   var clazz;
   try {
     // Java8 Nashorn
     clazz = eval("Java.type(name).class");
   } catch(e) {
     // Java7 Rhino
     clazz = eval("Packages."+name);
   }

   return clazz;
}

function processAdd(cmd) {
  var doc = cmd.getSolrInputDocument();

  var analyzer =
       req.getCore().getLatestSchema()
       .getFieldTypeByName("text")
       .getIndexAnalyzer();

  var token_stream =
       analyzer.tokenStream("subject", doc.getFieldValue("subject"));

  var cta_class = get_class("org.apache.lucene.analysis.tokenattributes.CharTermAttribute");
  var term_att = token_stream.getAttribute(cta_class);
  token_stream.reset();
  while (token_stream.incrementToken()) {
    doc.addField("term_s", term_att.toString());
  }
  token_stream.end();
  token_stream.close();

  return true;
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
