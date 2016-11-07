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

  doc = cmd.solrDoc;  // org.apache.solr.common.SolrInputDocument
  var id = doc.getFieldValue("id");
  logger.info("update-script#processAdd: id=" + id);

  // The idea here is to use the file's content_type value to
  // simplify into user-friendly values, such that types of, say, image/jpeg and image/tiff
  // are in an "Images" facet

  var ct = doc.getFieldValue("content_type");
  if (ct) {
    // strip off semicolon onward
    var semicolon_index = ct.indexOf(';');
    if (semicolon_index != -1) {
      ct = ct.substring(0,semicolon_index);
    }
    // and split type/subtype
    var ct_type = ct.substring(0,ct.indexOf('/'));
    var ct_subtype = ct.substring(ct.indexOf('/')+1);

    var doc_type;
    switch(true) {
      case /^application\/rtf/.test(ct) || /wordprocessing/.test(ct):
        doc_type = "doc";
        break;

      case /html/.test(ct):
        doc_type = "html";
        break;

      case /^image\/.*/.test(ct):
        doc_type = "image";
        break;

      case /presentation|powerpoint/.test(ct):
        doc_type = "presentation";
        break;

      case /spreadsheet|excel/.test(ct):
        doc_type = "spreadsheet";
        break;

      case /^application\/pdf/.test(ct):
        doc_type = "pdf";
        break;

      case /^text\/plain/.test(ct):
        doc_type = "text"
        break;

      default:
        break;
    }

    // TODO: error handling needed?   What if there is no slash?
    if(doc_type) { doc.setField("doc_type", doc_type); }
    doc.setField("content_type_type_s", ct_type);
    doc.setField("content_type_subtype_s", ct_subtype);
  }

  var content = doc.getFieldValue("content");
  if (!content) {
    return; //No content found, so we are done here
  }

    var analyzer =
         req.getCore().getLatestSchema()
         .getFieldTypeByName("text_email_url")
         .getIndexAnalyzer();

  var token_stream =
       analyzer.tokenStream("content", content);
  var term_att = token_stream.getAttribute(get_class("org.apache.lucene.analysis.tokenattributes.CharTermAttribute"));
  var type_att = token_stream.getAttribute(get_class("org.apache.lucene.analysis.tokenattributes.TypeAttribute"));
  token_stream.reset();
  while (token_stream.incrementToken()) {
    doc.addField(type_att.type().replace(/\<|\>/g,'').toLowerCase()+"_ss", term_att.toString());
  }
  token_stream.end();
  token_stream.close();
}

function processDelete(cmd) {
  // no-op
}

function processMergeIndexes(cmd) {
  // no-op
}

function processCommit(cmd) {
  // no-op
}

function processRollback(cmd) {
  // no-op
}

function finish() {
  // no-op
}
