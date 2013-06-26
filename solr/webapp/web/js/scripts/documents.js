/*
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */
//helper for formatting JSON and others
var content_generator = {

  _default: function (toEsc) {
    return toEsc.esc();
  },

  json: function (toEsc) {
    return app.format_json(toEsc);
  }

};

//Utiltity function for turning on/off various elements
function toggles(documents_form, show_json, show_file, show_doc, doc_text, show_wizard) {
  var json_only = $('#json-only');
  var the_document = $('#document', documents_form);
  if (show_doc) {
    //console.log("doc: " + doc_text);
    the_document.val(doc_text);
    the_document.show();
  } else {
    the_document.hide();
  }
  if (show_json) {
    json_only.show();
  } else {
    json_only.hide();
  }
  var file_upload = $('#file-upload', documents_form);
  var upload_only = $('#upload-only', documents_form);
  if (show_file) {
    file_upload.show();
    upload_only.show();
  } else {
    file_upload.hide();
    upload_only.hide();
  }
  var wizard = $('#wizard', documents_form);
  if (show_wizard) {
    wizard.show();
  } else {
    wizard.hide();
  }
}
// #/:core/documents

//Utiltity function for setting up the wizard fields
function addWizardFields(active_core, wizard) {
  var core_basepath = active_core.attr('data-basepath');
  var select_options = "";
  //Populate the select options based off the Fields REST API
  $.getJSON(window.location.protocol + '//' + window.location.host
          + core_basepath + "/schema/fields").done(
      //TODO: handle dynamic fields
      //TODO: get the unique key, too
      function (data) {
        var field_select = $("#wiz-field-select", wizard);
        field_select.empty();
        $.each(data.fields,
            function (i, item) {
              //console.log("i[" + i + "]=" + item.name);
              if (item.name != "_version_"){
                select_options += '<option name="' + item.name + '">'
                  + item.name + '</option>';
              }
            });
        //console.log("select_options: " + select_options);
        //fill in the select options
        field_select.append(select_options);
      });
  var wizard_doc = $("#wizard-doc", wizard);
  wizard_doc.die('focusin')
      .live('focusin', function (event) {
        $("#wizard-doc", wizard).text("");
      }
  );
  //Add the click handler for the "Add Field" target, which
  //takes the field content and moves it into the document target
  var add_field = $("#add-field-href", wizard);
  add_field.die("click")
      .live("click",
      function (event) {
        //take the field and the contents and append it to the document
        var wiz_select = $("#wiz-field-select", wizard);
        var selected = $("option:selected", wiz_select);
        console.log("selected field: " + selected);
        var wiz_doc = $("#wizard-doc", wizard);
        var the_document = $("#document");
        var current_doc = the_document.val();
        console.log("current_text: " + current_doc + " wiz_doc: " + wiz_doc.val());
        var index = current_doc.lastIndexOf("}");
        var new_entry = '"' + selected.val() + '":"' + wiz_doc.val() + '"';
        if (index >= 0) {
          current_doc = current_doc.substring(0, index) + ', ' + new_entry + "}";
        } else {
          //we don't have a doc at all
          current_doc = "{" + new_entry + "}";
        }
        current_doc = content_generator['json'](current_doc);
        the_document.val(current_doc);
        //clear the wiz doc window
        wiz_doc.val("");
        return false;
      }
  );

  //console.log("adding " + i + " child: " + child);

}

//The main program for adding the docs
sammy.get
(
    new RegExp(app.core_regex_base + '\\/(documents)$'),
    function (context) {
      var active_core = this.active_core;
      var core_basepath = active_core.attr('data-basepath');
      var content_element = $('#content');


      $.post
      (
          'tpl/documents.html',
          function (template) {

            content_element
                .html(template);
            var documents_element = $('#documents', content_element);
            var documents_form = $('#form form', documents_element);
            var url_element = $('#url', documents_element);
            var result_element = $('#result', documents_element);
            var response_element = $('#response', documents_element);
            var doc_type_select = $('#document-type', documents_form);
            //Since we are showing "example" docs, when the area receives the focus
            // remove the example content.
            $('#document', documents_form).die('focusin')
                .live('focusin',
                function (event) {
                  var document_type = $('#document-type', documents_form).val();
                  if (document_type != "wizard"){
                    //Don't clear the document when in wizard mode.
                    var the_document = $('#document', documents_form);
                    the_document.text("");
                  }
                }
            );

            /*response_element.html("");*/
            //Setup the handlers for toggling the various display options for the "Document Type" select
            doc_type_select
                .die('change')
                .live
            (
                'change',
                function (event) {
                  var document_type = $('#document-type', documents_form).val();
                  var file_upload = $('#file-upload', documents_form);

                  //need to clear out any old file upload by forcing a redraw so that
                  //we don't try to upload an old file
                  file_upload.html(file_upload.html());
                  if (document_type == "json") {
                    toggles(documents_form, true, false, true, '{"id":"change.me","title":"change.me"}', false);
                    $("#attribs").show();
                  } else if (document_type == "upload") {
                    toggles(documents_form, false, true, false, "", false);
                    $("#attribs").show();
                  } else if (document_type == "csv") {
                    toggles(documents_form, false, false, true, "id,title\nchange.me,change.me", false);
                    $("#attribs").show();
                  } else if (document_type == "solr") {
                    toggles(documents_form, false, false, true, '<add>\n' +
                        '<doc>\n' +
                        '<field name="id">change.me</field>\n' +
                        '<field name="title" >chang.me</field>\n' +
                        '</doc>\n' +
                        '</add>\n', false);
                    $("#attribs").hide();
                  } else if (document_type == "wizard") {
                    var wizard = $('#wizard', documents_form);
                    addWizardFields(active_core, wizard);
                    //$("#wizard-doc", wizard).text('Enter your field text here and then click "Add Field" to add the field to the document.');
                    toggles(documents_form, false, false, true, "", true);
                    $("#attribs").show();
                  } else if (document_type == "xml") {
                    toggles(documents_form, false, false, true, '<doc>\n' +
                        '<field name="id">change.me</field>' +
                        '<field name="title">change.me</field>' +
                        '</doc>', false);
                    $("#attribs").show();
                  }
                  return false;
                }
            );
            doc_type_select.chosen().trigger('change');
            //Setup the submit option handling.
            documents_form
                .die('submit')
                .live
            (
                'submit',
                function (event) {
                  var form_values = [];
                  var handler_path = $('#qt', documents_form).val();
                  if ('/' !== handler_path[0]) {
                    form_values.push({ name: 'qt', value: handler_path.esc() });
                    handler_path = '/update';
                  }

                  var document_url = window.location.protocol + '//' + window.location.host
                      + core_basepath + handler_path + '?wt=json';

                  url_element
                      .attr('href', document_url)
                      .text(document_url)
                      .trigger('change');
                  var the_document = $('#document', documents_form).val();
                  var commit_within = $('#commitWithin', documents_form).val();
                  var boost = $('#boost', documents_form).val();
                  var overwrite = $('#overwrite', documents_form).val();
                  var the_command = "";
                  var content_type = "";
                  var document_type = $('#document-type', documents_form).val();
                  var doingFileUpload = false;
                  //Both JSON and Wizard use the same pathway for submission
                  //New entries primarily need to fill the_command and set the content_type
                  if (document_type == "json" || document_type == "wizard") {
                    //create a JSON command
                    the_command = "{"
                        + '"add":{ "doc":' + the_document + ","
                        + '"boost":' + boost + ","
                        + '"overwrite":' + overwrite + ","
                        + '"commitWithin":' + commit_within
                        + "}}";
                    content_type = "application/json";
                  } else if (document_type == "csv") {
                    the_command = the_document;
                    document_url += "&commitWithin=" + commit_within + "&overwrite=" + overwrite;
                    content_type = "application/csv";
                  } else if (document_type == "xml") {
                    the_command = '<add commitWithin="' + commit_within
                        + '" overwrite="' + overwrite + '"'
                        + ">"
                        + the_document + "</add>";
                    content_type = "text/xml";
                  } else if (document_type == "upload") {
                    doingFileUpload = true;
                  } else if (document_type == "solr") {
                    //guess content_type
                    the_command = the_document;
                    if (the_document.indexOf("<") >= 0) {
                      //XML
                      content_type = "text/xml";
                    } else if (the_document.indexOf("{") >= 0) {
                      //JSON
                      content_type = "application/json";
                    } //TODO: do we need to handle others?
                  } else {
                    //How to handle other?
                  }

                  //Handle the submission of the form in the case where we are not uploading a file
                  if (doingFileUpload == false) {
                    $.ajax(
                        {
                          url: document_url,
                          //dataType : 'json',
                          processData: false,
                          type: 'POST',
                          contentType: content_type,
                          data: the_command,
                          context: response_element,
                          beforeSend: function (xhr, settings) {
                            console.log("beforeSend: Vals: " + document_url + " content-type: " + document_type + " the cmd: " + the_command);

                          },
                          success: function (response, text_status, xhr) {
                            console.log("success:  " + response + " status: " + text_status + " xhr: " + xhr.responseText);
                            this.html('<div><span class="description">Status</span>: ' + text_status + '</div>'
                                + '<div><span class="description">Response:</span>' + '<pre class="syntax language-json"><code>' + content_generator['json'](xhr.responseText) + "</code></pre></div>");
                            result_element.show();
                          },
                          error: function (xhr, text_status, error_thrown) {
                            console.log("error: " + text_status + " thrown: " + error_thrown);
                            this.html('<div><span class="description">Status</span>: ' + text_status + '</div><div><span class="description">Error:</span> '
                                + '' + error_thrown
                                + '</div>'
                                + '<div><span class="description">Error</span>:' + '<pre class="syntax language-json"><code>'
                                + content_generator['json'](xhr.responseText) +
                                '</code></pre></div>');
                            result_element.show();
                          },
                          complete: function (xhr, text_status) {
                            //console.log("complete: " + text_status + " xhr: " + xhr.responseText + " doc type: " + document_type);

                            //alert(text_status + ": " + xhr.responseText);
                            /*this
                             .removeClass( 'loader' );*/
                          }
                        }
                    );
                  } else {
                    //upload the file
                    var the_file = $('#the-file', documents_form);
                    var erh_params = $('#erh-params', documents_form).val();
                    if (erh_params != "") {
                      if (erh_params.substring(0,1) != "&"){
                        erh_params = "&" + erh_params;
                      }
                      document_url = document_url + erh_params;
                    }
                    console.log("uploading file to: " + document_url);
                    the_file.ajaxfileupload({
                      'action': document_url,
                      'validate_extensions': false,
                      'upload_now': true,
                      'params': {
                        'extra': 'info'
                      },
                      'onComplete': function (response) {
                        response = response.replace('<pre style="word-wrap: break-word; white-space: pre-wrap;">', "");
                        response = response.replace("</pre>", "");
                        console.log('completed upload: ' + response);
                        response_element.html('<div><span class="description">Response:</span>' + '<pre class="syntax language-json"><code>' + content_generator['json'](response) + "</code></pre></div>");
                        result_element.show();

                      },
                      'onStart': function () {
                        console.log("starting file upload");
                        //if (weWantedTo) return false; // cancels upload
                      },
                      'onCancel': function () {
                        console.log('no file selected');
                      }
                    });
                  }
                  return false;
                }
            );
          }
      )
    }
)
/*
 Sample docs:
 <doc boost="2.5">
 <field name="id">05991</field>
 <field name="title" boost="2.0">Bridgewater</field>
 </doc>

 {"id":"foo","title":"blah"}

 */