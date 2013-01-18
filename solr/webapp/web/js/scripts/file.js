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

// #/:core/schema, #/:core/config
sammy.get
(
  new RegExp( app.core_regex_base + '\\/(schema|config)$' ),
  function( context )
  {
    var core_basepath = this.active_core.attr( 'data-basepath' );
	var filetype = context.params.splat[1]; // either schema or config	
	var filename = this.active_core.attr( filetype );

    $.ajax
    (
      {
        url : core_basepath + "/admin/file?file=" + filename + "&contentType=text/xml;charset=utf-8",
        dataType : 'xml',
        context : $( '#content' ),
        beforeSend : function( xhr, settings )
        {
          this
          .html( '<div class="loader">Loading ...</div>' );
        },
        complete : function( xhr, text_status )
        {
          var code = $(
            '<pre class="syntax language-xml"><code>' +
            xhr.responseText.esc() +
            '</code></pre>'
          );
          this.html( code );

          if( 'success' === text_status )
          {
            hljs.highlightBlock( code.get(0) );
          }
        }
      }
    );
  }
);