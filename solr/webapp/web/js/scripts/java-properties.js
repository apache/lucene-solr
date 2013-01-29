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

// #/~java-properties
sammy.get
(
  /^#\/(~java-properties)$/,
  function( context )
  {
    var core_basepath = $( '[data-basepath]', app.menu_element ).attr( 'data-basepath' );
    var content_element = $( '#content' );

    content_element
      .html( '<div id="java-properties"></div>' );

    $.ajax
    (
      {
        url : core_basepath + '/admin/properties?wt=json',
        dataType : 'json',
        context : $( '#java-properties', content_element ),
        beforeSend : function( xhr, settings )
        {
          this
            .html( '<div class="loader">Loading ...</div>' );
        },
        success : function( response, text_status, xhr )
        {
          var system_properties = response['system.properties'];
          var properties_data = {};
          var properties_content = [];
          var properties_order = [];

          var workaround = xhr.responseText.match( /"(line\.separator)"\s*:\s*"(.+?)"/ );
          if( workaround && workaround[2] )
          {
            system_properties[workaround[1]] = workaround[2];
          }

          for( var key in system_properties )
          {
            var displayed_key = key.replace( /\./g, '.&#8203;' );
            var displayed_value = [ system_properties[key] ];
            var item_class = 'clearfix';

            if( -1 !== key.indexOf( '.path' ) || -1 !== key.indexOf( '.dirs' ) )
            {
              displayed_value = system_properties[key].split( system_properties['path.separator'] );
              if( 1 < displayed_value.length )
              {
                item_class += ' multi';
              }
            }

            var item_content = '<li><dl class="' + item_class + '">' + "\n"
                             + '<dt>' + displayed_key.esc() + '</dt>' + "\n";

            for( var i in displayed_value )
            {
              item_content += '<dd>' + displayed_value[i].esc() + '</dd>' + "\n";
            }

            item_content += '</dl></li>';

            properties_data[key] = item_content;
            properties_order.push( key );
          }

          properties_order.sort();
          for( var i in properties_order )
          {
            properties_content.push( properties_data[properties_order[i]] );
          }

          this
            .html( '<ul>' + properties_content.join( "\n" ) + '</ul>' );
                    
          $( 'li:odd', this )
            .addClass( 'odd' );
                    
          $( '.multi dd:odd', this )
            .addClass( 'odd' );
        },
        error : function( xhr, text_status, error_thrown)
        {
        },
        complete : function( xhr, text_status )
        {
        }
      }
    );
  }
);