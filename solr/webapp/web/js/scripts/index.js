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

var parse_memory_value = function( value )
{
  if( value !== Number( value ) )
  {
    var units = 'BKMGTPEZY';
    var match = value.match( /^(\d+([,\.]\d+)?) (\w)\w?$/ );
    var value = parseFloat( match[1] ) * Math.pow( 1024, units.indexOf( match[3].toUpperCase() ) );
  }
    
  return value;
};

var generate_bar = function( bar_container, bar_data, convert_label_values )
{
  bar_holder = $( '.bar-holder', bar_container );

  var bar_level = 1;
  var max_width = Math.round( $( '.bar-max', bar_holder ).width() );
  $( '.bar-max.val', bar_holder ).text( bar_data['max'] );
    
  bar_level++;
  $( '.bar-total.bar', bar_holder ).width( new String( (bar_data['total']/bar_data['max'])*100 ) + '%' );
  $( '.bar-total.val', bar_holder ).text( bar_data['total'] );

  if( bar_data['used'] )
  {
    bar_level++;
    $( '.bar-used.bar', bar_holder ).width( new String( (bar_data['used']/bar_data['total'])*100 ) + '%' );
    $( '.bar-used.val', bar_holder ).text( bar_data['used'] );
  }

  bar_holder
    .addClass( 'bar-lvl-' + bar_level );

  var percentage = ( ( ( bar_data['used'] || bar_data['total'] ) / bar_data['max'] ) * 100 ).toFixed(1);
        
  var hl = $( '[data-desc="' + bar_container.attr( 'id' ) + '"]' );

  $( '.bar-desc', hl )
    .remove();

  hl
    .append( ' <small class="bar-desc">' + percentage + '%</small>' );

  if( !!convert_label_values )
  {
    $( '.val', bar_holder )
      .each
      (
        function()
        {
          var self = $( this );

          var unit = null;
          var byte_value = parseInt( self.html() );

          self
            .attr( 'title', 'raw: ' + byte_value + ' B' );

          byte_value /= 1024;
          byte_value /= 1024;
          unit = 'MB';

          if( 1024 <= byte_value )
          {
            byte_value /= 1024;
            unit = 'GB';
          }

          byte_value = byte_value.toFixed( 2 ) + ' ' + unit;

          self
            .text( byte_value );
        }
      );
  }
};

var system_info = function( element, system_data )
{
  // -- usage

  var load_average = ( system_data['system']['uptime'] || '' ).match( /load averages?: (\d+[.,]\d\d),? (\d+[.,]\d\d),? (\d+[.,]\d\d)/ );
  if( load_average )
  {
    var hl = $( '#system h2', element );

    $( '.bar-desc', hl )
      .remove();

    hl
      .append( ' <small class="bar-desc">' + load_average.slice( 1 ).join( '  ' ).replace( /,/g, '.' ).esc() + '</small>' );
  }

  // -- physical-memory-bar
    
  var bar_holder = $( '#physical-memory-bar', element );
  if( system_data['system']['totalPhysicalMemorySize'] === undefined || system_data['system']['freePhysicalMemorySize'] === undefined )
  {
    bar_holder.hide();
  }
  else
  {
    bar_holder.show();

    var bar_data = {
      'max' : parse_memory_value( system_data['system']['totalPhysicalMemorySize'] ),
      'total' : parse_memory_value( system_data['system']['totalPhysicalMemorySize'] - system_data['system']['freePhysicalMemorySize'] )
    };

    generate_bar( bar_holder, bar_data, true );
  }

  // -- swap-space-bar
    
  var bar_holder = $( '#swap-space-bar', element );
  if( system_data['system']['totalSwapSpaceSize'] === undefined || system_data['system']['freeSwapSpaceSize'] === undefined )
  {
    bar_holder.hide();
  }
  else
  {
    bar_holder.show();

    var bar_data = {
      'max' : parse_memory_value( system_data['system']['totalSwapSpaceSize'] ),
      'total' : parse_memory_value( system_data['system']['totalSwapSpaceSize'] - system_data['system']['freeSwapSpaceSize'] )
    };

    generate_bar( bar_holder, bar_data, true );
  }

  // -- file-descriptor-bar
    
  var bar_holder = $( '#file-descriptor-bar', element );
  if( system_data['system']['maxFileDescriptorCount'] === undefined || system_data['system']['openFileDescriptorCount'] === undefined )
  {
    bar_holder.hide();
  }
  else
  {
    bar_holder.show();

    var bar_data = {
      'max' : parse_memory_value( system_data['system']['maxFileDescriptorCount'] ),
      'total' : parse_memory_value( system_data['system']['openFileDescriptorCount'] )
    };

    generate_bar( bar_holder, bar_data );
  }

  0 === $( '#system div[id$="-bar"]:visible', element ).size()
    ? $( '#system .no-info', element ).show()
    : $( '#system .no-info', element ).hide();

  // -- memory-bar

  var bar_holder = $( '#jvm-memory-bar', element );
  if( system_data['jvm']['memory'] === undefined )
  {
    bar_holder.hide();
  }
  else
  {
    bar_holder.show();

    var jvm_memory = $.extend
    (
      {
        'free' : null,
        'total' : null,
        'max' : null,
        'used' : null,
        'raw' : {
          'free' : null,
          'total' : null,
          'max' : null,
          'used' : null,
          'used%' : null
        }
      },
      system_data['jvm']['memory']
    );

    var bar_data = {
      'max' : parse_memory_value( jvm_memory['raw']['max'] || jvm_memory['max'] ),
      'total' : parse_memory_value( jvm_memory['raw']['total'] || jvm_memory['total'] ),
      'used' : parse_memory_value( jvm_memory['raw']['used'] || jvm_memory['used'] )
    };

    generate_bar( bar_holder, bar_data, true );
  }

}

// #/
sammy.get
(
  /^#\/$/,
  function( context )
  {
    var content_element = $( '#content' );

    content_element
      .html( '<div id="index"></div>' );

    $.ajax
    (
      {
        url : 'tpl/index.html',
        context : $( '#index', content_element ),
        beforeSend : function( arr, form, options )
        {
        },
        success : function( template )
        {
          var self = this;

          this
            .html( template );
    
          var data = {
            'start_time' : app.dashboard_values['jvm']['jmx']['startTime'],
            'jvm_version' : app.dashboard_values['jvm']['name'] + ' (' + app.dashboard_values['jvm']['version'] + ')',
            'processors' : app.dashboard_values['jvm']['processors'],
            'solr_spec_version' : app.dashboard_values['lucene']['solr-spec-version'] || '-',
            'solr_impl_version' : app.dashboard_values['lucene']['solr-impl-version'] || '-',
            'lucene_spec_version' : app.dashboard_values['lucene']['lucene-spec-version'] || '-',
            'lucene_impl_version' : app.dashboard_values['lucene']['lucene-impl-version'] || '-'
          };
    
          for( var key in data )
          {                                                        
            var value_element = $( '.' + key + ' dd', this );

            value_element
              .text( data[key].esc() );
                        
            value_element.closest( 'li' )
              .show();
          }

          var commandLineArgs = app.dashboard_values['jvm']['jmx']['commandLineArgs'].sort().reverse();
          if( 0 !== commandLineArgs.length )
          {
            var cmd_arg_element = $( '.command_line_args dt', this );
            var cmd_arg_key_element = $( '.command_line_args dt', this );
            var cmd_arg_element = $( '.command_line_args dd', this );

            for( var key in commandLineArgs )
            {
              cmd_arg_element = cmd_arg_element.clone();
              cmd_arg_element.text( commandLineArgs[key] );

              cmd_arg_key_element
                .after( cmd_arg_element );
            }

            cmd_arg_key_element.closest( 'li' )
              .show();

            $( '.command_line_args dd:last', this )
              .remove();

            $( '.command_line_args dd:odd', this )
              .addClass( 'odd' );
          }

          $( '.timeago', this )
            .timeago();

          $( '.index-left .block li:visible:odd', this )
            .addClass( 'odd' );
                    
          // -- system_info

          system_info( this, app.dashboard_values );

          $( '#system a.reload', this )
            .die( 'click' )
            .live
            (
              'click',
              function( event )
              {
                $.ajax
                (
                  {
                    url : config.solr_path + '/admin/info/system?wt=json',
                    dataType : 'json',
                    context : this,
                    beforeSend : function( arr, form, options )
                    {
                      loader.show( this );
                    },
                    success : function( response )
                    {
                      system_info( self, response );
                    },
                    error : function()
                    {
                    },
                    complete : function()
                    {
                      loader.hide( this );
                    }
                  }
                );

                return false;
              }
            );
        },
        error : function( xhr, text_status, error_thrown )
        {
        },
        complete : function( xhr, text_status )
        {
        }
      }
    );
  }
);
