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

var core_basepath = null;
var content_element = null;
var selected_type = null;
var context_path = null;
var active_context = null;
var changes = null;
var reference_xml = null;

var compute_plugin_data = function( response, changeset )
{
  var types = [];
  var sort_table = {};
  var plugin_data = {};

  var types_obj = {};
  var plugin_key = null;

  changes = { count : {}, list : {} }

  for( var i = 0; i < response['solr-mbeans'].length; i++ )
  {
    if( !( i % 2 ) )
    {
      plugin_key = response['solr-mbeans'][i];
    }
    else
    {
      plugin_data[plugin_key] = response['solr-mbeans'][i];
    }
  }

  for( var key in plugin_data )
  {
    sort_table[key] = {
      url : [],
      component : [],
      handler : []
    };
    for( var part_key in plugin_data[key] )
    {
      if( plugin_data[key][part_key]['_changed_'] )
      {
        delete plugin_data[key][part_key]['_changed_'];

        changes.count[key] = changes.count[key] || 0;
        changes.count[key]++;

        changes.list[key] = changes.list[key] || {};
        changes.list[key][part_key] = true;
      }

      if( 0 < part_key.indexOf( '.' ) )
      {
        types_obj[key] = true;
        sort_table[key]['handler'].push( part_key );
      }
      else if( 0 === part_key.indexOf( '/' ) )
      {
        types_obj[key] = true;
        sort_table[key]['url'].push( part_key );
      }
      else
      {
        types_obj[key] = true;
        sort_table[key]['component'].push( part_key );
      }
    }
  }

  for( var type in types_obj )
  {
    types.push( type );
  }
  types.sort();

  return {
    'plugin_data' : plugin_data,
    'sort_table' : sort_table,
    'types' : types
  };
};

var render_plugin_data = function( plugin_data, plugin_sort, types )
{
  var frame_element = $( '#frame', content_element );
  var navigation_element = $( '#navigation ul', content_element );

  var navigation_content = [];
  for( var i = 0; i < types.length; i++ )
  {
    var type_url = active_context.params.splat[0] + '/' + active_context.params.splat[1] + '/' + types[i].toLowerCase();

    var navigation_markup = '<li class="' + types[i].toLowerCase().esc() + '">' +
                            '<a href="#/' + type_url + '" rel="' + types[i].esc() + '">' + types[i].esc();

    if( changes.count[types[i]] )
    {
      navigation_markup += ' <span>' + changes.count[types[i]].esc() + '</span>';
    }

    navigation_markup += '</a>' +
                         '</li>';

    navigation_content.push( navigation_markup );
  }

  navigation_content.push( '<li class="PLUGINCHANGES"><a href="#">Watch Changes</a></li>' );
  navigation_content.push( '<li class="RELOAD"><a href="#" onClick="window.location.reload()">Refresh Values</a></li>' );

  navigation_element
    .html( navigation_content.join( "\n" ) );
    
  $( '.PLUGINCHANGES a', navigation_element )
    .die( 'click' )
    .live
    (
      'click',
      function( event )
      { 
        load_reference_xml();
        
        changes = { count : {}, list : {} }
        $( 'a > span', navigation_element ).remove();
        $( '.entry.changed', frame_element ).removeClass( 'changed' );

        $.blockUI
        (
          {
            message: $('#recording'),
            css: { width: '450px' }
          }
        );

        return false;
      }
    ); 

  $( '#recording button' )
    .die( 'click' )
    .live
    (
      'click',
      function( event )
      { 
        $.ajax
        (
          {
            type: 'POST',
            url: core_basepath + '/admin/mbeans',
            dataType : 'json',
            data: { 
              'stats': 'true',
              'wt': 'json', 
              'diff': 'true',
              'all': 'true',
              'stream.body': reference_xml 
            },
            success : function( response, text_status, xhr )
            {
              load_reference_xml();

              app.plugin_data = compute_plugin_data( response );
              render_plugin_data( app.plugin_data.plugin_data, app.plugin_data.sort_table, app.plugin_data.types );
            }
          }
        );
        $.unblockUI();
        return false;
      }
    ); 
              
  $( 'a[href="' + context_path + '"]', navigation_element )
    .parent().addClass( 'current' );
            
  var content = '<ul>';
  for( var sort_key in plugin_sort[selected_type] )
  {
    plugin_sort[selected_type][sort_key].sort();
    var plugin_type_length = plugin_sort[selected_type][sort_key].length;
                
    for( var i = 0; i < plugin_type_length; i++ )
    {
      var bean = plugin_sort[selected_type][sort_key][i];
      var classes = [ 'entry' ];

      if( changes.list[selected_type] && changes.list[selected_type][bean] )
      {
        classes.push( 'changed' );
      }

      content += '<li class="' + classes.join( ' ' ) + '">' + "\n";
      content += '<a href="' + context_path + '?entry=' + bean.esc() + '" data-bean="' + bean.esc() + '">';
      content += '<span>' + bean.esc() + '</span>';
      content += '</a>' + "\n";
      content += '<ul class="detail">' + "\n";
                    
      var details = plugin_data[selected_type][ plugin_sort[selected_type][sort_key][i] ];
      for( var detail_key in details )
      {
        if( 'stats' !== detail_key )
        {
          var detail_value = details[detail_key];

          if( 'description' === detail_key )
          {
            // Link component list to their MBeans page
            if(detail_value.match(/^Search using components: /)) {
              var idx = detail_value.indexOf(':');
              var url = '#/'+active_context.params.splat[0]+'/plugins/other?entry=';
              var tmp = 'Search using components:<ul>';
              $.each(detail_value.substr(idx+1).split(","), function(index, value) { 
                value = $.trim(value);
                tmp += '<li><a href="'+url+value+'" class="linker">'+value+"</a></li>";
              });
              tmp += "</ul>";
              detail_value = tmp;
            }
          }

          content += '<li><dl class="clearfix">' + "\n";
          content += '<dt>' + detail_key + ':</dt>' + "\n";
          if($.isArray(detail_value)) {
            $.each(detail_value, function(index, value) { 
              content += '<dd>' + value + '</dd>' + "\n";
            });
          }
          else {
            content += '<dd>' + detail_value + '</dd>' + "\n";
          }
          content += '</dl></li>' + "\n";
        }
        else if( 'stats' === detail_key && details[detail_key] )
        {
          content += '<li class="stats clearfix">' + "\n";
          content += '<span>' + detail_key + ':</span>' + "\n";
          content += '<ul>' + "\n";

          for( var stats_key in details[detail_key] )
          {
            var stats_value = new String( details[detail_key][stats_key] );
            stats_value = stats_value.replace( /([\(@])/g, '$1&#8203;' );

            content += '<li><dl class="clearfix">' + "\n";
            content += '<dt>' + stats_key + ':</dt>' + "\n";
            content += '<dd>' + stats_value + '</dd>' + "\n";
            content += '</dl></li>' + "\n";
          }

          content += '</ul></li>' + "\n";
        }
      }
                    
      content += '</ul>' + "\n";
    }
  }
  content += '</ul>' + "\n";

  frame_element
    .html( content );

  
  var path = active_context.path.split( '?entry=' );
  var entries = ( path[1] || '' ).split( ',' );
  
  var entry_count = entries.length;
  for( var i = 0; i < entry_count; i++ )
  {
    $( 'a[data-bean="' + entries[i] + '"]', frame_element )
      .parent().addClass( 'expanded' );
  }

  $( 'a', frame_element )
    .off( 'click' )
    .on
    (
      'click',
      function( event )
      { 
        var self = $( this );
        var bean = self.data( 'bean' );

        var split = '?entry=';
        var path = active_context.path.split( split );
        var entry = ( path[1] || '' );

        var regex = new RegExp( bean.replace( /\//g, '\\/' ) + '(,|$)' );
        var match = regex.test( entry );

        var url = path[0] + split;

        url += match
             ? entry.replace( regex, '' )
             : entry + ',' + bean;

        url = url.replace( /=,/, '=' );
        url = url.replace( /,$/, '' );
        url = url.replace( /\?entry=$/, '' );

        active_context.redirect( url );
        return false;
      }
    );
  
  // Try to make links for anything with http (but leave the rest alone)
  $( '.detail dd' ).each(function(index) {
    var txt = $(this).html();
    if(txt.indexOf("http") >= 0) {
      $(this).linker({
         className : 'linker'
      });
    }
  });
  
  // Add invisible whitespace after each slash
  $( '.detail a.linker' ).each(function(index) {
    $(this).html( $(this).html().replace( /\//g, '/&#8203;' ) );
  });
  
            
  $( '.entry', frame_element )
    .each
    (
      function( i, entry )
      {
        $( '.detail > li', entry ).not( '.stats' ).filter( ':even' )
          .addClass( 'odd' );

        $( '.stats li:odd', entry )
          .addClass( 'odd' );
      }
    );
};

var load_reference_xml = function()
{
  $.ajax
  (
    {
      type: 'GET',
      url: core_basepath + '/admin/mbeans?stats=true&wt=xml',
      dataType : 'text',
      success: function( data )
      {
        reference_xml = data;
      }
    }
  );
}

sammy.bind
(
  'plugins_load',
  function( event, params )
  {
    var callback = function()
    {
      params.callback( app.plugin_data.plugin_data, app.plugin_data.sort_table, app.plugin_data.types );
    }
        
    if( app.plugin_data )
    {
      callback( app.plugin_data );
      return true;
    }

    $.ajax
    (
      {
        url : core_basepath + '/admin/mbeans?stats=true&wt=json',
        dataType : 'json',
        beforeSend : function( xhr, settings )
        {
        },
        success : function( response, text_status, xhr )
        {
          app.plugin_data = compute_plugin_data( response );

          $.get
          (
            'tpl/plugins.html',
            function( template )
            {
              $( '#content' )
                .html( template );
                            
              callback( app.plugin_data );
            }
          );
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

// #/:core/plugins/$type
sammy.get
(
  new RegExp( app.core_regex_base + '\\/(plugins)\\/(\\w+)$' ),
  function( context )
  {
    core_basepath = this.active_core.attr( 'data-basepath' );
    content_element = $( '#content' );
    selected_type = context.params.splat[2].toUpperCase();
    context_path = context.path.split( '?' ).shift();
    active_context = context;
    
    sammy.trigger
    (
      'plugins_load',
      {
        active_core : this.active_core,
        callback : render_plugin_data
      }
    );                
  }
);

// #/:core/plugins
sammy.get
(
  new RegExp( app.core_regex_base + '\\/(plugins)$' ),
  function( context )
  {
    core_basepath = this.active_core.attr( 'data-basepath' );
    delete app.plugin_data;

    sammy.trigger
    (
      'plugins_load',
      {
        active_core : this.active_core,
        callback :  function( plugin_data, plugin_sort, types )
        {
          context.redirect( context.path + '/' + types[0].toLowerCase() );
        }
      }
    );
  }
);
