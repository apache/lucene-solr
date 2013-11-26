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

var loglevel_path = app.config.solr_path + '/admin/info/logging';
var cookie_logging_timezone = 'logging_timezone';
var frame_element = null;

var logging_handler = function( response, text_status, xhr )
{
  var self = this;
  var loggers = response.loggers;

  var levels = '<div class="selector-holder"><div class="selector">' + "\n"
             + '<a class="trigger"><span><em>null</em></span></a>' + "\n"
             + '<ul>' + "\n";

  for( var key in response.levels )
  {
    var level = response.levels[key].esc();
    levels += '<li><a href="#" data-level="' + level + '">' + level + '</a></li>' + "\n";
  }

  levels += '<li class="unset"><a href="#" data-level="unset">UNSET</a></li>' + "\n"
         + '</ul>' + "\n"
         + '<a class="close"><span>[x]</span></a>' + "\n"
         + '</div></div>';

  var logger_tree = function( filter )
  {
    var logger_content = '';
    var filter_regex = new RegExp( '^' + filter + '\\.\\w+$' );

    for( var i in loggers )
    {
      var logger = loggers[i];
      var continue_matcher = false;

      if( !filter )
      {
        continue_matcher = logger.name.indexOf( '.' ) !== -1;
      }
      else
      {
        continue_matcher = !logger.name.match( filter_regex );
      }

      if( continue_matcher )
      {
        continue;
      }

      var logger_class = '';

      if( logger.set )
      {
        logger_class = 'set';
      }
            
      if( !logger.level )
      {
        logger_class = 'null';
      }

      var logger_name = logger.name.split( '.' );
      var display_name = logger_name.pop();

      var leaf_class = 'jstree-leaf';
      if( logger.level )
      {
        leaf_class += ' level-' + logger.level.esc().toLowerCase();
      }

      logger_content += '<li class="' + leaf_class + '" data-logger="' + logger.name.esc() + '">';
      logger_content += '<ins class="trigger jstree-icon">&nbsp;</ins>' + "\n";
      logger_content += '<a href="#" class="trigger '+ logger_class + '"' ;

      if( logger.level )
      {
        logger_content += 'rel="' + logger.level.esc() + '" ';
      }
            
      logger_content += 'title="' + logger.name.esc() + '">' + "\n";

      if( 0 !== logger_name.length )
      {
        logger_content += '<span class="ns">' + logger_name.join( '.' ).esc() + '.</span>';
      }

      logger_content += '<span class="name">' + ( display_name ? display_name.esc() : '<em>empty</em>' ) + '</span>' + "\n";
      logger_content += '</a>';

      logger_content += levels;

      if( !!logger.name )
      {
        var child_logger_content = logger_tree( logger.name );
        if( child_logger_content )
        {
          logger_content += '<ul>';
          logger_content += child_logger_content;
          logger_content += '</ul>';
        }
      }

      logger_content += '</li>';
    }

    return logger_content;
  };

  var logger_content = '<div class="block">' + "\n"
                     + '<h2><span>' + response.watcher.esc() + '</span></h2>' + "\n"
                     + '<ul class="tree jstree">' + logger_tree( null ) + '</ul>' + "\n"
                     + '</div>';

  self
    .html( logger_content );

  self
    .die( 'clear' )
    .live
    (
      'clear',
      function( event )
      {
        $( '.open', this )
          .removeClass( 'open' );
      }
    );

  $( 'li:last-child', this )
    .addClass( 'jstree-last' );

  $( 'li.jstree-leaf > a', this )
    .each
    (
      function( index, element )
      {
        element = $( element );
        var level = element.attr( 'rel' );

        if( level )
        {
          var selector = $( '.selector-holder', element.closest( 'li' ) );

          var trigger = $( 'a.trigger', selector );

          trigger
            .text( level.esc() );

          if( element.hasClass( 'set' ) )
          {
            trigger.first()
              .addClass( 'set' );
          }

          $( 'ul a[data-level="' + level + '"]', selector ).first()
            .addClass( 'level' );
        }
      }
    )

  $( '.trigger', this )
    .die( 'click' )
    .live
    (
      'click',
      function( event )
      {
        self.trigger( 'clear' );

        $( '.selector-holder', $( this ).parents( 'li' ).first() ).first()
          .trigger( 'toggle' );

        return false;
      }
    );

  $( '.selector .close', this )
    .die( 'click' )
    .live
    (
      'click',
      function( event )
      {
        self.trigger( 'clear' );
        return false;
      }
    );
    
  $( '.selector-holder', this )
    .die( 'toggle')
    .live
    (
      'toggle',
      function( event )
      {
        var row = $( this ).closest( 'li' );

        $( 'a:first', row )
          .toggleClass( 'open' );

        $( '.selector-holder:first', row )
          .toggleClass( 'open' );
      }
    );

  $( '.selector ul a', this )
    .die( 'click' )
    .live
    (
      'click',
      function( event )
      {
        var element = $( this );

        $.ajax
        (
          {
            url : loglevel_path,
            dataType : 'json',
            data : {
              'wt' : 'json',
              'set' : $( this ).parents( 'li[data-logger]' ).data( 'logger' ) + ':' + element.data( 'level' )
            },
            type : 'POST',
            context : self,
            beforeSend : function( xhr, settings )
            {
              element
                .addClass( 'loader' );
            },
            success : logging_handler
          }
        );

        return false;
      }
    );

};

var format_time_options = {};

var format_time = function( time )
{
  time = time ? new Date( time ) : new Date();
  return '<time datetime="' + time.toISOString().esc() + '">' + format_time_content( time ) + '</abbr>';
}

var format_time_content = function( time )
{
  return time.toLocaleString( undefined, format_time_options ).esc();
}

var load_logging_viewer = function()
{
  var table = $( 'table', frame_element );
  var state = $( '#state', frame_element );
  var since = table.data( 'latest' ) || 0;
  var sticky_mode = null;

  $.ajax
  (
    {
      url : loglevel_path + '?wt=json&since=' + since,
      dataType : 'json',
      beforeSend : function( xhr, settings )
      {
        // initial request
        if( 0 === since )
        {
          sticky_mode = true;
        }

        // state element is in viewport
        else if( state.position().top <= $( window ).scrollTop() + $( window ).height() - ( $( 'body' ).height() - state.position().top ) )
        {
          sticky_mode = true;
        }

        else
        {
          sticky_mode = false;
        }
      },
      success : function( response, text_status, xhr )
      {
        var docs = response.history.docs;
        var docs_count = docs.length;

        var table = $( 'table', frame_element );

        $( 'h2 span', frame_element )
          .text( response.watcher.esc() );

        state
          .html( 'Last Check: ' + format_time() );

        app.timeout = setTimeout
        (
          load_logging_viewer,
          10000
        );

        if( 0 === docs_count )
        {
          table.trigger( 'update' );
          return false;
        }

        var content = '<tbody>';

        for( var i = 0; i < docs_count; i++ )
        {
          var doc = docs[i];

          if( 1 === doc.time.length )
          {
            for( var key in doc )
            {
              doc[key] = doc[key][0];
            }
          }

          if( !doc.trace )
          {
            var lines = doc.message.split( "\n" );
            if( 1 < lines.length )
            {
              doc.trace = doc.message;
              doc.message = lines[0];
              delete lines;
            }
          }

          var has_trace = 'undefined' !== typeof( doc.trace );

          doc.logger = '<abbr title="' + doc.logger.esc() + '">' + doc.logger.split( '.' ).pop().esc() + '</abbr>';

          var classes = [ 'level-' + doc.level.toLowerCase().esc() ];
          if( has_trace )
          {
            classes.push( 'has-trace' );
          }

          content += '<tr class="' + classes.join( ' ' ) + '">' + "\n";
            content += '<td class="span"><a><span>' + format_time( doc.time ) + '</span></a></td>' + "\n";
            content += '<td class="level span"><a><span>' + doc.level.esc() + '</span></span></a></td>' + "\n";
            content += '<td class="span"><a><span>' + doc.logger + '</span></a></td>' + "\n";
            content += '<td class="message span"><a><span>' + doc.message.replace( /,/g, ',&#8203;' ).esc() + '</span></a></td>' + "\n";
          content += '</tr>' + "\n";

          if( has_trace )
          {
            content += '<tr class="trace">' + "\n";
              content += '<td colspan="4"><pre>' + doc.trace.esc() + '</pre></td>' + "\n";
            content += '</tr>' + "\n";
          }

        }

        content += '</tbody>';

        $( 'table', frame_element )
          .append( content );

        table
          .data( 'latest', response.info.last )
          .removeClass( 'has-data' )
          .trigger( 'update' );

        if( sticky_mode )
        {
          $( 'body' )
            .animate
            (
                { scrollTop: state.position().top },
                1000
            );
        }
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

// #/~logging
sammy.get
(
  /^#\/(~logging)$/,
  function( context )
  {
    var content_element = $( '#content' );

    $.get
    (
      'tpl/logging.html',
      function( template )
      {
        content_element
          .html( template );

        frame_element = $( '#frame', content_element );
        frame_element
          .html
          (
            '<div id="viewer">' + "\n" +
              '<div class="block">' + "\n" +
                '<h2><span>&nbsp;</span></h2>' + "\n" +
              '</div>' + "\n" +
              '<table border="0" cellpadding="0" cellspacing="0">' + "\n" +
                '<thead>' + "\n" +
                  '<tr>' + "\n" +
                    '<th class="time">Time (<span>Local</span>)</th>' + "\n" +
                    '<th class="level">Level</th>' + "\n" +
                    '<th class="logger">Logger</th>' + "\n" +
                    '<th class="message">Message</th>' + "\n" +
                  '</tr>' + "\n" +
                '</thead>' + "\n" +
                '<tfoot>' + "\n" +
                  '<tr>' + "\n" +
                    '<td colspan="4">No Events available</td>' + "\n" +
                  '</tr>' + "\n" +
                '</thead>' + "\n" +
              '</table>' + "\n" +
              '<div id="footer" class="clearfix">' + "\n" +
                '<div id="state" class="loader">&nbsp;</div>' + "\n" +
                '<div id="date-format"><a>Show dates in UTC</a></div>' + "\n" +
              '</div>' + "\n" +
            '</div>'
          );

        var table = $( 'table', frame_element );

        table
          .die( 'update' )
          .live
          (
            'update',
            function( event )
            {
              var table = $( this );
              var tbody = $( 'tbody', table );

              0 !== tbody.size()
                ? table.addClass( 'has-data' )
                : table.removeClass( 'has-data' );

              return false;
            }
          );

        load_logging_viewer();

        $( '.has-trace a', table )
          .die( 'click' )
          .live
          (
            'click',
            function( event )
            {
              $( this ).closest( 'tr' )
                .toggleClass( 'open' )
                .next().toggle();

              return false;
            }
          );

        var date_format = $( '#date-format a', frame_element );

        date_format
          .off( 'click' )
          .on
          (
            'click',
            function( event )
            {
              var self = $( this );

              if( !self.hasClass( 'on' ) )
              {
                self.addClass( 'on' );
                $( 'table th.time span', frame_element ).text( 'UTC' );
                format_time_options.timeZone = 'UTC';
                $.cookie( cookie_logging_timezone, 'UTC' );
              }
              else
              {
                self.removeClass( 'on' );
                $( 'table th.time span', frame_element ).text( 'Local' );
                delete format_time_options.timeZone;
                $.cookie( cookie_logging_timezone, null );
              }

              $( 'time', frame_element )
                .each
                (
                  function( index, element )
                  {
                    var self = $( element );
                    self.text( format_time_content( new Date( self.attr( 'datetime' ) ) ) );
                  }
                )

              return false;
            }
          );

        if( 'UTC' === $.cookie( cookie_logging_timezone ) )
        {
          date_format
            .trigger( 'click' );
        }
      }
    );
  }
);

// #/~logging/level
sammy.get
(
  /^#\/(~logging)\/level$/,
  function( context )
  {
    var content_element = $( '#content' );

    $.get
    (
      'tpl/logging.html',
      function( template )
      {
        content_element
          .html( template );

        $( '#menu a[href="' + context.path + '"]' )
          .parent().addClass( 'active' );
                      
        $.ajax
        (
          {
            url : loglevel_path + '?wt=json',
            dataType : 'json',
            context : $( '#frame', content_element ),
            beforeSend : function( xhr, settings )
            {
              this
                .html( '<div class="loader">Loading ...</div>' );
            },
            success : logging_handler
          }
        );
      }
    );
  }
);