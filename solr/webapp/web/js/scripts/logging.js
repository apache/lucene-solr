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

var loglevel_path = null;
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

      logger_content += '<span class="name">' + display_name.esc() + '</span>' + "\n";
      logger_content += '</a>';

      logger_content += levels;

      var child_logger_content = logger_tree( logger.name );
      if( child_logger_content )
      {
        logger_content += '<ul>';
        logger_content += child_logger_content;
        logger_content += '</ul>';
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

// #/~logging
sammy.get
(
  /^#\/~(logging)$/,
  function( context )
  {
    var core_basepath = $( 'li[data-basepath]', app.menu_element ).attr( 'data-basepath' );
    loglevel_path = core_basepath + '/admin/logging';
    var content_element = $( '#content' );
        
    content_element
      .html( '<div id="logging"></div>' );

    $.ajax
    (
      {
        url : loglevel_path + '?wt=json',
        dataType : 'json',
        context : $( '#logging', content_element ),
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