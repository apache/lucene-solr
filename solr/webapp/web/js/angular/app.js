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

var solrAdminApp = angular.module("solrAdminApp", [
  "ngResource",
  "ngRoute",
  "ngCookies",
  "ngtimeago",
  "solrAdminServices",
  "localytics.directives"
]);

solrAdminApp.config([
  '$routeProvider', function($routeProvider) {
    $routeProvider.
      when('/', {
        templateUrl: 'partials/index.html',
        controller: 'IndexController'
      }).
      when('/~logging', {
        templateUrl: 'partials/logging.html',
        controller: 'LoggingController'
      }).
      when('/~logging/level', {
        templateUrl: 'partials/logging-levels.html',
        controller: 'LoggingLevelController'
      }).
      when('/~cloud', {
        templateUrl: 'partials/cloud.html',
        controller: 'CloudController'
      }).
      when('/~cores', {
        templateUrl: 'partials/cores.html',
        controller: 'CoreAdminController'
      }).
      when('/~cores/:core', {
        templateUrl: 'partials/cores.html',
        controller: 'CoreAdminController'
      }).
      when('/~threads', {
        templateUrl: 'partials/threads.html',
        controller: 'ThreadsController'
      }).
      when('/~java-properties', {
        templateUrl: 'partials/java-properties.html',
        controller: 'JavaPropertiesController'
      }).
      when('/:core', {
        templateUrl: 'partials/core_overview.html',
        controller: 'CoreOverviewController'
      }).
      when('/:core/analysis', {
        templateUrl: 'partials/analysis.html',
        controller: 'AnalysisController'
      }).
      when('/:core/documents', {
        templateUrl: 'partials/documents.html',
        controller: 'DocumentsController'
      }).
      when('/:core/query', {
        templateUrl: 'partials/query.html',
        controller: 'QueryController'
      }).
      otherwise({
        redirectTo: '/'
      });
}])
.filter('highlight', function($sce) {
  return function(input, lang) {
    if (lang && input) return hljs.highlight(lang, input).value;
    return input;
  }
})
.filter('unsafe', function($sce) { return $sce.trustAsHtml; })
.directive('loadingStatusMessage', function() {
  return {
    link: function($scope, $element, attrs) {
      var show = function() {$element.css('display', 'block')};
      var hide = function() {$element.css('display', 'none')};
      $scope.$on('loadingStatusActive', show);
      $scope.$on('loadingStatusInactive', hide);
    }
  };
})
.filter('orderObjectBy', function() {
  return function(items, field, reverse) {
    var filtered = [];
    angular.forEach(items, function(item) {
      filtered.push(item);
    });
    filtered.sort(function (a, b) {
      return (a[field] > b[field] ? 1 : -1);
    });
    if(reverse) filtered.reverse();
    return filtered;
  };
})
.directive('jstree', function($parse) {
    return {
        restrict: 'EA',
        scope: {
          data: '=',
          onSelect: '&'
        },
        link: function(scope, element, attrs) {
            scope.$watch("data", function(newValue, oldValue) {
                if (newValue) {
                  var treeConfig = {
                      "plugins" : [ "themes", "json_data", "ui" ],
                      "json_data" : {
                        "data" : scope.data,
                        "progressive_render" : true
                      },
                      "core" : {
                        "animation" : 0
                      }
                  };
                  var tree = $(element).jstree(treeConfig).jstree('open_node','li:first');
                  if (tree) {
                      tree.bind("select_node.jstree", function (event, data) {
                          scope.onSelect({url: data.args[0].href});
                      });
                  }
                }
            }, true);
        }
    };
})
.directive('connectionMessage', function() {
  return {
    link: function($scope, $element, attrs) {
      var show = function() {$element.css('display', 'block')};
      var hide = function() {$element.css('display', 'none')};
      $scope.$on('connectionStatusActive', show);
      $scope.$on('connectionStatusInactive', hide);
    }
  };
})
.factory('httpInterceptor', function($q, $rootScope, $timeout, $injector) {
  var activeRequests = 0;

  var started = function(config) {
    console.log("start HTTP for " + config.url);
    if (activeRequests == 0) {
      $rootScope.$broadcast('loadingStatusActive');
    }
    activeRequests++;
    config.timeout = 1000;
    return config || $q.when(config);
  };

  var ended = function(response) {
    activeRequests--;
    if (activeRequests == 0) {
      $rootScope.$broadcast('loadingStatusInactive');
    }
    console.log("ended");
    if ($rootScope.retryCount>0) {
      $rootScope.connectionRecovered = true;
      $rootScope.retryCount=0;
      $timeout(function() {
        $rootScope.connectionRecovered=false;
        $rootScope.$broadcast('connectionStatusInactive');
      },2000);
    }
    return response || $q.when(response);
  };

  var failed = function(rejection) {
    activeRequests--;
    if (activeRequests == 0) {
      $rootScope.$broadcast('loadingStatusInactive');
    }
    console.log("ERROR " + rejection.status + ": " + rejection.config.url);
    if (rejection.status === 0) {
      $rootScope.$broadcast('connectionStatusActive');
      if (!$rootScope.retryCount) $rootScope.retryCount=0;
      $rootScope.retryCount ++;
      var $http = $injector.get('$http');
      var result = $http(rejection.config);
      return result;
    } else {
      $rootScope.exception = rejection;
    }
    return $q.reject(rejection);
  }

  return {request: started, response: ended, responseError: failed};
})
.config(function($httpProvider) {
  $httpProvider.interceptors.push("httpInterceptor");
})
.directive('fileModel', function ($parse) {
    return {
        restrict: 'A',
        link: function(scope, element, attrs) {
            var model = $parse(attrs.fileModel);
            var modelSetter = model.assign;

            element.bind('change', function(){
                scope.$apply(function(){
                    modelSetter(scope, element[0].files[0]);
                });
            });
        }
    };
});

var solrAdminControllers = angular.module('solrAdminControllers', []);

solrAdminApp.controller('MainController', function($scope, $routeParams, $rootScope, $location, Cores, Ping) {
  $rootScope.hideException = function() {delete $rootScope.exception};
  $scope.refresh = function() {
    Cores.list(function(data) {
     var cores = [];
       for (key in data.status) {
        cores.push(data.status[key]);
      }
      $scope.cores = cores;
    });
  };
  $scope.refresh();

  $scope.resetMenu = function(page) {
    $scope.showingLogging = page.lastIndexOf("logging", 0) === 0;
    $scope.isCloudEnabled = true;
    $scope.showingCloud = page.lastIndexOf("cloud", 0) === 0;
    $scope.page = page;
  };

  $scope.ping = function() {
    Ping.ping({core: $scope.currentCore.name}, function(data) {
      $scope.pingMS = data.responseHeader.QTime;
    });
    // @todo .attr( 'title', '/admin/ping is not configured (' + xhr.status + ': ' + error_thrown + ')' );
  };

  $scope.dumpCloud = function() {
      $scope.$broadcast("cloud-dump");
  }
});



/* THE BELOW CODE IS TAKEN FROM js/scripts/app.js, AND STILL REQUIRES INTEGRATING

SolrDate = function( date )
{
  // ["Sat Mar 03 11:00:00 CET 2012", "Sat", "Mar", "03", "11:00:00", "CET", "2012"]
  var parts = date.match( /^(\w+)\s+(\w+)\s+(\d+)\s+(\d+\:\d+\:\d+)\s+(\w+)\s+(\d+)$/ );

  // "Sat Mar 03 2012 10:37:33"
  return new Date( parts[1] + ' ' + parts[2] + ' ' + parts[3] + ' ' + parts[6] + ' ' + parts[4] );
}

// @todo clear timeouts

    this.bind
    (
      'error',
      function( message, original_error )
      {
        alert( original_error.message );
      }
    );

    // activate_core
    this.before
    (
      {},
      function( context )
      {

        var menu_wrapper = $( '#menu-wrapper' );

        // global dashboard doesn't have params.splat
        if( !this.params.splat )
        {
          this.params.splat = [ '~index' ];
        }

        var selector = '~' === this.params.splat[0][0]
                     ? '#' + this.params.splat[0].replace( /^~/, '' ) + '.global'
                     : '#core-selector #' + this.params.splat[0].replace( /\./g, '__' );

        var active_element = $( selector, menu_wrapper );

        // @todo "There is no core with this name"

        if( active_element.hasClass( 'global' ) )
        {
          active_element
            .addClass( 'active' );

          if( this.params.splat[1] )
          {
            $( '.' + this.params.splat[1], active_element )
              .addClass( 'active' );
          }

          $( '#core-selector option[selected]' )
            .removeAttr( 'selected' )
            .trigger( 'liszt:updated' );

          $( '#core-selector .chzn-container > a' )
            .addClass( 'chzn-default' );
        }
        else
        {
          active_element
            .attr( 'selected', 'selected' )
            .trigger( 'liszt:updated' );


          $( '#core-menu .' + this.params.splat[1] )
            .addClass( 'active' );

      }
    );
  }
);

var solr_admin = function( app_config )
{
  this.menu_element = $( '#core-selector select' );
  this.core_menu = $( '#core-menu ul' );

  this.config = config;
  this.timeout = null;

  this.core_regex_base = '^#\\/([\\w\\d-\\.]+)';

  browser = {
    locale : null,
    language : null,
    country : null
  };

  show_global_error = function( error )
  {
    var main = $( '#main' );

    $( 'div[id$="-wrapper"]', main )
      .remove();

    main
      .addClass( 'error' )
      .append( error );

    var pre_tags = $( 'pre', main );
    if( 0 !== pre_tags.size() )
    {
      hljs.highlightBlock( pre_tags.get(0) );
    }
  };

  sort_cores_data = function sort_cores_data( cores_status )
  {
    // build array of core-names for sorting
    var core_names = [];
    for( var core_name in cores_status )
    {
      core_names.push( core_name );
    }
    core_names.sort();

    var core_count = core_names.length;
    var cores = {};

    for( var i = 0; i < core_count; i++ )
    {
      var core_name = core_names[i];
      cores[core_name] = cores_status[core_name];
    }

    return cores;
  };

  this.set_cores_data = function set_cores_data( cores )
  {
    that.cores_data = sort_cores_data( cores.status );

    that.menu_element
      .empty();

    var core_list = [];
    core_list.push( '<option></option>' );

    var core_count = 0;
    for( var core_name in that.cores_data )
    {
      core_count++;
      var core_path = config.solr_path + '/' + core_name;
      var classes = [];

      if( cores.status[core_name]['isDefaultCore'] )
      {
        classes.push( 'default' );
      }

      var core_tpl = '<option '
                   + '    id="' + core_name.replace( /\./g, '__' ) + '" '
                   + '    class="' + classes.join( ' ' ) + '"'
                   + '    data-basepath="' + core_path + '"'
                   + '    schema="' + cores.status[core_name]['schema'] + '"'
                   + '    config="' + cores.status[core_name]['config'] + '"'
                   + '    value="#/' + core_name + '"'
                   + '    title="' + core_name + '"'
                   + '>'
                   + core_name
                   + '</option>';

      core_list.push( core_tpl );
    }

    var has_cores = 0 !== core_count;
    if( has_cores )
    {
      that.menu_element
        .append( core_list.join( "\n" ) )
        .trigger( 'liszt:updated' );
    }

    var core_selector = $( '#core-selector' );
    core_selector.find( '#has-cores' ).toggle( has_cores );
    core_selector.find( '#has-no-cores' ).toggle( !has_cores );

    if( has_cores )
    {
      var cores_element = core_selector.find( '#has-cores' );
      var selector_width = cores_element.width();

      cores_element.find( '.chzn-container' )
        .css( 'width', selector_width + 'px' );

      cores_element.find( '.chzn-drop' )
        .css( 'width', ( selector_width - 2 ) + 'px' );
    }

    this.check_for_init_failures( cores );
  };

  this.remove_init_failures = function remove_init_failures()
  {
    $( '#init-failures' )
      .hide()
      .find( 'ul' )
        .empty();
  }

  this.check_for_init_failures = function check_for_init_failures( cores )
  {
    if( !cores.initFailures )
    {
      this.remove_init_failures();
      return false;
    }

    var failures = [];
    for( var core_name in cores.initFailures )
    {
      failures.push
      (
        '<li>' +
          '<strong>' + core_name.esc() + ':</strong>' + "\n" +
          cores.initFailures[core_name].esc() + "\n" +
        '</li>'
      );
    }

    if( 0 === failures.length )
    {
      this.remove_init_failures();
      return false;
    }

    $( '#init-failures' )
      .show()
      .find( 'ul' )
        .html( failures.join( "\n" ) );
  }

  this.run = function()
  {
    var navigator_language = navigator.userLanguage || navigator.language;
    var language_match = navigator_language.match( /^(\w{2})([-_](\w{2}))?$/ );
    if( language_match )
    {
      if( language_match[1] )
      {
        browser.language = language_match[1].toLowerCase();
      }
      if( language_match[3] )
      {
        browser.country = language_match[3].toUpperCase();
      }
      if( language_match[1] && language_match[3] )
      {
        browser.locale = browser.language + '_' + browser.country
      }
    }

    $.ajax
    (
      {
        // load cores (indexInfo = false
        success : function( response )
        {
          check_fixed_menu();
          $( window ).resize( check_fixed_menu );

          var system_url = config.solr_path + '/admin/info/system?wt=json';
          $.ajax
          (
            {
              url : system_url,
              dataType : 'json',
              beforeSend : function( arr, form, options )
              {
              },
              success : function( response )
              {
                that.dashboard_values = response;

                var environment_args = null;
                var cloud_args = null;

                if( response.jvm && response.jvm.jmx && response.jvm.jmx.commandLineArgs )
                {
                  var command_line_args = response.jvm.jmx.commandLineArgs.join( ' | ' );

                  environment_args = command_line_args.match( /-Dsolr.environment=((dev|test|prod)?[\w\d]*)/i );
                }

// @todo detect $scope.isCloud = response.mode.match( /solrcloud/i );

                // environment

                var wrapper = $( '#wrapper' );
                var environment_element = $( '#environment' );
                if( environment_args )
                {
                  wrapper
                    .addClass( 'has-environment' );

                  if( environment_args[1] )
                  {
                    environment_element
                      .html( environment_args[1] );
                  }

                  if( environment_args[2] )
                  {
                    environment_element
                      .addClass( environment_args[2] );
                  }
                }
                else
                {
                  wrapper
                    .removeClass( 'has-environment' );
                }

                // cloud

                var cloud_nav_element = $( '#menu #cloud' );
                if( cloud_args )
                {
                  cloud_nav_element
                    .show();
                }

                // sammy

                sammy.run( location.hash );
              },
              error : function()
              {
                show_global_error
                (
                  '<div class="message"><p>Unable to load environment info from <code>' + system_url.esc() + '</code>.</p>' +
                  '<p>This interface requires that you activate the admin request handlers in all SolrCores by adding the ' +
                  'following configuration to your <code>solrconfig.xml</code>:</p></div>' + "\n" +

                  '<div class="code"><pre class="syntax language-xml"><code>' +
                  '<!-- Admin Handlers - This will register all the standard admin RequestHandlers. -->'.esc() + "\n" +
                  '<requestHandler name="/admin/" class="solr.admin.AdminHandlers" />'.esc() +
                  '</code></pre></div>'
                );
              },
              complete : function()
              {
                loader.hide( this );
              }
            }
          );
        },
        error : function()
        {
        },
        complete : function()
        {
        }
      }
    );
  };

  this.convert_duration_to_seconds = function convert_duration_to_seconds( str )
  {
    var seconds = 0;
    var arr = new String( str || '' ).split( '.' );
    var parts = arr[0].split( ':' ).reverse();
    var parts_count = parts.length;

    for( var i = 0; i < parts_count; i++ )
    {
      seconds += ( parseInt( parts[i], 10 ) || 0 ) * Math.pow( 60, i );
    }

    // treat more or equal than .5 as additional second
    if( arr[1] && 5 <= parseInt( arr[1][0], 10 ) )
    {
      seconds++;
    }

    return seconds;
  };

  this.convert_seconds_to_readable_time = function convert_seconds_to_readable_time( seconds )
  {
    seconds = parseInt( seconds || 0, 10 );
    var minutes = Math.floor( seconds / 60 );
    var hours = Math.floor( minutes / 60 );

    var text = [];
    if( 0 !== hours )
    {
      text.push( hours + 'h' );
      seconds -= hours * 60 * 60;
      minutes -= hours * 60;
    }

    if( 0 !== minutes )
    {
      text.push( minutes + 'm' );
      seconds -= minutes * 60;
    }

    if( 0 !== seconds )
    {
      text.push( ( '0' + seconds ).substr( -2 ) + 's' );
    }

    return text.join( ' ' );
  };

  this.format_json = function format_json( json_str )
  {
    if( JSON.stringify && JSON.parse )
    {
      json_str = JSON.stringify( JSON.parse( json_str ), undefined, 2 );
    }

    return json_str.esc();
  };

  this.format_number = function format_number( number )
  {
    var sep = {
      'de_CH' : '\'',
      'de' : '.',
      'en' : ',',
      'es' : '.',
      'it' : '.',
      'ja' : ',',
      'sv' : ' ',
      'tr' : '.',
      '_' : '' // fallback
    };

    return ( number || 0 ).toString().replace
    (
      /\B(?=(\d{3})+(?!\d))/g,
      sep[ browser.locale ] || sep[ browser.language ] || sep['_']
    );
  };

  check_fixed_menu = function check_fixed_menu()
  {
    $( '#wrapper' ).toggleClass( 'scroll', $( window ).height() < $( '#menu-wrapper' ).height() + $( '#header' ).height() + 40 );
  }

};



$.ajaxSetup( { cache: false } );
var app = new solr_admin( app_config );
*/
