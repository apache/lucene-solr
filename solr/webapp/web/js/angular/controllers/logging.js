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

var format_time_content = function( time, timeZone ) {
  var format_time_options = {};
  if (timeZone && timeZone!="Local") {
    format_time_options.timeZone = timeZone;
  }
  return time.toLocaleString( undefined, format_time_options );
}

solrAdminApp.controller('LoggingController',
  function($scope, $timeout, $cookies, Logging, Constants){
    $scope.resetMenu("logging", Constants.IS_ROOT_PAGE);
    $scope.timezone = $cookies.logging_timezone || "Local";
    $scope.refresh = function() {
      Logging.events(function(data) {
        $scope.since = new Date();
        $scope.sinceDisplay = format_time_content($scope.since, "Local");
        var events = data.history.docs;
        for (var i=0; i<events.length; i++) {
          var event = events[i];
          var time = new Date(event.time);
          event.local_time = format_time_content(time, "Local");
          event.utc_time = format_time_content(time, "UTC");
          event.loggerBase = event.logger.split( '.' ).pop();

          if( !event.trace ) {
            var lines = event.message.split( "\n" );
            if( lines.length > 1) {
              event.trace = event.message;
              event.message = lines[0];
            }
          }
          event.message = event.message.replace(/,/g, ',&#8203;');
          event.showTrace = false;
        }
        $scope.events = events;
        $scope.watcher = data.watcher;
        /* @todo sticky_mode
        // state element is in viewport
        sticky_mode = ( state.position().top <= $( window ).scrollTop() + $( window ).height() - ( $( 'body' ).height() - state.position().top ) );
        // initial request
        if( 0 === since ) {
          sticky_mode = true;
        }
        $scope.loggingEvents = events;

        if( sticky_mode )
        {
          $( 'body' )
            .animate
            (
                { scrollTop: state.position().top },
                1000
            );
        }
      */
      });
      $scope.timeout = $timeout($scope.refresh, 10000);
      var onRouteChangeOff = $scope.$on('$routeChangeStart', function() {
        $timeout.cancel($scope.timeout);
        onRouteChangeOff();
      });
    };
    $scope.refresh();

    $scope.toggleTimezone = function() {
      $scope.timezone = ($scope.timezone=="Local") ? "UTC":"Local";
      $cookies.logging_timezone = $scope.timezone;
    }
    $scope.toggleRow = function(event) {
      event.showTrace =! event.showTrace;
    };
   }
)

.controller('LoggingLevelController',
  function($scope, Logging) {
    $scope.resetMenu("logging-levels");

    var packageOf = function(logger) {
      var parts = logger.name.split(".");
      return !parts.pop() ? "" : parts.join(".");
    };

    var shortNameOf = function(logger) {return logger.name.split(".").pop();}

    var makeTree = function(loggers, packag) {
      var tree = [];
      for (var i=0; i<loggers.length; i++) {
        var logger = loggers[i];
        logger.packag = packageOf(logger);
        logger.short = shortNameOf(logger);
        if (logger.packag == packag) {
          logger.children = makeTree(loggers, logger.name);
          tree.push(logger);
        }
      }
      return tree;
    };

    $scope.refresh = function() {
      Logging.levels(function(data) {
        $scope.logging = makeTree(data.loggers, "");
        $scope.watcher = data.watcher;
        $scope.levels = [];
        for (level in data.levels) {
          $scope.levels.push({name:data.levels[level], pos:level});
        }
      });
    };

    $scope.toggleOptions = function(logger) {
      if (logger.showOptions) {
        logger.showOptions = false;
        delete $scope.currentLogger;
      } else {
        if ($scope.currentLogger) {
          $scope.currentLogger.showOptions = false;
        }
        logger.showOptions = true;
        $scope.currentLogger = logger;
      }
    };

    $scope.setLevel = function(logger, newLevel) {
      var setString = logger.name + ":" + newLevel;
      logger.showOptions = false;
      Logging.setLevel({set: setString}, function(data) {
        $scope.refresh();
      });
    };

    $scope.refresh();
  });
