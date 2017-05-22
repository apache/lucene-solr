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
      when('/~cores/:corename', {
        templateUrl: 'partials/cores.html',
        controller: 'CoreAdminController'
      }).
      when('/~collections', {
        templateUrl: 'partials/collections.html',
        controller: 'CollectionsController'
      }).
      when('/~collections/:collection', {
        templateUrl: 'partials/collections.html',
        controller: 'CollectionsController'
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
      when('/:core/collection-overview', {
        templateUrl: 'partials/collection_overview.html',
        controller: 'CollectionOverviewController'
      }).
      when('/:core/analysis', {
        templateUrl: 'partials/analysis.html',
        controller: 'AnalysisController'
      }).
      when('/:core/dataimport', {
        templateUrl: 'partials/dataimport.html',
        controller: 'DataImportController'
      }).
      when('/:core/dataimport/:handler*', {
        templateUrl: 'partials/dataimport.html',
        controller: 'DataImportController'
      }).
      when('/:core/documents', {
        templateUrl: 'partials/documents.html',
        controller: 'DocumentsController'
      }).
      when('/:core/files', {
        templateUrl: 'partials/files.html',
        controller: 'FilesController'
      }).
      when('/:core/plugins', {
        templateUrl: 'partials/plugins.html',
        controller: 'PluginsController',
        reloadOnSearch: false
      }).
      when('/:core/plugins/:legacytype', {
        templateUrl: 'partials/plugins.html',
        controller: 'PluginsController',
        reloadOnSearch: false
      }).
      when('/:core/query', {
        templateUrl: 'partials/query.html',
        controller: 'QueryController'
      }).
      when('/:core/stream', {
        templateUrl: 'partials/stream.html',
        controller: 'StreamController'
      }).
      when('/:core/replication', {
        templateUrl: 'partials/replication.html',
        controller: 'ReplicationController'
      }).
      when('/:core/dataimport', {
        templateUrl: 'partials/dataimport.html',
        controller: 'DataImportController'
      }).
      when('/:core/dataimport/:handler*', {
        templateUrl: 'partials/dataimport.html',
        controller: 'DataImportController'
      }).
      when('/:core/schema', {
        templateUrl: 'partials/schema.html',
        controller: 'SchemaController'
      }).
      when('/:core/segments', {
        templateUrl: 'partials/segments.html',
        controller: 'SegmentsController'
      }).
      otherwise({
        redirectTo: '/'
      });
}])
.constant('Constants', {
  IS_ROOT_PAGE: 1,
  IS_CORE_PAGE: 2,
  IS_COLLECTION_PAGE: 3,
  ROOT_URL: "/"
})
.filter('uriencode', function() {
  return window.encodeURIComponent;
})
.filter('highlight', function($sce) {
  return function(input, lang) {
    if (lang && input && lang!="txt" && lang!="csv") return hljs.highlight(lang, input).value;
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
.directive('escapePressed', function () {
    return function (scope, element, attrs) {
        element.bind("keydown keypress", function (event) {
            if(event.which === 27) {
                scope.$apply(function (){
                    scope.$eval(attrs.escapePressed);
                });
                event.preventDefault();
            }
        });
    };
})
.directive('focusWhen', function($timeout) {
  return {
    link: function(scope, element, attrs) {
      scope.$watch(attrs.focusWhen, function(value) {
        if(value === true) {
          $timeout(function() {
            element[0].focus();
          }, 100);
        }
      });
    }
  };
})
.directive('scrollableWhenSmall', function($window) {
  return {
    link: function(scope, element, attrs) {
      var w = angular.element($window);

      var checkFixedMenu = function() {
        var shouldScroll = w.height() < (element.height() + $('#header').height() + 40);
        element.toggleClass( 'scroll', shouldScroll);
      };
      w.bind('resize', checkFixedMenu);
      w.bind('load', checkFixedMenu);
    }
  }
})
.filter('readableSeconds', function() {
    return function(input) {
    seconds = parseInt(input||0, 10);
    var minutes = Math.floor( seconds / 60 );
    var hours = Math.floor( minutes / 60 );

    var text = [];
    if( 0 !== hours ) {
      text.push( hours + 'h' );
      seconds -= hours * 60 * 60;
      minutes -= hours * 60;
    }

    if( 0 !== minutes ) {
      text.push( minutes + 'm' );
      seconds -= minutes * 60;
    }

    if( 0 !== seconds ) {
      text.push( ( '0' + seconds ).substr( -2 ) + 's' );
    }
    return text.join(' ');
  };
})
.filter('number', function($locale) {
    return function(input) {
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

        var browser = {};
        var match = $locale.id.match( /^(\w{2})([-_](\w{2}))?$/ );
        if (match[1]) {
            browser.language = match[1].toLowerCase();
        }
        if (match[1] && match[3]) {
            browser.locale = match[1] + '_' + match[3];
        }

        return ( input || 0 ).toString().replace(/\B(?=(\d{3})+(?!\d))/g,
            sep[ browser.locale ] || sep[ browser.language ] || sep['_']);
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

                  var tree = $(element).jstree(treeConfig);
                  tree.jstree('open_node','li:first');
                  if (tree) {
                      element.bind("select_node.jstree", function (event, data) {
                          scope.$apply(function() {
                              scope.onSelect({url: data.args[0].href, data: data});
                          });
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
    if (activeRequests == 0) {
      $rootScope.$broadcast('loadingStatusActive');
    }
    if ($rootScope.exceptions[config.url]) {
      delete $rootScope.exceptions[config.url];
    }
    activeRequests++;
    config.timeout = 10000;
    return config || $q.when(config);
  };

  var ended = function(response) {
    activeRequests--;
    if (activeRequests == 0) {
      $rootScope.$broadcast('loadingStatusInactive');
    }
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
    if (rejection.config.headers.doNotIntercept) {
        return rejection;
    }
    if (rejection.status === 0) {
      $rootScope.$broadcast('connectionStatusActive');
      if (!$rootScope.retryCount) $rootScope.retryCount=0;
      $rootScope.retryCount ++;
      var $http = $injector.get('$http');
      var result = $http(rejection.config);
      return result;
    } else {
      $rootScope.exceptions[rejection.config.url] = rejection.data.error;
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

solrAdminApp.controller('MainController', function($scope, $route, $rootScope, $location, Cores, Collections, System, Ping, Constants) {

  $rootScope.exceptions={};

  $rootScope.toggleException = function() {
    $scope.showException=!$scope.showException;
  };

  $scope.refresh = function() {
      $scope.cores = [];
      $scope.collections = [];
  }

  $scope.refresh();
  $scope.resetMenu = function(page, pageType) {
    Cores.list(function(data) {
      $scope.cores = [];
      var currentCoreName = $route.current.params.core;
      delete $scope.currentCore;
      for (key in data.status) {
        var core = data.status[key];
        $scope.cores.push(core);
        if ((!$scope.isSolrCloud || pageType == Constants.IS_CORE_PAGE) && core.name == currentCoreName) {
            $scope.currentCore = core;
        }
      }
      $scope.showInitFailures = Object.keys(data.initFailures).length>0;
      $scope.initFailures = data.initFailures;
    });

    System.get(function(data) {
      $scope.isCloudEnabled = data.mode.match( /solrcloud/i );

      if ($scope.isCloudEnabled) {
        Collections.list(function (data) {
          $scope.collections = [];
          var currentCollectionName = $route.current.params.core;
          delete $scope.currentCollection;
          for (key in data.collections) {
            var collection = {name: data.collections[key]};
            $scope.collections.push(collection);
            if (pageType == Constants.IS_COLLECTION_PAGE && collection.name == currentCollectionName) {
              $scope.currentCollection = collection;
            }
          }
        })
      }

    });

    $scope.showingLogging = page.lastIndexOf("logging", 0) === 0;
    $scope.showingCloud = page.lastIndexOf("cloud", 0) === 0;
    $scope.page = page;
  };

  $scope.ping = function() {
    Ping.ping({core: $scope.currentCore.name}, function(data) {
      $scope.showPing = true;
      $scope.pingMS = data.responseHeader.QTime;
    });
    // @todo .attr( 'title', '/admin/ping is not configured (' + xhr.status + ': ' + error_thrown + ')' );
  };

  $scope.dumpCloud = function() {
      $scope.$broadcast("cloud-dump");
  }

  $scope.showCore = function(core) {
    $location.url("/" + core.name);
  }

  $scope.showCollection = function(collection) {
    $location.url("/" + collection.name + "/collection-overview")
  }

  $scope.$on('$routeChangeStart', function() {
      $rootScope.exceptions = {};
  });
});
