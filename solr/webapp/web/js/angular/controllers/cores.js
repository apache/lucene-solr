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

// @todo test optimize (delete stuff, watch button appear, test button/form)
solrAdminApp.controller('CoreAdminController',
    function($scope, $routeParams, $location, $timeout, $route, Cores, Update, Constants){
      $scope.resetMenu("cores", Constants.IS_ROOT_PAGE);
      $scope.selectedCore = $routeParams.corename; // use 'corename' not 'core' to distinguish from /solr/:core/
      $scope.refresh = function() {
        Cores.get(function(data) {
          var coreCount = 0;
          var cores = data.status;
          for (_obj in cores) coreCount++;
          $scope.hasCores = coreCount >0;
          if (!$scope.selectedCore && coreCount==0) {
            $scope.showAddCore();
            return;
          } else if (!$scope.selectedCore) {
            for (firstCore in cores) break;
            $scope.selectedCore = firstCore;
            $location.path("/~cores/" + $scope.selectedCore).replace();
          }
          $scope.core = cores[$scope.selectedCore];
          $scope.corelist = [];
          $scope.swapCorelist = [];
          for (var core in cores) {
             $scope.corelist.push(cores[core]);
            if (cores[core] != $scope.core) {
              $scope.swapCorelist.push(cores[core]);
            }
          }
          if ($scope.swapCorelist.length>0) {
            $scope.swapOther = $scope.swapCorelist[0].name;
          }
        });
      };
      $scope.showAddCore = function() {
        $scope.hideAll();
        $scope.showAdd = true;
        $scope.newCore = {
          name: "new_core",
          dataDir: "data",
          instanceDir: "new_core",
          config: "solrconfig.xml",
          schema: "schema.xml",
          collection: "",
          shard: ""
        };
      };

      $scope.addCore = function() {
        if (!$scope.newCore.name) {
          $scope.addMessage = "Please provide a core name";
        } else if (false) { //@todo detect whether core exists
          $scope.AddMessage = "A core with that name already exists";
        } else {
          var params = {
            name: $scope.newCore.name,
            instanceDir: $scope.newCore.instanceDir,
            config: $scope.newCore.config,
            schema: $scope.newCore.schema,
            dataDir: $scope.newCore.dataDir
          };
          if ($scope.isCloud) {
            params.collection = $scope.newCore.collection;
            params.shard = $scope.newCore.shard;
          }
          Cores.add(params, function(data) {
            $location.path("/~cores/" + $scope.newCore.name);
            $scope.cancelAddCore();
          });
        }
      };

      $scope.cancelAddCore = function() {
        delete $scope.addMessage;
        $scope.showAdd = false
      };

      $scope.unloadCore = function() {
        var answer = confirm( 'Do you really want to unload Core "' + $scope.selectedCore + '"?' );
        if( !answer ) return;
        Cores.unload({core: $scope.selectedCore}, function(data) {
          $location.path("/~cores");
        });
      };

      $scope.showRenameCore = function() {
        $scope.hideAll();
        $scope.showRename = true;
      };

      $scope.renameCore = function() {
        if (!$scope.other) {
          $scope.renameMessage = "Please provide a new name for the " + $scope.selectedCore + " core";
        } else if ($scope.other == $scope.selectedCore) {
          $scope.renameMessage = "New name must be different from the current one";
        } else {
          Cores.rename({core:$scope.selectedCore, other: $scope.other}, function(data) {
            $location.path("/~cores/" + $scope.other);
            $scope.cancelRename();
          });
        }
      };

      $scope.cancelRenameCore = function() {
        $scope.showRename = false;
        delete $scope.renameMessage;
        $scope.other = "";
      };

      $scope.showSwapCores = function() {
        $scope.hideAll();
        $scope.showSwap = true;
      };

      $scope.swapCores = function() {
        if (!$scope.swapOther) {
          $scope.swapMessage = "Please select a core to swap with";
        } else if ($scope.swapOther == $scope.selectedCore) {
          $scope.swapMessage = "Cannot swap with the same core";
        } else {
          Cores.swap({core: $scope.selectedCore, other: $scope.swapOther}, function(data) {
            $location.path("/~cores/" + $scope.swapOther);
            delete $scope.swapOther;
            $scope.cancelSwapCores();
          });
        }
      };

      $scope.cancelSwapCores = function() {
        delete $scope.swapMessage;
        $scope.showSwap = false;
      }

      $scope.reloadCore = function() {
        if ($scope.initFailures[$scope.selectedCore]) {
          delete $scope.initFailures[$scope.selectedCore];
          $scope.showInitFailures = Object.keys(data.initFailures).length>0;
        }
        Cores.reload({core: $scope.selectedCore},
          function(data) {
            if (data.error) {
              $scope.reloadFailure = true;
              $timeout(function() {
                $scope.reloadFailure = false;
                $route.reload();
              }, 1000);
            } else {
              $scope.reloadSuccess = true;
              $timeout(function () {
                $scope.reloadSuccess = false;
                $route.reload();
              }, 1000);
            }
          });
      };

      $scope.hideAll = function() {
        $scope.showRename = false;
        $scope.showAdd = false;
        $scope.showSwap = false;
      };

      $scope.optimizeCore = function() {
        Update.optimize({core: $scope.selectedCore},
          function(successData) {
            $scope.optimizeSuccess = true;
            $timeout(function() {$scope.optimizeSuccess=false}, 1000);
            $scope.refresh();
          },
          function(failureData) {
            $scope.optimizeFailure = true;
            $timeout(function () {$scope.optimizeFailure=false}, 1000);
            $scope.refresh();
          });
      };

      $scope.refresh();
    }
);

/**************
  'cores_load_data',
  function( event, params )
  {
    $.ajax
    (
      {
        url : app.config.solr_path + app.config.core_admin_path + '?wt=json',
        dataType : 'json',
        success : function( response, text_status, xhr )
        {
          if( params.only_failures )
          {
            app.check_for_init_failures( response );
            return true;
          }


=========== NO CORES
        error : function()
        {
          sammy.trigger
          (
            'cores_load_template',
            {
              content_element : content_element,
              callback : function()
              {
                var cores_element = $( '#cores', content_element );
                var navigation_element = $( '#navigation', cores_element );
                var data_element = $( '#data', cores_element );
                var core_data_element = $( '#core-data', data_element );
                var index_data_element = $( '#index-data', data_element );

                // layout

                var ui_block = $( '#ui-block' );
                var actions_element = $( '.actions', cores_element );
                var div_action = $( 'div.action', actions_element );

                ui_block
                  .css( 'opacity', 0.7 )
                  .width( cores_element.width() + 10 )
                  .height( cores_element.height() );

                if( $( '#cloud.global' ).is( ':visible' ) )
                {
                  $( '.cloud', div_action )
                    .show();
                }

                $( 'button.action', actions_element )
                  .die( 'click' )
                  .live
                  (
                    'click',
                    function( event )
                    {
                      var self = $( this );

                      self
                        .toggleClass( 'open' );

                      $( '.action.' + self.attr( 'id' ), actions_element )
                        .trigger( 'open' );

                      return false;
                    }
                  );

                div_action
                  .die( 'close' )
                  .live
                  (
                    'close',
                    function( event )
                    {
                      div_action.hide();
                      ui_block.hide();
                    }
                  )
                  .die( 'open' )
                  .live
                  (
                    'open',
                    function( event )
                    {
                      var self = $( this );
                      var rel = $( '#' + self.data( 'rel' ) );

                      self
                        .trigger( 'close' )
                        .show()
                        .css( 'left', rel.position().left );

                      ui_block
                        .show();
                    }
                  );

                $( 'form button.reset', actions_element )
                  .die( 'click' )
                  .live
                  (
                    'click',
                    function( event )
                    {
                      $( this ).closest( 'div.action' )
                        .trigger( 'close' );
                    }
                  );

                $( 'form', div_action )
                  .ajaxForm
                  (
                    {
                      url : app.config.solr_path + app.config.core_admin_path + '?wt=json&indexInfo=false',
                      dataType : 'json',
                      beforeSubmit : function( array, form, options )
                      {
                        $( 'button[type="submit"] span', form )
                          .addClass( 'loader' );
                      },
                      success : function( response, status_text, xhr, form )
                      {
                        delete app.cores_data;
                        sammy.refresh();

                        $( 'button.reset', form )
                          .trigger( 'click' );
                      },
                      error : function( xhr, text_status, error_thrown )
                      {
                        var response = null;
                        eval( 'response = ' + xhr.responseText + ';' );

                        var error_elem = $( '.error', div_action.filter( ':visible' ) );
                        error_elem.show();
                        $( 'span', error_elem ).text( response.error.msg );
                      },
                      complete : function()
                      {
                        $( 'button span.loader', actions_element )
                          .removeClass( 'loader' );
                      }
                    }
                  );

                // --

                $( '#add', content_element )
                  .trigger( 'click' );

                $( '[data-rel="add"] input[type="text"]:first', content_element )
                  .focus();
              }
            }
          );
        }
      }
    );
  }
);

// #/~cores
sammy.get
(
  /^#\/(~cores)\//,
  function( context )
  {
    var content_element = $( '#content' );

    var path_parts = this.path.match( /^(.+\/~cores\/)(.*)$/ );
    var current_core = path_parts[2];

    sammy.trigger
    (
      'cores_load_data',
      {
        error : function()
        {
          context.redirect( '#/' + context.params.splat[0] );
        },
        success : function( cores )
        {
          sammy.trigger
          (
            'cores_load_template',
            {
              content_element : content_element,
              callback : function()
              {
                var cores_element = $( '#cores', content_element );
                var navigation_element = $( '#navigation', cores_element );
                var data_element = $( '#data', cores_element );
                var core_data_element = $( '#core-data', data_element );
                var index_data_element = $( '#index-data', data_element );

                cores_element
                  .removeClass( 'empty' );

                var core_data = cores[current_core];
                var core_basepath = $( '#' + current_core, app.menu_element ).attr( 'data-basepath' );

                var core_names = [];
                var core_selects = $( '#actions select', cores_element );

                $( 'option[value="' + current_core + '"]', core_selects.filter( '.other' ) )
                  .remove();

                $( 'input[data-core="current"]', cores_element )
                  .val( current_core );

                // layout

                var ui_block = $( '#ui-block' );
                var actions_element = $( '.actions', cores_element );
                var div_action = $( 'div.action', actions_element );

                ui_block
                  .css( 'opacity', 0.7 )
                  .width( cores_element.width() + 10 )
                  .height( cores_element.height() );

                if( $( '#cloud.global' ).is( ':visible' ) )
                {
                  $( '.cloud', div_action )
                    .show();
                }

                var form_callback = {

                  rename : function( form, response )
                  {
                    var url = path_parts[1] + $( 'input[name="other"]', form ).val();
                    context.redirect( url );
                  }

                };

                $( 'form', div_action )
                  .ajaxForm
                  (
                    {
                      url : app.config.solr_path + app.config.core_admin_path + '?wt=json&indexInfo=false',
                      success : function( response, status_text, xhr, form )
                      {
                        var action = $( 'input[name="action"]', form ).val().toLowerCase();

                        delete app.cores_data;

                        if( form_callback[action] )
                        {
                         form_callback[action]( form, response );
                        }
                        else
                        {
                          sammy.refresh();
                        }

                        $( 'button.reset', form )
                          .trigger( 'click' );
                      },
                  );

                $( '#actions #unload', cores_element )
                      var ret = confirm( 'Do you really want to unload Core "' + current_core + '"?' );
                      if( !ret )
                        return false;

                          url : app.config.solr_path + app.config.core_admin_path + '?wt=json&action=UNLOAD&core=' + current_core,
                          success : function( response, text_status, xhr )
                          {
                            delete app.cores_data;
                            context.redirect( path_parts[1].substr( 0, path_parts[1].length - 1 ) );
                          },

                optimize_button
                          url : core_basepath + '/update?optimize=true&waitFlush=true&wt=json',
                          success : function( response, text_status, xhr )

******/
