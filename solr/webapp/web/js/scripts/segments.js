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


var get_tooltip = function( segment_response ) {
    var tooltip =
        '<div>Segment <b>' + segment_response.name + '</b>:</div>' +
        '<div class="label">#docs:</div><div>' + number_format(segment_response.size) +'</div>' +
        '<div class="label">#dels:</div><div>' + number_format(segment_response.delCount) + '</div>' +
        '<div class="label">size:</div><div>' + number_format(segment_response.sizeInBytes) + ' bytes </div>' +
        '<div class="label">age:</div><div>' + segment_response.age + '</div>' +
        '<div class="label">source:</div><div>' + segment_response.source + '</div>';
    return tooltip;
};

var get_entry = function( segment_response, segment_bytes_max ) {
    //calcualte dimensions of graph
    var dims = calculate_dimensions(segment_response.sizeInBytes, 
            segment_bytes_max, segment_response.size, segment_response.delCount)
    //create entry for segment with given dimensions
    var entry = get_entry_item(segment_response.name, dims, 
            get_tooltip(segment_response), (segment_response.mergeCandidate)?true:false);

    return entry;
};

var get_entry_item = function(name, dims, tooltip, isMergeCandidate) {
    var entry = '<li>' + "\n" +
    '  <dl class="clearfix" style="width: ' + dims['size'] + '%;">' + "\n" +
    '    <dt><div>' + name + '</div></dt>' + "\n" +
    '    <dd>';
    entry += '<div class="live' + ((isMergeCandidate)?' merge-candidate':'') + 
         '" style="width: ' + dims['alive_doc_size'] + '%;">&nbsp;</div>';
    entry += '<div class="toolitp">' + tooltip +'</div>';
      
    if (dims['deleted_doc_size'] > 0.001) {
     entry += '<div class="deleted" style="width:' + dims['deleted_doc_size']  
         + '%;margin-left:' + dims['alive_doc_size'] + '%;">&nbsp;</div>';
    }
    entry += '</dd></dl></li>';
    return entry;
};

var get_footer = function(deletions_count, documents_count) {
    return '<li><dl><dt></dt><dd>Deletions: ' + 
        (documents_count == 0 ? 0 : round_2(deletions_count/documents_count * 100)) +
            '% </dd></dl></li>';
};

var calculate_dimensions = function(segment_size_in_bytes, segment_size_in_bytes_max, doc_count, delete_doc_count) {
    var segment_size_in_bytes_log = Math.log(segment_size_in_bytes);
    var segment_size_in_bytes_max_log = Math.log(segment_size_in_bytes_max);

    var dims = {};
    //Normalize to 100% size of bar
    dims['size'] = Math.floor((segment_size_in_bytes_log / segment_size_in_bytes_max_log ) * 100);
    //Deleted doc part size
    dims['deleted_doc_size'] = Math.floor((delete_doc_count/(delete_doc_count + doc_count)) * dims['size']);
    //Alive doc part size
    dims['alive_doc_size'] = dims['size'] - dims['deleted_doc_size'];

    return dims;
};

var calculate_max_size_on_disk = function(segment_entries) {
    var max = 0;
    $.each(segment_entries, function(idx, obj) {
        if (obj.sizeInBytes > max) {
            max = obj.sizeInBytes;
        }
    });
    return max;
};

var round_2 = function(num) {
    return Math.round(num*100)/100;
};

var number_format = function(x) {
    return x.toString().replace(/\B(?=(\d{3})+(?!\d))/g, " ");
};

var prepare_x_axis = function(segment_bytes_max) {
    var factor = 1024*1024; //for MB
    
    var segment_bytes_max_log = Math.log(segment_bytes_max);
    
    var series_entry = '<li>' + "\n" +
    '  <dl class="clearfix" style="width:100%;">' + "\n" +
    '    <dt><div>Size</div></dt>' + "\n" +
    '    <dd>' + 
    '        <div class="start">0</div>';
    var step = 0;
    for (var j = 0; j < 3; j+=1) {
            step += segment_bytes_max_log/4;
            var step_value = number_format(Math.floor((Math.pow(Math.E, step))/factor))
            series_entry += '<div class="w5">' + ((step_value > 0.001)?step_value : '&nbsp;')  + '</div>'
    }
    series_entry += '<div class="end">' + number_format(Math.floor(segment_bytes_max/factor)) + ' MB </div>' +
    '    </dd>' +
    '  </dl>' +
    '</li>';
    return series_entry;
};

// #/:core/admin/segments
sammy.get
(
  new RegExp( app.core_regex_base + '\\/(segments)$' ),
  function( context )
  {
    var core_basepath = this.active_core.attr( 'data-basepath' );
    var content_element = $( '#content' );
        
    $.get
    (
      'tpl/segments.html',
      function( template )
      {
        content_element.html( template );
            
        var segments_element = $('#segments', content_element);
        var segments_reload = $( '#segments a.reload' );
        var url_element = $('#url', segments_element);
        var result_element = $('#result', segments_element);
        var response_element = $('#response', result_element);
        var segments_holder_element = $('.segments-holder', result_element);

        segments_reload
            .die( 'click' )
            .live
            (
            'click',
            function( event )
            {
                $.ajax
                (
                  {
                    url : core_basepath +  '/admin/segments?wt=json',
                    dataType : 'json',
                    context: this,
                    beforeSend : function( arr, form, options )
                    {
                      loader.show( this );    
                    },
                    success : function( response, text_status, xhr )
                    {
                        var segments_response = response['segments'],
                            segments_entries = [],
                            segment_bytes_max = calculate_max_size_on_disk( segments_response );

                        //scale
                        segments_entries.push( prepare_x_axis( segment_bytes_max ) );
                        
                        var documents_count = 0, deletions_count = 0;
                        
                        //elements
                        $.each( segments_response, function( key, segment_response ) {
                            segments_entries.push( get_entry( segment_response, segment_bytes_max ) );
                            
                            documents_count += segment_response.size;
                           deletions_count += segment_response.delCount;
                        });
     
                        //footer
                        segments_entries.push( get_footer( deletions_count, documents_count ) );
                        
                        $( 'ul', segments_holder_element ).html( segments_entries.join("\n" ) );
                    },
                    error : function( xhr, text_status, error_thrown )
                    {
                      $( this )
                        .attr( 'title', '/admin/segments is not configured (' + xhr.status + ': ' + error_thrown + ')' );

                      $( this ).parents( 'li' )
                        .addClass( 'error' );
                    },
                    complete : function( xhr, text_status )
                    {
                      loader.hide( this );
                    }
                  }
                );
              return false;
            }
          );
        //initially submit
        segments_reload.click();
      }
    );
  }
);