/*
* Copyright (c) 2011 Jordan Feldstein

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

// Original code from: https://github.com/jfeldstein/jQuery.AjaxFileUpload.js https://github.com/jfeldstein/jQuery.AjaxFileUpload.js/commit/9dd56b4161cbed138287d3ae29a476bb59eb5fc4
// All modifications are BSD licensed
// GSI: Modifications made to support immediate upload
/*
 //
 //  - Ajaxifies an individual <input type="file">
 //  - Files are sandboxed. Doesn't matter how many, or where they are, on the page.
 //  - Allows for extra parameters to be included with the file
 //  - onStart callback can cancel the upload by returning false
 */


(function ($) {
  $.fn.ajaxfileupload = function (options) {
    var settings = {
      params: {},
      action: '',
      onStart: function () {
        console.log('starting upload');
        console.log(this);
      },
      onComplete: function (response) {
        console.log('got response: ');
        console.log(response);
        console.log(this);
      },
      onCancel: function () {
        console.log('cancelling: ');
        console.log(this);
      },
      validate_extensions: true,
      valid_extensions: ['gif', 'png', 'jpg', 'jpeg'],
      submit_button: null,
      upload_now: false
    };

    var uploading_file = false;

    if (options) {
      $.extend(settings, options);
    }


    // 'this' is a jQuery collection of one or more (hopefully)
    //  file elements, but doesn't check for this yet
    return this.each(function () {
      var $element = $(this);
      /*
       // Internal handler that tries to parse the response
       //  and clean up after ourselves.
       */
      var handleResponse = function (loadedFrame, element) {
        var response, responseStr = loadedFrame.contentWindow.document.body.innerHTML;
        try {
          //response = $.parseJSON($.trim(responseStr));
          response = JSON.parse(responseStr);
        } catch (e) {
          response = responseStr;
        }

        // Tear-down the wrapper form
        element.siblings().remove();
        element.unwrap();

        uploading_file = false;

        // Pass back to the user
        settings.onComplete.apply(element, [response, settings.params]);
      };
      /*
       // Wraps element in a <form> tag, and inserts hidden inputs for each
       //  key:value pair in settings.params so they can be sent along with
       //  the upload. Then, creates an iframe that the whole thing is
       //  uploaded through.
       */
      var wrapElement = function (element) {
        // Create an iframe to submit through, using a semi-unique ID
        var frame_id = 'ajaxUploader-iframe-' + Math.round(new Date().getTime() / 1000)
        $('body').after('<iframe width="0" height="0" style="display:none;" name="' + frame_id + '" id="' + frame_id + '"/>');
        $('#' + frame_id).load(function () {
          handleResponse(this, element);
        });
        console.log("settings.action: " + settings.action);
        // Wrap it in a form
        element.wrap(function () {
          return '<form action="' + settings.action + '" method="POST" enctype="multipart/form-data" target="' + frame_id + '" />'
        })
          // Insert <input type='hidden'>'s for each param
            .before(function () {
              var key, html = '';
              for (key in settings.params) {
                var paramVal = settings.params[key];
                if (typeof paramVal === 'function') {
                  paramVal = paramVal();
                }
                html += '<input type="hidden" name="' + key + '" value="' + paramVal + '" />';
              }
              return html;
            });
      }

      var upload_file = function () {
        if ($element.val() == '') return settings.onCancel.apply($element, [settings.params]);

        // make sure extension is valid
        var ext = $element.val().split('.').pop().toLowerCase();
        if (true === settings.validate_extensions && $.inArray(ext, settings.valid_extensions) == -1) {
          // Pass back to the user
          settings.onComplete.apply($element, [
            {status: false, message: 'The select file type is invalid. File must be ' + settings.valid_extensions.join(', ') + '.'},
            settings.params
          ]);
        } else {
          uploading_file = true;

          // Creates the form, extra inputs and iframe used to
          //  submit / upload the file
          wrapElement($element);

          // Call user-supplied (or default) onStart(), setting
          //  it's this context to the file DOM element
          var ret = settings.onStart.apply($element, [settings.params]);

          // let onStart have the option to cancel the upload
          if (ret !== false) {
            $element.parent('form').submit(function (e) {
              e.stopPropagation();
            }).submit();
          }
        }
      };
      if (settings.upload_now) {
        if (!uploading_file) {
          console.log("uploading now");
          upload_file();
        }
      }
      // Skip elements that are already setup. May replace this
      //  with uninit() later, to allow updating that settings
      if ($element.data('ajaxUploader-setup') === true) return;

      /*
      $element.change(function () {
        // since a new image was selected, reset the marker
        uploading_file = false;

        // only update the file from here if we haven't assigned a submit button
        if (settings.submit_button == null) {
          console.log("uploading");
          upload_file();
        }
      });
      //*/

      if (settings.submit_button == null) {
        // do nothing
      } else {
        settings.submit_button.click(function () {
          console.log("uploading: " + uploading_file);
          // only attempt to upload file if we're not uploading
          if (!uploading_file) {
            upload_file();
          }
        });
      }


      // Mark this element as setup
      $element.data('ajaxUploader-setup', true);


    });
  }
})(jQuery)