require File.dirname(__FILE__) + '/../test_helper'
require 'sparklines_controller'

# Re-raise errors caught by the controller.
class SparklinesController; def rescue_action(e) raise e end; end

class SparklinesControllerTest < Test::Unit::TestCase

  #fixtures :data

  def setup
    @controller = SparklinesController.new
    @request    = ActionController::TestRequest.new
    @response   = ActionController::TestResponse.new
  end

  def test_index
    get :index, :results => "1,2,3,4,5", :type => 'bar', :line_color => 'black'
    assert_response :success
    assert_equal 'image/png', @response.headers['Content-Type']
  end

  # TODO Replace this with your actual tests
  def test_show
    get :show
    assert_response :success
    assert_equal 'image/png', @response.headers['Content-Type']
  end
  
end
