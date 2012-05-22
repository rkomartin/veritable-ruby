require 'test/unit'
require 'veritable'

class VeritableTest < Test::Unit::TestCase
  def test_connect
  end

  def test_connect_unauthorized_fails
    assert_raise(RestClient::Unauthorized) { Veritable.connect "foo"}
  end

  def test_connect_explicit
  end

  def test_connect_nogzip
  end

  def test_connect_ssl_verify_fails
  end

  def test_connect_not_api_fails
  end

  def test_instantiate_veritable_resource
  end

  def test_instantiate_api
  end
  
  def test_api_root
    api = Veritable.connect
    api.root
  end

  def test_api_limits
    api = Veritable.connect
    api.limits
  end

  def test_api_list_tables
    api = Veritable.connect
    api.tables
  end
end

class VeritableTestConnection < Test::Unit::TestCase
  def test_instantiate
  end
end