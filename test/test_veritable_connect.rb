# encoding: utf-8

require 'helper'

class Class
  def publicize_methods
    saved_private_instance_methods = self.private_instance_methods
    self.class_eval { public *saved_private_instance_methods }
    yield
    self.class_eval { private *saved_private_instance_methods }
  end
end

class VeritableConnectTest < Test::Unit::TestCase
  def test_connect
    a = Veritable.connect
    assert a.is_a? Veritable::API
    assert a.to_s.is_a? String
  end

  def test_connect_unauthorized_fails
    assert_raise(Veritable::VeritableError) { Veritable.connect({:api_key => "foo"}) }
  end

  def test_connect_nogzip
    a = Veritable.connect(opts={:enable_gzip => false})
    assert a.is_a? Veritable::API
  end

  def test_connect_no_ssl_verify
    a = Veritable.connect(opts={:ssl_verify => false})
    assert a.is_a? Veritable::API
  end

  def test_connect_no_json_failes
    assert_raise(Veritable::VeritableError) { Veritable.connect({:api_base_url => "http://www.google.com"}) }
  end

  def test_connect_not_api_fails
    assert_raise(Veritable::VeritableError) { Veritable.connect({:api_base_url => "https://graph.facebook.com/zuck"}) }
  end

  def test_instantiate_api
    a1 = Veritable.connect
    a1.class.publicize_methods do
        a2 = Veritable::API.new({:api_key =>a1.api_key, :api_base_url =>a1.api_base_url})
        assert a2.is_a? Veritable::API
    end
  end
end
