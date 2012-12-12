require 'test/unit'
require 's3io'

class S3ioTest < Test::Unit::TestCase
  def test_s3io_new
    s3object = Object.new
    wrapper = S3io.new(s3object)
    assert_equal(S3io::Wrapper, wrapper.class)
  end

  def test_s3io_new_with_options
    s3object = Object.new
    wrapper = S3io.new(s3object, :line_buffer_size => 128)
    assert_equal(128, wrapper.options[:line_buffer_size])
  end
end
