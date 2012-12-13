require 'test/unit'
require 's3io'
require 'stringio'

# Emulate S3Object in a way that allows us to check if its methods are being called properly
class S3ObjectMock
  def initialize(body = '')
    @body = body
  end

  def read(options = {})
    range = options[:range]
    fail "The mock should be called with a :range option" unless range

    return @body[range]
  end

  def content_length
    @body.size
  end
end

class S3ioWrapperTest < Test::Unit::TestCase
  S3_TEST_DATA = File.read('test/s3_test_data.csv')

  def setup
    @s3object = S3ObjectMock.new(S3_TEST_DATA)
  end

  def test_s3io_wrapper_full_read
    wrapper = S3io::Wrapper.new(@s3object)

    assert_equal(S3_TEST_DATA, wrapper.read)
  end

  def test_s3io_wrapper_zero_read
    wrapper = S3io::Wrapper.new(@s3object)

    assert_equal('', wrapper.read(0))
    assert_equal(0, wrapper.pos)
  end

  def test_s3io_wrapper_partial_read
    wrapper = S3io::Wrapper.new(@s3object)

    assert_equal(S3_TEST_DATA[0..99], wrapper.read(100))
    assert_equal(S3_TEST_DATA[100..100], wrapper.read(1))
  end

  def test_s3io_wrapper_each
    wrapper = S3io::Wrapper.new(@s3object)

    lines = []
    wrapper.each do |line|
      lines << line
    end

    assert_equal(S3_TEST_DATA.lines.to_a, lines)
  end

  def test_s3io_wrapper_each_enum
    wrapper = S3io::Wrapper.new(@s3object)

    assert_equal(S3_TEST_DATA.lines.to_a,
                 wrapper.lines.to_a)
  end

  def test_s3io_wrapper_pos
    wrapper = S3io::Wrapper.new(@s3object)

    assert_equal(0, wrapper.pos)

    wrapper.pos = 77

    assert_equal(77, wrapper.pos)
    assert_equal(S3_TEST_DATA[77, 32], wrapper.read(32))
  end

  def test_s3io_wrapper_pos_beyond
    wrapper = S3io::Wrapper.new(@s3object)

    pos_beyond = @s3object.content_length + 100
    wrapper.pos = pos_beyond

    assert_equal('', wrapper.read)
    assert_equal(pos_beyond, wrapper.pos)
  end

  def test_s3io_wrapper_rewind
    wrapper = S3io::Wrapper.new(@s3object)

    wrapper.lines do |line|
      # iterate through all the lines
    end

    wrapper.rewind
    assert_equal(0, wrapper.pos)

    assert_equal(S3_TEST_DATA[0..100], wrapper.read(101))
  end

  def test_s3io_wrapper_line_buffer_size
    wrapper = S3io::Wrapper.new(@s3object, :line_buffer_size => 25)

    wrapper.lines.each_with_index do |line, index|
      break if index == 1 # skip two buffered reads
    end

    assert_equal(50, wrapper.pos)
    assert_equal(S3_TEST_DATA[50..-1], wrapper.read)
  end

  def test_s3io_wrapper_lines_custom_separator
    wrapper = S3io::Wrapper.new(@s3object)

    assert_equal(S3_TEST_DATA.lines(",").to_a, wrapper.lines(",").to_a)
  end

  def test_s3io_wrapper_empty_each
    wrapper = S3io::Wrapper.new(S3ObjectMock.new(''))

    wrapper.each do |line|
      assert_equal(false, :never_gets_called)
    end

    assert_equal('', wrapper.read)
  end
end
