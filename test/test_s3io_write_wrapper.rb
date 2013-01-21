require 'test/unit'
require 's3io'
require 'stringio'

class S3ObjectWriteMock
  attr_reader :body
  attr_accessor :uploaded
  attr_accessor :multipart_upload_options

  class MultiPartUploadMock
    attr_reader :options

    def initialize(body, options, mock)
      @body = body
      @mock = mock
      @mock.multipart_upload_options = options
    end

    def add_part(part)
      @body << part
    end

    def close
      @mock.uploaded = @body
    end
  end

  def initialize
    @body = ''
  end

  def multipart_upload(options = {})
    MultiPartUploadMock.new(@body, options, self)
  end
end

class S3ioWriteWrapperTest < Test::Unit::TestCase
  def setup
    @s3object = S3ObjectWriteMock.new
  end

  def test_full_write
    wrapper = S3io::WriteWrapper.new(@s3object)

    test_string = 'This is a test.'

    wrapper.write(test_string)
    assert_equal(test_string.size, wrapper.pos)
    assert_equal({}, @s3object.multipart_upload_options)

    wrapper.close
    assert_equal(test_string, @s3object.uploaded)
    assert_equal(0, wrapper.pos)
  end

  def test_multipart_write
    wrapper = S3io::WriteWrapper.new(@s3object, :max_file_size => 1)

    chunk1 = 'z' * (S3io::WriteWrapper::MIN_CHUNK_SIZE + 100)
    chunk2 = 'y' * (S3io::WriteWrapper::MIN_CHUNK_SIZE + 300)

    full_body = chunk1 + chunk2

    wrapper.write(chunk1)
    assert_equal(chunk1, @s3object.body)
    assert_equal(nil, @s3object.uploaded)

    wrapper.write(chunk2)
    assert_equal(full_body, @s3object.body)
    assert_equal(nil, @s3object.uploaded)

    wrapper.close
    assert_equal(full_body, @s3object.uploaded)
  end

  def test_flush
    wrapper = S3io::WriteWrapper.new(@s3object, :multipart_upload_options => {:metadata => {:yes => 'no'}})
    test_string = '42'

    assert_equal('', @s3object.body)
    assert_equal({:metadata => {:yes => 'no'}}, @s3object.multipart_upload_options)

    wrapper.write(test_string)
    wrapper.flush
    assert_equal(test_string, @s3object.body)

    wrapper.close
    assert_equal(test_string, @s3object.body)
    assert_equal(test_string, @s3object.uploaded)
end
end
