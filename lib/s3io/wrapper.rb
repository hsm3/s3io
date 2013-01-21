module S3io

  def self.open(s3object, mode_string = 'r', options = {}, &block)
    wrapper_class = case mode_string
    when 'r'
      ReadWrapper
    when 'w'
      WriteWrapper
    else
      fail "S3IO only supports 'r' or 'w' as access modes"
    end

    wrapper = wrapper_class.new(s3object, options)

    if block_given?
      result = yield wrapper if block_given?
      wrapper.close

      return result
    else
      return wrapper
    end
  end

  # This class wraps an AWS S3 object in order to provide IO-like API.
  # S3 objects wrapped this way can be used in methods that would otherwise expect an instance of File, StringIO etc.
  class Wrapper

    # Wraps an AWS::S3::S3Object into IO-like object.
    #
    # @param [AWS::S3::S3Object] s3object an object to wrap
    def initialize(s3object)
      @s3object = s3object
      @pos = 0
    end

    def close
      @s3object = nil
      @pos = 0

      return nil
    end
  end
end
