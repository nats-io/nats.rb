module NATS
  class NUID
    DIGITS = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz'
    BASE          = 62
    PREFIX_LENGTH = 12
    SEQ_LENGTH    = 10
    TOTAL_LENGTH  = PREFIX_LENGTH + SEQ_LENGTH
    MAX_SEQ       = BASE**10
    MIN_INC       = 33
    MAX_INC       = 333
    INC = MAX_INC - MIN_INC

    def initialize
      @prand    = Random.new
      @seq      = @prand.rand(MAX_SEQ)
      @inc      = MIN_INC + @prand.rand(INC)
      @prefix   = ''
      randomize_prefix!
    end

    def next
      @seq += @inc
      if @seq >= MAX_SEQ
        randomize_prefix!
        reset_sequential!
      end

      l = @seq
      suffix = SEQ_LENGTH.times.reduce('') do |b|
        b.prepend DIGITS[l % BASE]
        l /= BASE
        b
      end

      "#{@prefix}#{suffix}"
    end

    def randomize_prefix!
      @prefix = \
      SecureRandom.random_bytes(PREFIX_LENGTH).each_byte
        .reduce('') do |prefix, n|
        prefix << DIGITS[n % BASE]
      end
    end

    private

    def reset_sequential!
      @seq = prand.rand(MAX_SEQ)
      @inc = MIN_INC + @prand.rand(INC)
    end

    class << self
      def next
        @nuid ||= NUID.new.extend(MonitorMixin)
        @nuid.synchronize do
          @nuid.next
        end
      end
    end
  end
end
