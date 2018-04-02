# Copyright 2016-2018 The NATS Authors
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
require 'securerandom'

module NATS
  class NUID
    DIGITS = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz'.split('')
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

      # Do this inline 10 times to avoid even more extra allocs,
      # then use string interpolation of everything which works
      # faster for doing concat.
      s_10 = DIGITS[l % BASE];

      # Ugly, but parallel assignment is slightly faster here...
      s_09, s_08, s_07, s_06, s_05, s_04, s_03, s_02, s_01 = \
      (l /= BASE; DIGITS[l % BASE]), (l /= BASE; DIGITS[l % BASE]), (l /= BASE; DIGITS[l % BASE]),\
      (l /= BASE; DIGITS[l % BASE]), (l /= BASE; DIGITS[l % BASE]), (l /= BASE; DIGITS[l % BASE]),\
      (l /= BASE; DIGITS[l % BASE]), (l /= BASE; DIGITS[l % BASE]), (l /= BASE; DIGITS[l % BASE])
      "#{@prefix}#{s_01}#{s_02}#{s_03}#{s_04}#{s_05}#{s_06}#{s_07}#{s_08}#{s_09}#{s_10}"
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
      @@nuid = NUID.new.extend(MonitorMixin)
      def next
        @@nuid.synchronize do
          @@nuid.next
        end
      end
    end
  end
end
