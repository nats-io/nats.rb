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

require 'spec_helper'

describe 'NUID' do
  it "should have a fixed length and be unique" do
    nuid = NATS::NUID.new
    entries = []
    total = 500_000
    total.times do
      entry = nuid.next
      expect(entry.size).to eql(NATS::NUID::TOTAL_LENGTH)
      entries << entry
    end
    entries.uniq!
    expect(entries.count).to eql(total)
  end

  it "should be unique after 1M entries" do
    total = 1_000_000
    entries = []
    nuid = NATS::NUID.new
    total.times do
      entries << nuid.next
    end
    entries.uniq!
    expect(entries.count).to eql(total)
  end

  it "should randomize the prefix after sequence is done" do
    nuid = NATS::NUID.new
    seq_a = nuid.instance_variable_get('@seq')
    inc_a = nuid.instance_variable_get('@inc')
    a = nuid.next

    seq_b = nuid.instance_variable_get('@seq')
    inc_b = nuid.instance_variable_get('@inc')
    expect(seq_a < seq_b).to eql(true)
    expect(seq_b).to eql(seq_a + inc_a)
    b = nuid.next

    nuid.instance_variable_set('@seq', NATS::NUID::MAX_SEQ+1)
    c = nuid.next
    l = NATS::NUID::PREFIX_LENGTH
    expect(a[0..l]).to eql(b[0..l])
    expect(a[0..l]).to_not eql(c[0..l])
  end
end
