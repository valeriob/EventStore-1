// Copyright (c) 2012, Event Store LLP
// All rights reserved.
// 
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
// 
// Redistributions of source code must retain the above copyright notice,
// this list of conditions and the following disclaimer.
// Redistributions in binary form must reproduce the above copyright
// notice, this list of conditions and the following disclaimer in the
// documentation and/or other materials provided with the distribution.
// Neither the name of the Event Store LLP nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
// 

using System.Linq;
using EventStore.Projections.Core.Services.Processing;
using NUnit.Framework;

namespace EventStore.Projections.Core.Tests.Services.partitioned_state_reader
{
    [TestFixture]
    public class when_reading_partition_state_stream : TestFixtureWithPartitionedStateReader
    {
        private string _partitionStateData;
        private string _partitionName;

        protected override void Given()
        {
            base.Given();

            _partitionName = "account-001";
            _partitionStateData = "{\"Count\":123}";

            ExistingEvent(_catalogStream, "PartitionCreated", null, _partitionName);
            ExistingEvent(
                string.Format("$projections-{0}-{1}-state", _projectionName, _partitionName), "StateUpdate",
                @"{""CommitPosition"":100, ""PreparePosition"":50}", _partitionStateData);
        }

        [Test]
        public void publishes_partitioned_state_begin_message()
        {
            Assert.AreEqual(1, _consumer.HandledMessages.OfType<PartitionedStateBegin>().Count());
        }

        [Test]
        public void publishes_partitioned_state_part_message()
        {
            Assert.AreEqual(1, _consumer.HandledMessages.OfType<PartitionedStatePart>().Count());
        }

        [Test]
        public void partitioned_state_part_message_holds_correct_data()
        {
            Assert.AreEqual(
                1, _consumer.HandledMessages.OfType<PartitionedStatePart>().Count(m => m.Data == _partitionStateData));
        }
    }
}
