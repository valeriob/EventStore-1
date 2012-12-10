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

using System;
using System.Linq;
using EventStore.Core.Messages;
using EventStore.Projections.Core.Services.Processing;
using EventStore.Projections.Core.Tests.Services.core_projection;
using NUnit.Framework;

namespace EventStore.Projections.Core.Tests.Services.partitioned_state_reader
{
    [TestFixture]
    public class when_starting : TestFixtureWithExistingEvents
    {
        private CheckpointTag _atPosition;
        private string _projectionName;
        private PartitionedStateReader _psr;
        private Exception _exception;
        private string _catalogStream;
        private ProjectionNamesBuilder _projectionNamesBuilder;
        private Guid _requestCorrelationId;

        protected override void Given()
        {
            base.Given();
            _atPosition = CheckpointTag.FromPosition(100, 50);
            _projectionName = "projection";
            _requestCorrelationId = Guid.NewGuid();
            _projectionNamesBuilder = new ProjectionNamesBuilder();
            _catalogStream = _projectionNamesBuilder.GetPartitionCatalogStreamName(_projectionName);
            _psr = new PartitionedStateReader(
                _bus, _requestCorrelationId, _readDispatcher, _atPosition, _projectionNamesBuilder, _projectionName);
        }

        [SetUp]
        public void When()
        {
            _consumer.HandledMessages.Clear();
            try
            {
                _psr.Start();
            }
            catch (Exception ex)
            {
                _exception = ex;
            }
        }

        [Test]
        public void it_can_be_started()
        {
            Assert.IsNull(_exception);
        }

        [Test]
        public void starts_reading_catalog_stream()
        {
            Assert.AreEqual(
                1,
                _consumer.HandledMessages.OfType<ClientMessage.ReadStreamEventsBackward>()
                         .Count(m => m.EventStreamId == _catalogStream));
        }
    }
}
