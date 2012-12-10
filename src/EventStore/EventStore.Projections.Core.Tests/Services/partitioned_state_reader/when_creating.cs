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
using EventStore.Core.Messages;
using EventStore.Core.Messaging;
using EventStore.Core.Tests.Fakes;
using EventStore.Projections.Core.Services;
using EventStore.Projections.Core.Services.Processing;
using NUnit.Framework;

namespace EventStore.Projections.Core.Tests.Services.partitioned_state_reader
{
    [TestFixture]
    public class when_creating
    {
        private
            RequestResponseDispatcher
                <ClientMessage.ReadStreamEventsBackward, ClientMessage.ReadStreamEventsBackwardCompleted>
            _requestResponseDispatcher;

        private string _projectionName;
        private ProjectionNamesBuilder _projectionNamesBuilder;
        private FakePublisher _bus;

        [SetUp]
        public void setup()
        {
            _projectionName = "projection";
            _bus = new FakePublisher();
            _requestResponseDispatcher =
                new RequestResponseDispatcher
                    <ClientMessage.ReadStreamEventsBackward, ClientMessage.ReadStreamEventsBackwardCompleted>(
                    _bus, v => v.CorrelationId, v => v.CorrelationId, new NoopEnvelope());
            _projectionNamesBuilder = new ProjectionNamesBuilder();
        }

        [Test]
        public void can_be_created()
        {
            var psr = new PartitionedStateReader(
                _bus, _requestResponseDispatcher, CheckpointTag.FromPosition(100, 50), _projectionNamesBuilder,
                _projectionName);
        }

        [Test, ExpectedException(typeof (ArgumentNullException))]
        public void null_publisher_throws_argument_null_exception()
        {
            var psr = new PartitionedStateReader(
                null, _requestResponseDispatcher, CheckpointTag.FromPosition(100, 50), _projectionNamesBuilder,
                _projectionName);
        }

        [Test, ExpectedException(typeof (ArgumentNullException))]
        public void null_dispatcher_throws_argument_null_exception()
        {
            var psr = new PartitionedStateReader(
                _bus, null, CheckpointTag.FromPosition(100, 50), _projectionNamesBuilder, _projectionName);
        }

        [Test, ExpectedException(typeof (ArgumentNullException))]
        public void null_at_position_throws_argument_null_exception()
        {
            var psr = new PartitionedStateReader(
                _bus, _requestResponseDispatcher, null, _projectionNamesBuilder, _projectionName);
        }

        [Test, ExpectedException(typeof (ArgumentNullException))]
        public void null_projections_name_builder_throws_argument_null_exception()
        {
            var psr = new PartitionedStateReader(
                _bus, _requestResponseDispatcher, CheckpointTag.FromPosition(100, 50), null, _projectionName);
        }

        [Test, ExpectedException(typeof (ArgumentNullException))]
        public void null_projection_name_throws_argument_null_exception()
        {
            var psr = new PartitionedStateReader(
                _bus, _requestResponseDispatcher, CheckpointTag.FromPosition(100, 50), _projectionNamesBuilder, null);
        }

        [Test, ExpectedException(typeof (ArgumentException))]
        public void empty_projection_name_throws_argument_exception()
        {
            var psr = new PartitionedStateReader(
                _bus, _requestResponseDispatcher, CheckpointTag.FromPosition(100, 50), _projectionNamesBuilder, "");
        }
    }
}
