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
using System.Text;
using EventStore.Core.Bus;
using EventStore.Core.Messages;
using EventStore.Core.Messaging;
using EventStore.Core.Services.Storage.ReaderIndex;

namespace EventStore.Projections.Core.Services.Processing
{
    public class PartitionedStateReaderMessage : Message
    {
    }

    public class PausePartitionedStateReader : PartitionedStateReaderMessage
    {
    }

    public class ResumePartitionedStateReader : PartitionedStateReaderMessage
    {
    }

    public class PartitionedStateBegin : PartitionedStateReaderMessage
    {
    }

    public class PartitionedStatePart : PartitionedStateReaderMessage
    {
        public readonly string Data;

        public PartitionedStatePart(string data)
        {
            Data = data;
        }
    }

    public class PartitionedStateEnd : PartitionedStateReaderMessage
    {
    }

    public class PartitionedStateReader : IHandle<PausePartitionedStateReader>, IHandle<ResumePartitionedStateReader>
    {
        private readonly IPublisher _publisher;
        private readonly CheckpointTag _atPosition;
        private readonly ProjectionNamesBuilder _namesBuilder;
        private readonly string _projectionName;

        private readonly
            RequestResponseDispatcher
                <ClientMessage.ReadStreamEventsBackward, ClientMessage.ReadStreamEventsBackwardCompleted>
            _readDispatcher;


        public class WorkItem : StagedTask
        {
            private Action<int> _complete;
            private int _onStage;
            private readonly int _lastStage;
            protected readonly PartitionedStateReader _reader;

            public WorkItem(object correlationId, PartitionedStateReader reader)
                : base(correlationId)
            {
                _reader = reader;
                _lastStage = 2;
            }

            protected void NextStage()
            {
                _complete(_onStage == _lastStage ? -1 : _onStage + 1);
            }

            public override void Process(int onStage, Action<int> readyForStage)
            {
                _complete = readyForStage;
                _onStage = onStage;
                switch (onStage)
                {
                    case 0:
                        RequestRead();
                        break;
                    case 1:
                        Send();
                        break;
                    case 2:
                        Complete();
                        break;
                    default:
                        throw new NotSupportedException();
                }
            }

            protected virtual void Complete()
            {
                NextStage();
            }

            protected virtual void Send()
            {
                NextStage();
            }

            protected virtual void RequestRead()
            {
                NextStage();
            }

            protected void CompleteStage()
            {
                NextStage();
                _reader.Process();
            }
        }

        private class ReadPartitionIndex : WorkItem
        {
            private readonly string _catalogStream;
            private readonly int _atPosition;

            private readonly
                RequestResponseDispatcher
                    <ClientMessage.ReadStreamEventsBackward, ClientMessage.ReadStreamEventsBackwardCompleted>
                _readDispatcher;


            public ReadPartitionIndex(
                object correlationId, PartitionedStateReader reader, string catalogStream, int atPosition)
                : base(correlationId, reader)
            {
                _atPosition = atPosition;
                _catalogStream = catalogStream;
                _readDispatcher = reader._readDispatcher;
            }

            protected override void RequestRead()
            {
                _readDispatcher.Publish(
                    new ClientMessage.ReadStreamEventsBackward(
                        Guid.NewGuid(), _readDispatcher.Envelope, _catalogStream, _atPosition, 10, false), ReadCompleted);
            }

            private void ReadCompleted(
                ClientMessage.ReadStreamEventsBackwardCompleted readStreamEventsBackwardCompleted)
            {
                if (readStreamEventsBackwardCompleted.Result == RangeReadResult.Success)
                {
                    foreach (var @event in readStreamEventsBackwardCompleted.Events)
                    {
                        var partitionName = Encoding.UTF8.GetString(@event.Event.Data);
                        _reader._queue.Enqueue(new ReadPartitionState(Guid.NewGuid(), _reader, partitionName, -1));
                    }
                    if (!readStreamEventsBackwardCompleted.IsEndOfStream)
                        _reader._queue.Enqueue(
                            new ReadPartitionIndex(
                                Guid.NewGuid(), _reader, _catalogStream,
                                readStreamEventsBackwardCompleted.NextEventNumber));
                    else
                        _reader._queue.Enqueue(new EndReadPartitionedState(Guid.NewGuid(), _reader));
                }
                CompleteStage();
            }
        }

        private class BeginReadPartitionedState : WorkItem
        {
            public BeginReadPartitionedState(object correlationId, PartitionedStateReader reader)
                : base(correlationId, reader)
            {
            }

            protected override void RequestRead()
            {
                _reader._queue.Enqueue(new ReadPartitionIndex(Guid.NewGuid(), _reader, _reader._catalogStream, -1));
                NextStage();
            }

            protected override void Send()
            {
                _reader._publisher.Publish(new PartitionedStateBegin());
                NextStage();
            }
        }

        private class EndReadPartitionedState : WorkItem
        {
            public EndReadPartitionedState(object correlationId, PartitionedStateReader reader)
                : base(correlationId, reader)
            {
            }

            protected override void Send()
            {
                _reader._publisher.Publish(new PartitionedStateEnd());
                NextStage();
            }
        }

        private class ReadPartitionState : WorkItem
        {
            private readonly string _partition;
            private readonly int _atPosition;

            private readonly
                RequestResponseDispatcher
                    <ClientMessage.ReadStreamEventsBackward, ClientMessage.ReadStreamEventsBackwardCompleted>
                _readDispatcher;

            private readonly string _partitionStateStream;
            private byte[] _state;


            public ReadPartitionState(
                object correlationId, PartitionedStateReader reader, string partition, int atPosition)
                : base(correlationId, reader)
            {
                _partition = partition;
                _atPosition = atPosition;
                _readDispatcher = reader._readDispatcher;
                _partitionStateStream = reader.MakeParitionStreamName(partition);
            }

            protected override void RequestRead()
            {
                RequestNext(-1);
            }

            private void ReadCompleted(
                ClientMessage.ReadStreamEventsBackwardCompleted readStreamEventsBackwardCompleted)
            {
                if (readStreamEventsBackwardCompleted.Result == RangeReadResult.Success)
                {
                    if (readStreamEventsBackwardCompleted.Events.Length == 1)
                    {
                        var @event = readStreamEventsBackwardCompleted.Events[0].Event;
                        var metadata = @event.Metadata.ParseJson<CheckpointTag>();
                        if (metadata > _reader._atPosition)
                        {
                            RequestNext(readStreamEventsBackwardCompleted.NextEventNumber);
                            return; // do not complete stage yet
                        }
                        _state = @event.Data;
                        // we found required state record
                        // complete stage
                    }
                    if (!readStreamEventsBackwardCompleted.IsEndOfStream)
                    {
                        RequestNext(readStreamEventsBackwardCompleted.NextEventNumber);
                        return; // do not complete stage yet
                    }
                }
                CompleteStage();
            }

            private void RequestNext(int nextEventNumber)
            {
                _readDispatcher.Publish(
                    new ClientMessage.ReadStreamEventsBackward(
                        Guid.NewGuid(), _readDispatcher.Envelope, _partitionStateStream, nextEventNumber, 1, false),
                    ReadCompleted);
            }

            protected override void Send()
            {
                if (_state != null)
                {
                    var stringState = Encoding.UTF8.GetString(_state);
                    _reader._publisher.Publish(new PartitionedStatePart(stringState));
                }
                NextStage();
            }
        }

        private string MakeParitionStreamName(string partitionName)
        {
            return string.Format(_partitionStreamNamePattern, partitionName);
        }

        private readonly StagedProcessingQueue _queue = new StagedProcessingQueue(new[] {false, true, false});
        private readonly string _catalogStream;
        private bool _paused;
        private readonly string _partitionStreamNamePattern;

        public PartitionedStateReader(
            IPublisher publisher,
            RequestResponseDispatcher
                <ClientMessage.ReadStreamEventsBackward, ClientMessage.ReadStreamEventsBackwardCompleted> readDispatcher,
            CheckpointTag atPosition, ProjectionNamesBuilder namesBuilder, string projectionName)
        {
            if (publisher == null) throw new ArgumentNullException("publisher");
            if (readDispatcher == null) throw new ArgumentNullException("readDispatcher");
            if (atPosition == null) throw new ArgumentNullException("atPosition");
            if (namesBuilder == null) throw new ArgumentNullException("namesBuilder");
            if (projectionName == null) throw new ArgumentNullException("projectionName");
            if (projectionName == "") throw new ArgumentException("projectionName");
            _publisher = publisher;
            _readDispatcher = readDispatcher;
            _atPosition = atPosition;
            _namesBuilder = namesBuilder;
            _projectionName = projectionName;
            _catalogStream = namesBuilder.GetPartitionCatalogStreamName(projectionName);
            _partitionStreamNamePattern = namesBuilder.GetStateStreamNamePattern(projectionName);
        }

        public void Start()
        {
            _queue.Enqueue(new BeginReadPartitionedState(Guid.NewGuid(), this));
            Process();
        }

        private void Process()
        {
            while (_queue.Process() > 0) ;
        }

        public void Handle(PausePartitionedStateReader message)
        {
            if (_paused)
                throw new InvalidOperationException("Paused");
            _paused = true;
        }

        public void Handle(ResumePartitionedStateReader message)
        {
            if (!_paused)
                throw new InvalidOperationException("Not paused");
            _paused = false;
            Process();
        }
    }
}
