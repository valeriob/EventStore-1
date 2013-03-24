﻿// Copyright (c) 2012, Event Store LLP
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
using System.Threading.Tasks;
using EventStore.ClientAPI.Messages;
using EventStore.ClientAPI.SystemData;

namespace EventStore.ClientAPI.ClientOperations
{
    internal class ReadAllEventsForwardOperation : TaskOperationBase<AllEventsSlice, ClientMessage.ReadAllEventsCompleted>
    {
        public override bool IsLongRunning { get { return false; } }

        private readonly Position _position;
        private readonly int _maxCount;
        private readonly bool _resolveLinkTos;

        public ReadAllEventsForwardOperation(ILogger log, TaskCompletionSource<AllEventsSlice> source, Position position, int maxCount, bool resolveLinkTos)
            : base(log, source, TcpCommand.ReadAllEventsForward, TcpCommand.ReadAllEventsForwardCompleted)
        {
            _position = position;
            _maxCount = maxCount;
            _resolveLinkTos = resolveLinkTos;
        }

        protected override object CreateRequestDto()
        {
            return new ClientMessage.ReadAllEvents(_position.CommitPosition, _position.PreparePosition, _maxCount, _resolveLinkTos);
        }

        protected override InspectionResult InspectResponse(ClientMessage.ReadAllEventsCompleted response)
        {
            Succeed();
            return new InspectionResult(InspectionDecision.EndOperation);
        }

        protected override AllEventsSlice TransformResponse(ClientMessage.ReadAllEventsCompleted response)
        {
            return new AllEventsSlice(ReadDirection.Forward,
                                      new Position(response.CommitPosition, response.PreparePosition),
                                      new Position(response.NextCommitPosition, response.NextPreparePosition),
                                      response.Events);
        }

        public override string ToString()
        {
            return string.Format("Position: {0}, MaxCount: {1}, ResolveLinkTos: {2}", _position, _maxCount, _resolveLinkTos);
        }
    }
}