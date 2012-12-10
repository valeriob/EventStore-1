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
using EventStore.Core.Bus;
using EventStore.Core.Messages;
using EventStore.Core.Messaging;
using EventStore.Transport.Http;
using EventStore.Transport.Http.EntityManagement;

namespace EventStore.Core.Services.Transport.Http
{
    public class ChunkedSendToHttpEnvelope<TExpectedBeginResponseMessage, TExpectedResponsePartMessage,
                                           TExpectedEndResponseMessage> : IEnvelope
        where TExpectedBeginResponseMessage : Message
        where TExpectedResponsePartMessage : Message
        where TExpectedEndResponseMessage : Message
    {
        private enum State
        {
            Initial,
            Open,
            Body,
            Closed
        }

        private readonly IPublisher _networkSendQueue;
        private readonly HttpEntity _entity;
        private readonly Func<ICodec, TExpectedResponsePartMessage, string> _formatter;
        private readonly Func<ICodec, TExpectedBeginResponseMessage, ResponseConfiguration> _configurator;
        private State _state;

        public ChunkedSendToHttpEnvelope(
            IPublisher networkSendQueue, HttpEntity entity, Func<ICodec, TExpectedResponsePartMessage, string> formatter,
            Func<ICodec, TExpectedBeginResponseMessage, ResponseConfiguration> configurator)
        {
            if (networkSendQueue == null) throw new ArgumentNullException("networkSendQueue");
            if (entity == null) throw new ArgumentNullException("entity");
            if (formatter == null) throw new ArgumentNullException("formatter");
            if (configurator == null) throw new ArgumentNullException("configurator");
            _networkSendQueue = networkSendQueue;
            _entity = entity;
            _formatter = formatter;
            _configurator = configurator;
        }

        public void ReplyWith<T>(T message) where T : Message
        {
            if (message == null) throw new ArgumentNullException("message");

            try
            {

                switch (_state)
                {
                    case State.Initial:
                        var m = message as TExpectedBeginResponseMessage;
                        var responseConfiguration = _configurator(_entity.ResponseCodec, m);
                        _networkSendQueue.Publish(
                            new HttpMessage.HttpBeginSend(Guid.Empty, null, _entity.Manager, responseConfiguration));
                        _networkSendQueue.Publish(
                            new HttpMessage.HttpSendPart(
                                Guid.Empty, null, _entity.Manager, _entity.ResponseCodec.BeginChunked()));
                        _state = State.Open;
                        break;
                    case State.Open:
                    case State.Body:
                        var part = message as TExpectedResponsePartMessage;
                        if (part != null)
                        {
                            var data = (_state == State.Body ? _entity.ResponseCodec.ChunkSeparator() : "")
                                       + _formatter(_entity.ResponseCodec, part);
                            _networkSendQueue.Publish(
                                new HttpMessage.HttpSendPart(Guid.Empty, null, _entity.Manager, data));
                            _state = State.Body;
                        }
                        else
                        {
                            var end = message as TExpectedEndResponseMessage;
                            if (end == null)
                                throw new InvalidOperationException("Unexpected message received: " + message.GetType());
                            _networkSendQueue.Publish(
                                new HttpMessage.HttpSendPart(
                                    Guid.Empty, null, _entity.Manager, _entity.ResponseCodec.EndChunk()));
                            _networkSendQueue.Publish(new HttpMessage.HttpEndSend(Guid.Empty, null, _entity.Manager));
                            _state = State.Body;
                        }
                        break;
                    default:
                        throw new Exception();
                }
            }
            catch (Exception ex)
            {
                switch (_state)
                {
                    case State.Initial:
                        _networkSendQueue.Publish(
                            new HttpMessage.HttpSend(
                                _entity.Manager, new ResponseConfiguration(HttpStatusCode.InternalServerError, "Internal Server Error", null), "", message));
                        break;
                    default:
                        _networkSendQueue.Publish(new HttpMessage.HttpDropSend(Guid.Empty, null, _entity.Manager));
                        break;
                }
            }
        }
    }
}
