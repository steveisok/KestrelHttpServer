// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Buffers;
using System.IO;
using System.IO.Pipelines;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Connections;
using Microsoft.AspNetCore.Server.Kestrel.Core.Internal.Http;
using Microsoft.AspNetCore.Server.Kestrel.Core.Internal.Infrastructure;
using Microsoft.Net.Http.Headers;

namespace Microsoft.AspNetCore.Server.Kestrel.Core.Internal.Http2
{
    public partial class Http2Stream : HttpProtocol
    {
        private readonly Http2StreamContext _context;
        private readonly Http2OutputProducer _http2Output;
        private readonly Http2InputFlowControl _inputFlowControl;
        private readonly Http2StreamOutputFlowControl _outputFlowControl;

        private long _totalBytesReceived;
        private long _totalBytesReadByApp;

        private int _requestAborted;

        public Http2Stream(Http2StreamContext context)
            : base(context)
        {
            _context = context;

            _inputFlowControl = new Http2InputFlowControl(_context.FrameWriter, _context.StreamId, (int)Http2PeerSettings.DefaultInitialWindowSize / 2);
            _outputFlowControl = new Http2StreamOutputFlowControl(context.ConnectionOutputFlowControl, context.ClientPeerSettings.InitialWindowSize);
            _http2Output = new Http2OutputProducer(context.StreamId, context.FrameWriter, _outputFlowControl, context.TimeoutControl, context.MemoryPool);
            Output = _http2Output;
        }

        public int StreamId => _context.StreamId;

        public bool RequestBodyStarted { get; private set; }
        public bool EndStreamReceived { get; private set; }

        public override bool IsUpgradableRequest => false;

        protected override void OnReset()
        {
            ResetIHttp2StreamIdFeature();
        }

        protected override void OnRequestProcessingEnded()
        {
            _context.StreamLifetimeHandler.OnStreamCompleted(StreamId);
        }

        protected override string CreateRequestId()
            => StringUtilities.ConcatAsHexSuffix(ConnectionId, ':', (uint)StreamId);

        protected override MessageBody CreateMessageBody()
            => Http2MessageBody.For(HttpRequestHeaders, this);

        protected override bool TryParseRequest(ReadResult result, out bool endConnection)
        {
            // We don't need any of the parameters because we don't implement BeginRead to actually
            // do the reading from a pipeline, nor do we use endConnection to report connection-level errors.

            _httpVersion = Http.HttpVersion.Http2;
            var methodText = RequestHeaders[HeaderNames.Method];
            Method = HttpUtilities.GetKnownMethod(methodText);
            _methodText = methodText;
            if (!string.Equals(RequestHeaders[HeaderNames.Scheme], Scheme, StringComparison.OrdinalIgnoreCase))
            {
                BadHttpRequestException.Throw(RequestRejectionReason.InvalidRequestLine);
            }

            var path = RequestHeaders[HeaderNames.Path].ToString();
            var queryIndex = path.IndexOf('?');

            Path = queryIndex == -1 ? path : path.Substring(0, queryIndex);
            QueryString = queryIndex == -1 ? string.Empty : path.Substring(queryIndex);
            RawTarget = path;

            // https://tools.ietf.org/html/rfc7230#section-5.4
            // A server MUST respond with a 400 (Bad Request) status code to any
            // HTTP/1.1 request message that lacks a Host header field and to any
            // request message that contains more than one Host header field or a
            // Host header field with an invalid field-value.

            var authority = RequestHeaders[HeaderNames.Authority];
            var host = HttpRequestHeaders.HeaderHost;
            if (authority.Count > 0)
            {
                // https://tools.ietf.org/html/rfc7540#section-8.1.2.3
                // An intermediary that converts an HTTP/2 request to HTTP/1.1 MUST
                // create a Host header field if one is not present in a request by
                // copying the value of the ":authority" pseudo - header field.
                //
                // We take this one step further, we don't want mismatched :authority
                // and Host headers, replace Host if :authority is defined.
                HttpRequestHeaders.HeaderHost = authority;
                host = authority;
            }

            // TODO: OPTIONS * requests?
            // To ensure that the HTTP / 1.1 request line can be reproduced
            // accurately, this pseudo - header field MUST be omitted when
            // translating from an HTTP/ 1.1 request that has a request target in
            // origin or asterisk form(see[RFC7230], Section 5.3).
            // https://tools.ietf.org/html/rfc7230#section-5.3

            if (host.Count <= 0)
            {
                BadHttpRequestException.Throw(RequestRejectionReason.MissingHostHeader);
            }
            else if (host.Count > 1)
            {
                BadHttpRequestException.Throw(RequestRejectionReason.MultipleHostHeaders);
            }

            var hostText = host.ToString();
            HttpUtilities.ValidateHostHeader(hostText);

            endConnection = false;
            return true;
        }

        public Task OnDataAsync(Http2Frame dataFrame)
        {
            // TODO: content-length accounting

            var payload = dataFrame.DataPayload;
            var endStream = (dataFrame.DataFlags & Http2DataFrameFlags.END_STREAM) == Http2DataFrameFlags.END_STREAM;
            var flushTask = default(ValueTask<FlushResult>);

            // TODO: Abort connection if _totalBytesReceived - _totalBytesReadByApp > Http2PeerSettings.DefaultInitialWindowSize.
            _totalBytesReceived += dataFrame.Length;

            // Since padding isn't buffered, immediately count padding bytes as read for flow control purposes.
            if (dataFrame.DataHasPadding)
            {
                // Add 1 byte for the padding length prefix.
                OnDataReadByApp(dataFrame.DataPadLength + 1);
            }

            if (payload.Count > 0)
            {
                RequestBodyPipe.Writer.Write(payload);

                RequestBodyStarted = true;
                flushTask = RequestBodyPipe.Writer.FlushAsync();
            }

            if (flushTask.IsCompleted)
            {
                if (endStream)
                {
                    OnEndStream();
                }

                return Task.CompletedTask;
            }

            return OnDataAsyncAwaited(flushTask, endStream);
        }

        private async Task OnDataAsyncAwaited(ValueTask<FlushResult> flushTask, bool endStream)
        {
            await flushTask;

            if (endStream)
            {
                OnEndStream();
            }
        }

        public void OnEndStream()
        {
            EndStreamReceived = true;
            RequestBodyPipe.Writer.Complete();
        }

        public void OnDataReadByApp(int bytesRead)
        {
            _totalBytesReadByApp += bytesRead;

            _inputFlowControl.OnDataRead(bytesRead);

            // TODO: This is bad, and I feel bad.
            lock (_context.ConnectionInputFlowControl)
            {
                _context.ConnectionInputFlowControl.OnDataRead(bytesRead);
            }
        }

        public bool TryUpdateOutputWindow(int bytes)
        {
            return _context.FrameWriter.TryUpdateStreamWindow(_outputFlowControl, bytes);
        }

        public override void Abort(ConnectionAbortedException abortReason)
        {
            if (Interlocked.Exchange(ref _requestAborted, 1) != 0)
            {
                return;
            }

            AbortCore(abortReason);
        }

        protected override void ApplicationAbort()
        {
            Log.ApplicationAbortedConnection(ConnectionId, TraceIdentifier);
            var abortReason = new ConnectionAbortedException(CoreStrings.ConnectionAbortedByApplication);
            ResetAndAbort(abortReason, Http2ErrorCode.CANCEL);
        }

        private void ResetAndAbort(ConnectionAbortedException abortReason, Http2ErrorCode error)
        {
            if (Interlocked.Exchange(ref _requestAborted, 1) != 0)
            {
                return;
            }

            // Don't block on IO. This never faults.
            _ = _http2Output.WriteRstStreamAsync(error);

            AbortCore(abortReason);
        }

        private void AbortCore(ConnectionAbortedException abortReason)
        {
            base.Abort(abortReason);

            // Unblock the request body.
            RequestBodyPipe.Writer.Complete(new IOException(CoreStrings.Http2StreamAborted, abortReason));

            // Count all data for the stream as read for the purposes of connection-level flow control.
            if (_totalBytesReceived > _totalBytesReadByApp)
            {
                _context.ConnectionInputFlowControl.OnDataRead((int)(_totalBytesReceived - _totalBytesReadByApp));
            }
        }
    }
}
