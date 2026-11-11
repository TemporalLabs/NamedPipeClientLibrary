using System;
using System.Buffers.Binary;
using System.IO;
using System.IO.Pipes;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace NamedPipeClientLibrary
{
    /// <summary>
    /// IPC client for the ClientAgentWorker agent. Speaks one wire protocol over two transports:
    /// Windows named pipes, and Unix domain sockets on macOS and Linux.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Wire protocol (unchanged, and identical on both transports):</b> a 4-byte little-endian
    /// length prefix followed by that many bytes of UTF-8. This matches
    /// <c>GlobalNamedPipeServer</c> on Windows and <c>LinuxUnixSocketServer</c> on Unix byte for
    /// byte, so the transport swap needs no protocol change.
    /// </para>
    /// <para>
    /// <b>Why a Unix socket rather than .NET's pipe emulation.</b> .NET does implement
    /// <see cref="NamedPipeClientStream"/> on Unix, on top of Unix domain sockets -- but it uses
    /// its own path convention (<c>CoreFxPipe_&lt;name&gt;</c> under the temp directory), whereas
    /// the agent's Unix server binds a configurable path of its own. The two never meet, so on
    /// Unix this client connects to that path directly.
    /// </para>
    /// <para>
    /// The public API is unchanged. <c>NotifyTray</c> consumes this assembly as a loose binary via
    /// <c>HintPath</c>, so the class name, constructor, methods and events are a binary contract.
    /// The added static factories are purely additive.
    /// </para>
    /// </remarks>
    public class NamedPipeClient : IDisposable
    {
        /// <summary>Windows pipe name the agent hosts.</summary>
        public const string DefaultPipeName = "ClientAgentPipe";

        /// <summary>
        /// Unix socket path the agent binds.
        /// </summary>
        /// <remarks>
        /// Deliberately matches <c>LinuxUnixSocketServer.DefaultSocketPath</c> as it exists today.
        /// The macOS plan (§11 decision #4) recommends moving both to <c>/var/run/</c> because
        /// <c>/tmp</c> is world-writable and lets an unprivileged process pre-create the path --
        /// but changing only this side would break the connection, so it tracks the server until
        /// that decision lands.
        /// </remarks>
        /// <remarks>
        /// <c>static readonly</c>, not <c>const</c>: a <c>const</c> is inlined into consumers at
        /// compile time, so when decision #4 moves this to <c>/var/run/</c> a freshly compiled
        /// consumer would use the new path while an older binary kept the old one -- and they
        /// would silently fail to meet, with no build error anywhere.
        /// </remarks>
        public static readonly string DefaultUnixSocketPath = "/tmp/clientagentworker.sock";

        private readonly string _clientId;
        private readonly string _endpoint;
        private readonly bool _useUnixSocket;
        private readonly CancellationTokenSource _cts;

        // Null until ConnectAsync succeeds. IsConnected and Dispose must tolerate that -- the
        // previous version built the pipe in the constructor, so callers could always poll
        // IsConnected safely, and that has to keep working.
        private Stream? _stream;
        private Socket? _socket;

        // One frame on the wire at a time. Without this, two callers awaiting SendMessageAsync
        // concurrently (NotifyTray's location timer and a status request, say) could interleave
        // their length and body writes. The agent then read the tail of a LocationUpdate --
        // the bytes 00"} -- as a length of 2,099,392,560 and allocated 2 GB (2026-10-06).
        private readonly SemaphoreSlim _writeLock = new(1, 1);

        // Ceiling on what the agent may send us, mirroring the agent's own IpcMaxMessageBytes
        // default. Real traffic is a few KB; this only rejects an out-of-frame stream.
        private const int MaxMessageBytes = 16 * 1024 * 1024;
        private const int ReadChunkSize = 64 * 1024;

        // 0 = not yet signalled. OnDisconnected used to be raised from up to four places for a
        // single disconnect; this makes it fire exactly once.
        private int _disconnectSignalled;

        public Action<string>? OnMessageReceived { get; set; }
        public event Action? OnDisconnected;
        public event Action? OnConnected;

        // Expose the CancellationToken so it can be accessed from outside
        public CancellationToken CancellationToken => _cts.Token;

        /// <summary>True once connected and while the underlying transport still believes it is.</summary>
        /// <remarks>
        /// Both transports report a cached state that only refreshes on I/O, so a connection
        /// dropped by the peer can read as connected until the next read or write. That is equally
        /// true of the named-pipe implementation this replaces -- it is parity, not a regression.
        /// </remarks>
        public bool IsConnected
        {
            get
            {
                if (_cts.IsCancellationRequested)
                {
                    return false;
                }

                return _useUnixSocket
                    ? _socket?.Connected ?? false
                    : (_stream as NamedPipeClientStream)?.IsConnected ?? false;
            }
        }

        /// <summary>
        /// Creates a client for the local agent, choosing the transport from the running OS:
        /// a named pipe on Windows, a Unix domain socket everywhere else.
        /// </summary>
        public NamedPipeClient(string? clientId = null)
            : this(clientId,
                   OperatingSystem.IsWindows() ? DefaultPipeName : DefaultUnixSocketPath,
                   !OperatingSystem.IsWindows())
        {
        }

        private NamedPipeClient(string? clientId, string endpoint, bool useUnixSocket)
        {
            _clientId = clientId ?? $"Client-{Guid.NewGuid()}";
            _endpoint = endpoint;
            _useUnixSocket = useUnixSocket;
            _cts = new CancellationTokenSource();
        }

        /// <summary>Creates a client bound to an explicit Unix domain socket path.</summary>
        /// <remarks>
        /// Also usable on Windows 10 1803+ and Windows 11, which support <c>AF_UNIX</c> -- which is
        /// how the Unix transport is exercised without a Mac. See <c>NamedPipeClientLibrary.Probe</c>.
        /// </remarks>
        public static NamedPipeClient ForUnixSocket(string socketPath, string? clientId = null)
        {
            if (string.IsNullOrWhiteSpace(socketPath))
            {
                throw new ArgumentException("Socket path must not be empty.", nameof(socketPath));
            }

            return new NamedPipeClient(clientId, socketPath, useUnixSocket: true);
        }

        /// <summary>Creates a client bound to an explicit Windows named pipe.</summary>
        public static NamedPipeClient ForNamedPipe(string pipeName, string? clientId = null)
        {
            if (string.IsNullOrWhiteSpace(pipeName))
            {
                throw new ArgumentException("Pipe name must not be empty.", nameof(pipeName));
            }

            return new NamedPipeClient(clientId, pipeName, useUnixSocket: false);
        }

        public async Task ConnectAsync()
        {
            Console.WriteLine($"[Client] Connecting as '{_clientId}'...");

            if (_useUnixSocket)
            {
                var socket = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
                try
                {
                    await socket.ConnectAsync(new UnixDomainSocketEndPoint(_endpoint), _cts.Token);
                }
                catch
                {
                    socket.Dispose();
                    throw;
                }

                _socket = socket;
                _stream = new NetworkStream(socket, ownsSocket: true);
            }
            else
            {
                var pipe = new NamedPipeClientStream(".", _endpoint, PipeDirection.InOut, PipeOptions.Asynchronous);
                try
                {
                    await pipe.ConnectAsync(_cts.Token);
                }
                catch
                {
                    pipe.Dispose();
                    throw;
                }

                _stream = pipe;
            }

            Console.WriteLine("[Client] Connected to server.");

            OnConnected?.Invoke();

            // Send clientId to server after connection
            await SendMessageAsync(_clientId, _cts.Token);

            // Unexplained in the original and kept deliberately: it looks like a workaround for a
            // server-side race on the identity handshake. Removing it is not safe without being
            // able to reproduce whatever it was papering over.
            await Task.Delay(100);

            Console.WriteLine($"[Client] Sent ID: {_clientId}");
        }

        public async Task SendMessageAsync(string message, CancellationToken token)
        {
            Stream stream = _stream
                ?? throw new InvalidOperationException("Not connected. Call ConnectAsync first.");

            // Length prefix and body in ONE buffer, written once. Explicit little-endian rather
            // than BitConverter: identical bytes on x64 and ARM64, but it states the wire format.
            int bodyLength = Encoding.UTF8.GetByteCount(message);
            var frame = new byte[4 + bodyLength];
            BinaryPrimitives.WriteInt32LittleEndian(frame, bodyLength);
            Encoding.UTF8.GetBytes(message, 0, message.Length, frame, 4);

            // Cancelled while waiting: nothing was written, the stream is still in frame.
            await _writeLock.WaitAsync(token);
            try
            {
                await stream.WriteAsync(frame.AsMemory(), token);
                await stream.FlushAsync(token);
            }
            catch
            {
                // A failed or cancelled write may have left part of a frame on the wire, and every
                // later message would then be mis-read by the agent. Close the connection so the
                // caller reconnects cleanly instead of writing into a broken stream.
                try { stream.Dispose(); } catch { }
                SignalDisconnected();
                throw;
            }
            finally
            {
                _writeLock.Release();
            }
        }

        public async Task<string?> ReadMessageAsync(CancellationToken token)
        {
            Stream stream = _stream
                ?? throw new InvalidOperationException("Not connected. Call ConnectAsync first.");

            // A single read may legitimately return fewer than 4 bytes; keep reading.
            byte[] lengthBuffer = new byte[4];
            int read = 0;
            while (read < 4)
            {
                int n = await stream.ReadAsync(lengthBuffer.AsMemory(read, 4 - read), token);
                if (n == 0)
                {
                    SignalDisconnected();
                    return null;
                }
                read += n;
            }

            int length = BinaryPrimitives.ReadInt32LittleEndian(lengthBuffer);
            if (length <= 0 || length > MaxMessageBytes)
            {
                // Out of frame (or absurdly large). There is no way to find the next boundary,
                // so treat it as a disconnect rather than trusting the number.
                Console.WriteLine($"[Client Error] Frame claims {length:N0} bytes (limit {MaxMessageBytes:N0}); disconnecting.");
                SignalDisconnected();
                return null;
            }

            // Grow with the bytes that actually arrive, not with what the header claims.
            using var body = new MemoryStream(Math.Min(length, ReadChunkSize));
            byte[] chunk = new byte[Math.Min(length, ReadChunkSize)];
            int remaining = length;

            while (remaining > 0)
            {
                int bytesRead = await stream.ReadAsync(chunk.AsMemory(0, Math.Min(remaining, chunk.Length)), token);
                if (bytesRead == 0)
                {
                    // Peer closed mid-message. The original fell through and decoded the whole
                    // buffer, so a truncated message surfaced as text padded with NUL characters.
                    // A partial frame is a disconnect, not a message.
                    SignalDisconnected();
                    return null;
                }

                body.Write(chunk, 0, bytesRead);
                remaining -= bytesRead;
            }

            return Encoding.UTF8.GetString(body.GetBuffer(), 0, (int)body.Length);
        }

        public async Task ListenForMessagesAsync()
        {
            try
            {
                while (!_cts.Token.IsCancellationRequested)
                {
                    string? message = await ReadMessageAsync(_cts.Token);
                    if (message == null)
                    {
                        SignalDisconnected();
                        break;
                    }

                    OnMessageReceived?.Invoke(message);

                    Console.WriteLine($"[Client Received] {message}");
                }
            }
            catch (Exception ex) when (IsDisconnectionException(ex))
            {
                Console.WriteLine($"[Client Error] {ex.Message}");
                SignalDisconnected();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[Client Error] {ex.Message}");
            }
        }

        // Added helper method for disconnection detection
        private bool IsDisconnectionException(Exception ex)
        {
            return ex is IOException ||
                   ex is SocketException ||
                   ex is OperationCanceledException ||
                   ex is ObjectDisposedException;
        }

        /// <summary>Raises <see cref="OnDisconnected"/> at most once for the life of the client.</summary>
        private void SignalDisconnected()
        {
            if (Interlocked.Exchange(ref _disconnectSignalled, 1) == 0)
            {
                OnDisconnected?.Invoke();
            }
        }

        public void Dispose()
        {
            try
            {
                _cts.Cancel();
                SignalDisconnected();

                // NetworkStream owns the socket (ownsSocket: true), so disposing the stream closes
                // it. The fields are deliberately NOT nulled: a listen loop still in flight will
                // re-enter ReadMessageAsync, and a disposed-but-present stream throws
                // ObjectDisposedException -- which IsDisconnectionException already classifies as
                // a clean disconnect. Nulling them instead produces an InvalidOperationException
                // that falls through to the generic handler and logs an error on normal shutdown.
                // IsConnected is unaffected: it short-circuits on _cts.IsCancellationRequested,
                // already true here, so it never touches the disposed socket.
                _stream?.Dispose();

                _cts.Dispose();
            }
            catch { }
        }
    }
}
