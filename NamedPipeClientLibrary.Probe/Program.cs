using System.Buffers.Binary;
using System.Net.Sockets;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text;
using NamedPipeClientLibrary;

namespace NamedPipeClientLibrary.Probe;

/// <summary>
/// Test harness for <see cref="NamedPipeClient"/>. Stands up a loopback echo server that speaks
/// the agent's wire protocol, then drives the client against it.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why a loopback server rather than the real one.</b> The agent's <c>LinuxUnixSocketServer</c>
/// lives in <c>ClientAgentWorker</c>, which is still single-TFM <c>net8.0-windows</c> until the
/// port's Phase 0 lands. This harness replicates its framing exactly -- 4-byte little-endian
/// length, UTF-8 payload, decoded with the actual byte count -- so the client is tested against
/// the contract rather than against an implementation that cannot be built here yet.
/// </para>
/// <para>
/// <b>Why this runs on Windows.</b> Windows 10 1803+ and Windows 11 support <c>AF_UNIX</c>, so the
/// Unix-domain-socket transport can be exercised without a Mac. The same binary run on macOS
/// proves the platform, not just the code path.
/// </para>
/// </remarks>
internal static class Program
{
    private static int _failures;
    private static int _checks;

    private static async Task<int> Main(string[] args)
    {
        if (args.Any(a => a is "--help" or "-h" or "/?"))
        {
            Usage();
            return 0;
        }

        Header();

        await UnixSocketRoundTrip();
        await WireFormatIsExactlyFourByteLePlusUtf8();
        await PartialFrameIsTreatedAsDisconnect();
        await DisconnectRaisedExactlyOnce();
        await ListenLoopDeliversToCallback();
        await DisposeDuringActiveListen();
        NullSafeBeforeConnect();

        if (OperatingSystem.IsWindows() && !args.Contains("--no-pipe"))
        {
            await NamedPipeRoundTrip();
        }
        else
        {
            Console.WriteLine("\n--- Named pipe round trip -------------------------------------");
            Console.WriteLine("  SKIPPED (not Windows). On macOS the agent exposes a Unix socket,");
            Console.WriteLine("  so the named-pipe transport is intentionally not exercised.");
        }

        return Finish();
    }

    // ---------------------------------------------------------------- scenarios

    private static async Task UnixSocketRoundTrip()
    {
        Section("Unix domain socket round trip");

        string path = TempSocketPath();
        using var server = new EchoServer(path);
        server.Start();

        using var client = NamedPipeClient.ForUnixSocket(path, "probe-uds");

        bool connected = false;
        client.OnConnected += () => connected = true;

        await client.ConnectAsync();
        Check("OnConnected raised", connected);
        Check("IsConnected true after connect", client.IsConnected);

        // ConnectAsync sends the client id as its first message; the echo server bounces it back.
        string? echoedId = await client.ReadMessageAsync(client.CancellationToken);
        Check("client id delivered and echoed", echoedId == "probe-uds", $"got '{echoedId}'");

        await client.SendMessageAsync("hello agent", client.CancellationToken);
        string? echo = await client.ReadMessageAsync(client.CancellationToken);
        Check("message round-tripped", echo == "hello agent", $"got '{echo}'");

        const string unicode = "état ✓ 日本語 🔒";
        await client.SendMessageAsync(unicode, client.CancellationToken);
        string? back = await client.ReadMessageAsync(client.CancellationToken);
        Check("UTF-8 multi-byte survives framing", back == unicode, $"got '{back}'");
    }

    private static async Task WireFormatIsExactlyFourByteLePlusUtf8()
    {
        Section("Wire format");

        const string id = "probe-wire";

        string path = TempSocketPath();
        using var server = new EchoServer(path) { CaptureRaw = true };
        server.Start();

        using var client = NamedPipeClient.ForUnixSocket(path, id);
        await client.ConnectAsync();
        await client.SendMessageAsync("AB", client.CancellationToken);

        // Exactly the bytes both frames occupy: the id frame then the "AB" frame. Overstating this
        // by even one byte makes the wait time out silently, and the assertions below then pass
        // against whatever happened to arrive rather than against what was awaited.
        byte[] raw = await server.WaitForRawAsync(minBytes: 4 + id.Length + 4 + 2);

        int first = BinaryPrimitives.ReadInt32LittleEndian(raw.AsSpan(0, 4));
        Check("length prefix is 4-byte little-endian", first == id.Length, $"prefix said {first}");

        int offset = 4 + first;
        int second = BinaryPrimitives.ReadInt32LittleEndian(raw.AsSpan(offset, 4));
        string payload = Encoding.UTF8.GetString(raw, offset + 4, second);
        Check("second frame length correct", second == 2, $"got {second}");
        Check("payload is raw UTF-8, no delimiter", payload == "AB", $"got '{payload}'");
        Check("no trailing framing bytes", raw.Length == offset + 4 + 2, $"saw {raw.Length} bytes");
    }

    private static async Task PartialFrameIsTreatedAsDisconnect()
    {
        Section("Truncated frame handling");

        string path = TempSocketPath();
        // Announces 64 bytes, sends 4, then closes -- the classic partial-read case.
        using var server = new EchoServer(path) { TruncateAfterHandshake = true };
        server.Start();

        using var client = NamedPipeClient.ForUnixSocket(path, "probe-trunc");

        int disconnects = 0;
        client.OnDisconnected += () => Interlocked.Increment(ref disconnects);

        await client.ConnectAsync();

        string? result = await client.ReadMessageAsync(client.CancellationToken);
        Check("truncated frame returns null, not NUL-padded text", result is null,
              result is null ? "" : $"got '{result.Replace("\0", "\\0")}'");
        Check("truncated frame raises OnDisconnected", disconnects >= 1);
    }

    private static async Task DisconnectRaisedExactlyOnce()
    {
        Section("OnDisconnected fires exactly once");

        string path = TempSocketPath();
        using var server = new EchoServer(path) { CloseAfterHandshake = true };
        server.Start();

        var client = NamedPipeClient.ForUnixSocket(path, "probe-once");

        int disconnects = 0;
        client.OnDisconnected += () => Interlocked.Increment(ref disconnects);

        await client.ConnectAsync();
        await client.ListenForMessagesAsync();   // server closed: read fails, loop exits
        client.Dispose();                        // used to raise it again

        Check("raised exactly once across read + listen + dispose", disconnects == 1,
              $"raised {disconnects} times");
    }

    private static async Task ListenLoopDeliversToCallback()
    {
        Section("ListenForMessagesAsync delivers to OnMessageReceived");

        string path = TempSocketPath();
        using var server = new EchoServer(path) { PushAfterHandshake = "pushed-from-server" };
        server.Start();

        using var client = NamedPipeClient.ForUnixSocket(path, "probe-push");

        var received = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        // This is the path NotifyTray actually consumes messages through -- the direct
        // ReadMessageAsync calls in the other scenarios do not exercise it.
        client.OnMessageReceived = m => received.TrySetResult(m);

        await client.ConnectAsync();
        _ = client.ListenForMessagesAsync();

        Task done = await Task.WhenAny(received.Task, Task.Delay(TimeSpan.FromSeconds(10)));
        Check("OnMessageReceived invoked by the listen loop", done == received.Task, "timed out");

        if (done == received.Task)
        {
            Check("callback got the exact payload", received.Task.Result == "pushed-from-server",
                  $"got '{received.Task.Result}'");
        }
    }

    private static async Task DisposeDuringActiveListen()
    {
        Section("Dispose during an active listen");

        string path = TempSocketPath();
        using var server = new EchoServer(path) { SilentAfterHandshake = true };
        server.Start();

        var client = NamedPipeClient.ForUnixSocket(path, "probe-shutdown");

        int disconnects = 0;
        client.OnDisconnected += () => Interlocked.Increment(ref disconnects);

        await client.ConnectAsync();

        // Fire-and-forget listen then dispose: the shutdown shape NotifyTray uses.
        var listening = client.ListenForMessagesAsync();
        await Task.Delay(150);   // let the loop block inside ReadMessageAsync

        var log = new StringWriter();
        TextWriter previous = Console.Out;
        Console.SetOut(log);
        try
        {
            client.Dispose();
            await Task.WhenAny(listening, Task.Delay(TimeSpan.FromSeconds(5)));
        }
        finally
        {
            Console.SetOut(previous);
        }

        Check("listen loop exited", listening.IsCompleted);
        Check("disconnect signalled exactly once", disconnects == 1, $"raised {disconnects} times");

        // A disposed-but-present stream throws ObjectDisposedException, which is classified as a
        // disconnect. Nulling the field instead would surface InvalidOperationException here.
        string output = log.ToString();
        Check("clean shutdown logs no 'Not connected' error",
              !output.Contains("Not connected", StringComparison.Ordinal),
              output.Trim().Replace(Environment.NewLine, " | "));
    }

    private static void NullSafeBeforeConnect()
    {
        Section("Safe before ConnectAsync");

        // The old implementation built the pipe in the constructor, so callers could always poll
        // IsConnected. The transport is now created in ConnectAsync, and that must not regress.
        var client = new NamedPipeClient("probe-unconnected");

        try
        {
            Check("IsConnected returns false, does not throw", client.IsConnected == false);
        }
        catch (Exception ex)
        {
            Check("IsConnected returns false, does not throw", false, ex.GetType().Name);
        }

        try
        {
            client.Dispose();
            Check("Dispose before connect does not throw", true);
        }
        catch (Exception ex)
        {
            Check("Dispose before connect does not throw", false, ex.GetType().Name);
        }
    }

    private static async Task NamedPipeRoundTrip()
    {
        Section("Named pipe round trip (Windows transport unchanged)");

        string pipeName = $"npcl-probe-{Guid.NewGuid():N}";
        using var server = new PipeEchoServer(pipeName);
        server.Start();

        using var client = NamedPipeClient.ForNamedPipe(pipeName, "probe-pipe");
        await client.ConnectAsync();

        Check("IsConnected true over named pipe", client.IsConnected);

        string? echoedId = await client.ReadMessageAsync(client.CancellationToken);
        Check("client id echoed", echoedId == "probe-pipe", $"got '{echoedId}'");

        await client.SendMessageAsync("hello pipe", client.CancellationToken);
        string? echo = await client.ReadMessageAsync(client.CancellationToken);
        Check("message round-tripped", echo == "hello pipe", $"got '{echo}'");
    }

    // ---------------------------------------------------------------- servers

    /// <summary>Minimal AF_UNIX echo server speaking the agent's framing.</summary>
    private sealed class EchoServer : IDisposable
    {
        private readonly string _path;
        private readonly Socket _listener;
        private readonly CancellationTokenSource _cts = new();
        private readonly List<byte> _raw = new();
        private readonly TaskCompletionSource _rawReady = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _rawTarget = int.MaxValue;

        public bool CaptureRaw { get; init; }
        public bool TruncateAfterHandshake { get; init; }
        public bool CloseAfterHandshake { get; init; }

        /// <summary>Unsolicited frame pushed once the client id arrives, to drive the listen loop.</summary>
        public string? PushAfterHandshake { get; init; }

        /// <summary>Accept and then say nothing, so the client blocks in a read.</summary>
        public bool SilentAfterHandshake { get; init; }

        public EchoServer(string path)
        {
            _path = path;
            if (File.Exists(_path))
            {
                File.Delete(_path);
            }

            _listener = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
            _listener.Bind(new UnixDomainSocketEndPoint(_path));
            _listener.Listen(1);
        }

        public void Start() => _ = Task.Run(AcceptLoop);

        private async Task AcceptLoop()
        {
            try
            {
                Socket conn = await _listener.AcceptAsync(_cts.Token);
                using var stream = new NetworkStream(conn, ownsSocket: true);

                while (!_cts.IsCancellationRequested)
                {
                    byte[] lengthBuffer = new byte[4];
                    if (!await ReadExactly(stream, lengthBuffer, 4))
                    {
                        return;
                    }

                    int length = BinaryPrimitives.ReadInt32LittleEndian(lengthBuffer);
                    byte[] payload = new byte[length];
                    if (!await ReadExactly(stream, payload, length))
                    {
                        return;
                    }

                    if (CaptureRaw)
                    {
                        lock (_raw)
                        {
                            _raw.AddRange(lengthBuffer);
                            _raw.AddRange(payload);
                            if (_raw.Count >= _rawTarget)
                            {
                                _rawReady.TrySetResult();
                            }
                        }
                    }

                    if (CloseAfterHandshake)
                    {
                        return;   // disposing the stream closes the connection
                    }

                    if (SilentAfterHandshake)
                    {
                        // Hold the connection open and send nothing, so the client's listen loop
                        // parks inside ReadMessageAsync until it is disposed.
                        await Task.Delay(Timeout.Infinite, _cts.Token);
                        return;
                    }

                    if (PushAfterHandshake is not null)
                    {
                        byte[] push = Encoding.UTF8.GetBytes(PushAfterHandshake);
                        byte[] header = new byte[4];
                        BinaryPrimitives.WriteInt32LittleEndian(header, push.Length);
                        await stream.WriteAsync(header, _cts.Token);
                        await stream.WriteAsync(push, _cts.Token);
                        await stream.FlushAsync(_cts.Token);
                        continue;
                    }

                    if (TruncateAfterHandshake)
                    {
                        // Announce 64 bytes, send 4, then hang up.
                        byte[] header = new byte[4];
                        BinaryPrimitives.WriteInt32LittleEndian(header, 64);
                        await stream.WriteAsync(header, _cts.Token);
                        await stream.WriteAsync(new byte[] { 1, 2, 3, 4 }, _cts.Token);
                        await stream.FlushAsync(_cts.Token);
                        return;
                    }

                    // Echo the frame back verbatim.
                    await stream.WriteAsync(lengthBuffer, _cts.Token);
                    await stream.WriteAsync(payload, _cts.Token);
                    await stream.FlushAsync(_cts.Token);
                }
            }
            catch (OperationCanceledException) { }
            catch (SocketException) { }
            catch (IOException) { }
            catch (ObjectDisposedException) { }
        }

        private async Task<bool> ReadExactly(Stream stream, byte[] buffer, int count)
        {
            int total = 0;
            while (total < count)
            {
                int n = await stream.ReadAsync(buffer.AsMemory(total, count - total), _cts.Token);
                if (n == 0)
                {
                    return false;
                }

                total += n;
            }

            return true;
        }

        public async Task<byte[]> WaitForRawAsync(int minBytes)
        {
            lock (_raw)
            {
                _rawTarget = minBytes;
                if (_raw.Count >= minBytes)
                {
                    _rawReady.TrySetResult();
                }
            }

            await Task.WhenAny(_rawReady.Task, Task.Delay(TimeSpan.FromSeconds(10)));

            lock (_raw)
            {
                return _raw.ToArray();
            }
        }

        public void Dispose()
        {
            try { _cts.Cancel(); } catch { }
            try { _listener.Dispose(); } catch { }
            try { if (File.Exists(_path)) File.Delete(_path); } catch { }
            _cts.Dispose();
        }
    }

    /// <summary>Named-pipe echo server, to prove the Windows path is unaffected by the change.</summary>
    private sealed class PipeEchoServer : IDisposable
    {
        private readonly string _pipeName;
        private readonly CancellationTokenSource _cts = new();

        public PipeEchoServer(string pipeName) => _pipeName = pipeName;

        public void Start() => _ = Task.Run(Serve);

        private async Task Serve()
        {
            try
            {
                using var server = new System.IO.Pipes.NamedPipeServerStream(
                    _pipeName, System.IO.Pipes.PipeDirection.InOut, 1,
                    System.IO.Pipes.PipeTransmissionMode.Byte,
                    System.IO.Pipes.PipeOptions.Asynchronous);

                await server.WaitForConnectionAsync(_cts.Token);

                while (!_cts.IsCancellationRequested && server.IsConnected)
                {
                    byte[] lengthBuffer = new byte[4];
                    int total = 0;
                    while (total < 4)
                    {
                        int n = await server.ReadAsync(lengthBuffer.AsMemory(total, 4 - total), _cts.Token);
                        if (n == 0) return;
                        total += n;
                    }

                    int length = BinaryPrimitives.ReadInt32LittleEndian(lengthBuffer);
                    byte[] payload = new byte[length];
                    total = 0;
                    while (total < length)
                    {
                        int n = await server.ReadAsync(payload.AsMemory(total, length - total), _cts.Token);
                        if (n == 0) return;
                        total += n;
                    }

                    await server.WriteAsync(lengthBuffer, _cts.Token);
                    await server.WriteAsync(payload, _cts.Token);
                    await server.FlushAsync(_cts.Token);
                }
            }
            catch (OperationCanceledException) { }
            catch (IOException) { }
            catch (ObjectDisposedException) { }
        }

        public void Dispose()
        {
            try { _cts.Cancel(); } catch { }
            _cts.Dispose();
        }
    }

    // ---------------------------------------------------------------- plumbing

    /// <summary>
    /// Short socket path. <c>sun_path</c> is ~104 bytes on macOS, and
    /// <c>Path.GetTempPath()</c> there returns a long <c>/var/folders/...</c> path, so Unix uses
    /// <c>/tmp</c> directly.
    /// </summary>
    private static string TempSocketPath()
    {
        string dir = OperatingSystem.IsWindows() ? Path.GetTempPath() : "/tmp";

        // 12 hex chars is plenty of uniqueness and keeps the whole path far inside sun_path.
        string name = "npcl-" + Guid.NewGuid().ToString("N")[..12] + ".sock";
        return Path.Combine(dir, name);
    }

    private static void Header()
    {
        Console.WriteLine("NamedPipeClientLibrary Probe");
        Console.WriteLine(new string('=', 62));
        Console.WriteLine($"  .NET runtime      : {RuntimeInformation.FrameworkDescription}");
        Console.WriteLine($"  OS                : {RuntimeInformation.OSDescription.Trim()}");
        Console.WriteLine($"  Architecture      : {RuntimeInformation.OSArchitecture}");
        Console.WriteLine($"  Platform          : {(OperatingSystem.IsWindows() ? "Windows" : OperatingSystem.IsMacOS() ? "macOS" : "Linux/other")}");
        Console.WriteLine($"  Transport (auto)  : {(OperatingSystem.IsWindows() ? "named pipe" : "unix socket")}");
        Console.WriteLine($"  Library assembly  : {AssemblyPath()}");
        Console.WriteLine();
    }

    private static string AssemblyPath()
    {
        // AppContext.BaseDirectory rather than Assembly.Location: Location is empty inside a
        // single-file bundle, which silently hides which build actually ran.
        string? loc = typeof(NamedPipeClient).Assembly.Location;
        return string.IsNullOrEmpty(loc)
            ? Path.Combine(AppContext.BaseDirectory, "NamedPipeClientLibrary.dll") + "  (single-file)"
            : loc;
    }

    private static void Section(string title)
    {
        Console.WriteLine();
        Console.WriteLine($"--- {title} {new string('-', Math.Max(0, 58 - title.Length))}");
    }

    private static void Check(string what, bool ok, string detail = "")
    {
        _checks++;
        if (ok)
        {
            Console.WriteLine($"  [PASS] {what}");
        }
        else
        {
            _failures++;
            Console.WriteLine($"  [FAIL] {what}{(string.IsNullOrEmpty(detail) ? "" : $" -- {detail}")}");
        }
    }

    private static int Finish()
    {
        Console.WriteLine();
        Console.WriteLine(new string('=', 62));
        Console.WriteLine(_failures == 0
            ? $"RESULT: all {_checks} checks passed."
            : $"RESULT: {_failures} of {_checks} checks FAILED.");
        return _failures == 0 ? 0 : 1;
    }

    private static void Usage()
    {
        Console.WriteLine("Usage: dotnet NamedPipeClientLibrary.Probe.dll [--no-pipe]");
        Console.WriteLine();
        Console.WriteLine("  Exercises NamedPipeClient against a loopback server speaking the");
        Console.WriteLine("  agent's wire protocol (4-byte LE length + UTF-8).");
        Console.WriteLine();
        Console.WriteLine("  --no-pipe   skip the Windows named-pipe scenario");
    }
}
