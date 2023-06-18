using System;
using System.Threading;

namespace Kinescribe.Internals
{
    /// <summary>
    /// Abstracts specialized cancellation handling that allows consumers to choose from two possible cancellation tokens:
    /// <list type="number">
    ///   <item>
    ///     <see cref="StoppingToken"/>: signaled as soon as graceful cancellation is requested,
    ///     which in turn can happen either because the <see cref="CancellationToken"/> provided in the constructor is signaled,
    ///     or when someone calls <see cref="RequestStop"/>, or when someone calls <see cref="Abort"/>.
    ///   </item>
    ///   <item>
    ///     <see cref="GracefulToken"/>: signaled after a grace period elapses since a graceful cancellation is requested.
    ///     The grace period timer starts when a graceful cancellation is initiated
    ///     either because the <see cref="CancellationToken"/> provided in the constructor is signaled,
    ///     or when someone calls <see cref="RequestStop"/>.
    ///     The <see cref="GracefulToken"/> is immediately signaled, without any grace period, when <see cref="Abort"/> is called.
    ///   </item>
    /// </list>
    /// <para>
    /// Consumers should use <see cref="StoppingToken"/> to control cancellation of non-critical tasks that can be safely interrupted without harm,
    /// and should use <see cref="GracefulToken"/> for tasks that are intended to run to completion even during a graceful shutdown (e.g. graceful teardown).
    /// While there is no guarantee such tasks leveraging <see cref="GracefulToken"/> will catualy run to completion, this provides best-effort semantics
    /// and affords up to the configured grace period for such tasks to complete, after which they too are canceled.
    /// </para>
    /// </summary>
    internal class GracefulCancellation : IDisposable
    {
        private readonly CancellationTokenSource _stoppingCts;
        private readonly CancellationTokenSource _gracefulCts;
        private readonly CancellationTokenRegistration _registration;
        private readonly TimeSpan _gracePeriod;

        private bool _disposed;

        public GracefulCancellation(CancellationToken stoppingToken, TimeSpan gracePeriod)
        {
            if (gracePeriod <= TimeSpan.Zero)
            {
                throw new ArgumentOutOfRangeException(nameof(gracePeriod));
            }

            stoppingToken.ThrowIfCancellationRequested();
            _gracePeriod = gracePeriod;

            _stoppingCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
            _gracefulCts = new CancellationTokenSource();
            _registration = _stoppingCts.Token.Register(() =>
            {
                _gracefulCts.CancelAfter(_gracePeriod);
            });
        }

        public void RequestStop()
        {
            _stoppingCts.Cancel();
        }

        public void Abort()
        {
            _stoppingCts.Cancel();
            _gracefulCts.Cancel();
        }

        public CancellationToken StoppingToken => _stoppingCts.Token;
        public CancellationToken GracefulToken => _gracefulCts.Token;

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            try
            {
                try
                {
                    try
                    {
                    }
                    finally
                    {
                        _registration.Dispose();
                    }
                }
                finally
                {
                    _gracefulCts.Dispose();
                }
            }
            finally
            {
                _stoppingCts.Dispose();
            }
        }
    }
}
