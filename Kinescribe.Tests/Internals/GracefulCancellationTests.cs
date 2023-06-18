using System;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using Kinescribe.Internals;
using Xunit;

namespace Kinescribe.Internal.Tests
{
    public class GracefulCancellationTests
    {
        [Fact]
        public void InputTokenCanceled_Throws()
        {
            // Arrange
            using var cts = new CancellationTokenSource();
            cts.Cancel();

            // Act
            Action action = () => new GracefulCancellation(cts.Token, TimeSpan.FromMinutes(1));

            // Act & Assert
            action.Should().Throw<OperationCanceledException>();
        }

        [Fact]
        public void InputTokenNotCanceled_ValidInitialState()
        {
            // Arrange
            var subject = new GracefulCancellation(CancellationToken.None, TimeSpan.FromMinutes(1));

            // Act & Assert
            subject.StoppingToken.IsCancellationRequested.Should().BeFalse();
            subject.GracefulToken.IsCancellationRequested.Should().BeFalse();
        }

        [Fact]
        public void StoppingToken_ReactsToConstructorCancellationImmediately()
        {
            // Arrange
            using var stoppingCts = new CancellationTokenSource();
            var subject = new GracefulCancellation(stoppingCts.Token, TimeSpan.FromMinutes(1));

            // Act & Assert
            stoppingCts.Cancel();
            subject.StoppingToken.IsCancellationRequested.Should().BeTrue();
            subject.GracefulToken.IsCancellationRequested.Should().BeFalse();
        }

        [Fact]
        public async Task GracefulToken_ReactsToConstructorCancellation()
        {
            // Arrange
            using var stoppingCts = new CancellationTokenSource();
            var subject = new GracefulCancellation(stoppingCts.Token, TimeSpan.FromMilliseconds(1));

            // Act & Assert
            stoppingCts.Cancel();
            subject.StoppingToken.IsCancellationRequested.Should().BeTrue();

            while (!subject.GracefulToken.IsCancellationRequested)
            {
                await Task.Delay(1);
            }
        }

        [Fact]
        public void StoppingToken_ReactsToRequestStopImmediately()
        {
            // Arrange
            var subject = new GracefulCancellation(CancellationToken.None, TimeSpan.FromMinutes(1));

            // Act & Assert
            subject.RequestStop();
            subject.StoppingToken.IsCancellationRequested.Should().BeTrue();
            subject.GracefulToken.IsCancellationRequested.Should().BeFalse();
        }

        [Fact]
        public async Task GracefulToken_ReactsToRequestStop()
        {
            // Arrange
            var subject = new GracefulCancellation(CancellationToken.None, TimeSpan.FromMilliseconds(1));

            // Act & Assert
            subject.RequestStop();
            subject.StoppingToken.IsCancellationRequested.Should().BeTrue();

            while (!subject.GracefulToken.IsCancellationRequested)
            {
                await Task.Delay(1);
            }
        }

        [Fact]
        public void Abort_Works()
        {
            // Arrange
            var subject = new GracefulCancellation(CancellationToken.None, TimeSpan.FromMinutes(1));

            // Act & Assert
            subject.Abort();
            subject.StoppingToken.IsCancellationRequested.Should().BeTrue();
            subject.GracefulToken.IsCancellationRequested.Should().BeTrue();
        }
    }
}
