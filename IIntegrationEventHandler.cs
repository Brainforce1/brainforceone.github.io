using Bvb.Framework.Contracts;
using NServiceBus;

namespace Bvb.Framework.Server.Integration;

public interface IIntegrationEventHandler<T> : IHandleMessages<T>, IRequestHandler<T>
    where T : IIntegrationEvent
{
    Task Handle(T eventMessage, CancellationToken cancellationToken);
}

public abstract class BaseIntegrationEventHandler<T> : IIntegrationEventHandler<T>
    where T : IIntegrationEvent
{
    public abstract Task Handle(T eventMessage, CancellationToken cancellationToken);

    public async Task Handle(T message, IMessageHandlerContext context)
    {
        await Handle(message, CancellationToken.None);
    }

    public async Task<Unit> Execute(T request, CancellationToken cancellationToken)
    {
        await Handle(request, cancellationToken);
        return Unit.Value;
    }
}
