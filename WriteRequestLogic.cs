using System.Text.Json;
using Bvb.Framework.Context;
using Bvb.Framework.Contracts;
using Bvb.Framework.Database;
using Bvb.Framework.Server.Formatting;
using Bvb.Framework.Tracing;

namespace Bvb.Framework.Server.Db;

internal interface IWriteRequestLogic
{
    /// <summary>
    /// Executes a request handler in an atomic Db transaction, processes root entity concerns and stores the request as a handled message. 
    /// </summary>
    Task<TResponse> Handle<TRequest, TResponse>(
        Guid requestId, IRequestHandler<TRequest, TResponse> handler, TRequest request, CancellationToken cancellationToken)
        where TRequest : IRequest<TResponse>;
}

internal class WriteRequestLogic : IWriteRequestLogic
{
    private readonly DomainDb _db;
    private readonly IDomainDbTransaction _transaction;
    private readonly IWorkSession _workSession;
    private readonly IRootEntityLogic _rootEntity;
    private readonly IUserContext _userContext;
    private readonly ITraceTransactionInfo? _tracingData;

    public WriteRequestLogic(
        DomainDb db, IDomainDbTransaction transaction,
        IWorkSession workSession, IRootEntityLogic rootEntity,
        IUserContext userContext, ITraceTransactionInfo? tracingData = null)
    {
        _db = db;
        _transaction = transaction;
        _workSession = workSession;
        _rootEntity = rootEntity;
        _userContext = userContext;
        _tracingData = tracingData;
    }

    public async Task<TResponse> Handle<TRequest, TResponse>(
        Guid requestId, IRequestHandler<TRequest, TResponse> handler, TRequest request, CancellationToken cancellationToken)
        where TRequest : IRequest<TResponse>
    {
        DateTime received = DateTime.Now;

        return await _transaction.DoInTransaction(async () =>
        {
            (TResponse response, Guid? rootId) =
                await _rootEntity.Handle(handler, request, cancellationToken);

            _db.HandledMessages.Add(new HandledMessage(
                requestId,
                request.GetType().ToString(),
                JsonSerializer.Serialize(request, SerializerOptions.UseEnumMemberNames),
                received,
                DateTime.Now,
                rootId,
                _workSession.ID,
                _userContext.UserType,
                _userContext.UserIdentifier,
                _userContext.UserName,
                _tracingData?.OutgoingTraceparent
            ));

            return response;
        }, cancellationToken);
    }
}
