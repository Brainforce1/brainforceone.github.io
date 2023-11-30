namespace Bvb.Framework.Server.Commands;

internal static class TypeExtensions
{
    public static Type GetCommandResponseType(this Type request) => request
        .GetInterfaces()
        .Single(type => type.IsGenericInterface(typeof(ICommandHandler<,>)))
        .GenericTypeArguments
        .Skip(1).First();
}
