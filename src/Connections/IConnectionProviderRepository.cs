namespace FluentCassandra.Connections {
    public interface IConnectionProviderRepository {
        IConnectionProvider Get(IConnectionBuilder builder);
    }
}