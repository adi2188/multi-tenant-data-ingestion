package io.atlan.ingestion;

import java.lang.reflect.*;
import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

/** Test-only driver decorator; gates actual JDBC boundaries without replacing persistence. */
final class JdbcProbe implements Driver, AutoCloseable {
  interface Hooks {
    default void beforeQuery(String sql) throws Exception {}

    default void beforeUpdate(String sql) throws Exception {}

    default void afterUpdate(String sql) throws Exception {}

    default void afterNext(String sql, boolean hasRow) throws Exception {}
  }

  private final Driver delegate;
  private final Hooks hooks;

  JdbcProbe(Hooks hooks) throws Exception {
    Class.forName("org.sqlite.JDBC");
    delegate = DriverManager.getDriver("jdbc:sqlite::memory:");
    this.hooks = hooks;
    DriverManager.deregisterDriver(delegate);
    DriverManager.registerDriver(this);
  }

  @Override
  public Connection connect(String url, Properties info) throws SQLException {
    Connection connection = delegate.connect(url, info);
    if (connection == null) return null;
    return proxy(
        Connection.class,
        (method, arguments) -> {
          Object result = invoke(connection, method, arguments);
          if (method.getName().equals("prepareStatement")
              && result instanceof PreparedStatement statement)
            return statement(statement, (String) arguments[0]);
          return result;
        });
  }

  private PreparedStatement statement(PreparedStatement statement, String sql) {
    return proxy(
        PreparedStatement.class,
        (method, arguments) -> {
          if (method.getName().equals("executeUpdate")) hooks.beforeUpdate(sql);
          if (method.getName().equals("executeQuery")) hooks.beforeQuery(sql);
          Object result = invoke(statement, method, arguments);
          if (method.getName().equals("executeUpdate")) hooks.afterUpdate(sql);
          if (result instanceof ResultSet rows)
            return proxy(
                ResultSet.class,
                (rowMethod, rowArguments) -> {
                  Object value = invoke(rows, rowMethod, rowArguments);
                  if (rowMethod.getName().equals("next")) hooks.afterNext(sql, (boolean) value);
                  return value;
                });
          return result;
        });
  }

  private interface Call {
    Object run(Method method, Object[] arguments) throws Throwable;
  }

  private static <T> T proxy(Class<T> type, Call call) {
    return type.cast(
        Proxy.newProxyInstance(
            type.getClassLoader(),
            new Class<?>[] {type},
            (instance, method, arguments) -> call.run(method, arguments)));
  }

  private static Object invoke(Object target, Method method, Object[] arguments) throws Throwable {
    try {
      return method.invoke(target, arguments);
    } catch (InvocationTargetException ex) {
      throw ex.getCause();
    }
  }

  @Override
  public boolean acceptsURL(String url) throws SQLException {
    return delegate.acceptsURL(url);
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
    return delegate.getPropertyInfo(url, info);
  }

  @Override
  public int getMajorVersion() {
    return delegate.getMajorVersion();
  }

  @Override
  public int getMinorVersion() {
    return delegate.getMinorVersion();
  }

  @Override
  public boolean jdbcCompliant() {
    return delegate.jdbcCompliant();
  }

  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    return delegate.getParentLogger();
  }

  @Override
  public void close() throws SQLException {
    DriverManager.deregisterDriver(this);
    DriverManager.registerDriver(delegate);
  }
}
