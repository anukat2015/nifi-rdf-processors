package org.rdfpipeline.processors;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.config.SocketConfig;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.LayeredConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.DefaultConnectionReuseStrategy;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.client.SystemDefaultCredentialsProvider;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.protocol.HttpContext;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.ssl.SSLContextService.ClientAuth;

@Tags({ "http", "https", "connection", "pooling" })
@CapabilityDescription("HTTP Connection pooling")
public class HttpClientServiceImpl extends AbstractControllerService implements HttpClientService {

	private static final PropertyDescriptor PROP_AUTH_USERNAME = new PropertyDescriptor.Builder()
			.name("Username")
			.displayName("Username")
			.description(
					"The username to be used by the client to authenticate against a remote host.  Cannot include control characters (0-31), ':', or DEL (127).")
			.addValidator(
					StandardValidators.createRegexMatchingValidator(Pattern
							.compile("^[\\x20-\\x39\\x3b-\\x7e\\x80-\\xff]+$"))).build();

	private static final PropertyDescriptor PROP_AUTH_PASSWORD = new PropertyDescriptor.Builder()
			.name("Password")
			.displayName("Password")
			.description(
					"The password to be used by the client to authenticate against a remote host.")
			.sensitive(true)
			.addValidator(
					StandardValidators.createRegexMatchingValidator(Pattern
							.compile("^[\\x20-\\x7e\\x80-\\xff]+$"))).build();

	private static final PropertyDescriptor PROP_SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
			.name("SSL Context Service")
			.description(
					"The SSL Context Service used to provide client certificate information for TLS/SSL (https) connections.")
			.identifiesControllerService(SSLContextService.class).build();

	private static final PropertyDescriptor PROP_TCP_NO_DELAY = new PropertyDescriptor.Builder()
			.name("TCP No Delay")
			.description(
					"Disable Nagle's algorithm for this connection.  Written data to the network is not buffered pending acknowledgement of previously written data.")
			.required(true).allowableValues("true", "false").defaultValue("false")
			.addValidator(StandardValidators.BOOLEAN_VALIDATOR).build();

	private static final PropertyDescriptor PROP_CONNECT_TIMEOUT = new PropertyDescriptor.Builder()
			.name("Connection Timeout")
			.description("Max wait time for connection to remote service.").required(true)
			.defaultValue("5 secs").addValidator(StandardValidators.TIME_PERIOD_VALIDATOR).build();

	private static final PropertyDescriptor PROP_READ_TIMEOUT = new PropertyDescriptor.Builder()
			.name("Read Timeout")
			.description(
					"Max wait time for response from remote service. A timeout on blocking Socket operations.")
			.required(true).defaultValue("15 secs")
			.addValidator(StandardValidators.TIME_PERIOD_VALIDATOR).build();

	private static final PropertyDescriptor PROP_ALIVE_TIMEOUT = new PropertyDescriptor.Builder()
			.name("Keep Alive Timeout")
			.description("Max wait time to keep an inactive TCP connection open.").required(true)
			.defaultValue("4 secs").addValidator(StandardValidators.TIME_PERIOD_VALIDATOR).build();

	private static final PropertyDescriptor PROP_BUFFER_SIZE = new PropertyDescriptor.Builder()
			.name("Buffer Size").description("If size of an internal stream buffer.")
			.required(true).defaultValue("8 KB")
			.addValidator(StandardValidators.DATA_SIZE_VALIDATOR).build();

	private static final PropertyDescriptor PROP_MAX_CONN_ROUTE = new PropertyDescriptor.Builder()
			.name("Max Connections Per Route")
			.description("Max connections per route to have open at once.").required(true)
			.defaultValue(System.getProperty("http.maxConnections", "20"))
			.addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR).build();

	private static final PropertyDescriptor PROP_MAX_CONN = new PropertyDescriptor.Builder()
			.name("Max Connections")
			.description("Max connections to have open at once.")
			.required(true)
			.defaultValue(
					Integer.toString(2 * Integer.valueOf(PROP_MAX_CONN_ROUTE.getDefaultValue())))
			.addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR).build();

	private static final PropertyDescriptor PROP_MAX_REDIRECT = new PropertyDescriptor.Builder()
			.name("Max Redirects").description("Max number of redirects to follow.").required(true)
			.defaultValue("20").addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
			.build();

	private static final PropertyDescriptor PROP_USER_AGENT = new PropertyDescriptor.Builder()
			.name("User Agent")
			.description("User agent string to sent to remote servers.")
			.required(true)
			.defaultValue("Java/" + java.lang.System.getProperty("java.version"))
			.addValidator(
					StandardValidators.createRegexMatchingValidator(Pattern
							.compile("^(\\S|\t| )+$"))).build();

	private HttpClientConnectionManager manager;
	private ConnectionKeepAliveStrategy keepAliveStrategy;
	private String userAgent;
	private CredentialsProvider credentials;
	private long aliveTimeout;
	private RequestConfig requestConfig;

	@OnEnabled
	public synchronized void enable(ConfigurationContext context) {
		String username = context.getProperty(PROP_AUTH_USERNAME).getValue();
		String password = context.getProperty(PROP_AUTH_PASSWORD).getValue();
		SSLContextService sslService = context.getProperty(PROP_SSL_CONTEXT_SERVICE)
				.asControllerService(SSLContextService.class);
		boolean tcpNoDelay = context.getProperty(PROP_TCP_NO_DELAY).asBoolean();
		int connTimeout = context.getProperty(PROP_CONNECT_TIMEOUT)
				.asTimePeriod(TimeUnit.MILLISECONDS).intValue();
		int soTimeout = context.getProperty(PROP_READ_TIMEOUT)
				.asTimePeriod(TimeUnit.MILLISECONDS).intValue();
		int bufferSize = context.getProperty(PROP_BUFFER_SIZE).asDataSize(DataUnit.B)
				.intValue();
		int maxConnRoute = context.getProperty(PROP_MAX_CONN_ROUTE).asInteger();
		int maxConn = context.getProperty(PROP_MAX_CONN).asInteger();
		int maxRedirect = context.getProperty(PROP_MAX_REDIRECT).asInteger();
		aliveTimeout = context.getProperty(PROP_ALIVE_TIMEOUT).asTimePeriod(
				TimeUnit.MILLISECONDS);
		userAgent = context.getProperty(PROP_USER_AGENT).getValue();
		disable();
		credentials = createCredentialsProvider(username, password);
		manager = createConnectionManager(sslService, tcpNoDelay, soTimeout, bufferSize,
				maxConnRoute, maxConn);
		requestConfig = RequestConfig.custom().setConnectTimeout(connTimeout)
				.setSocketTimeout(soTimeout).setExpectContinueEnabled(username != null)
				.setMaxRedirects(maxRedirect).setRedirectsEnabled(maxRedirect > 0)
				.setCircularRedirectsAllowed(false).build();
		keepAliveStrategy = new ConnectionKeepAliveStrategy() {
			private ConnectionKeepAliveStrategy delegate = DefaultConnectionKeepAliveStrategy.INSTANCE;

			public long getKeepAliveDuration(HttpResponse response, HttpContext context) {
				long ret = delegate.getKeepAliveDuration(response, context);
				if (ret > 0)
					return ret;
				return aliveTimeout;
			}
		};
	}

	public synchronized CloseableHttpClient getHttpClient() {
		return HttpClients.custom().useSystemProperties().setConnectionManager(manager)
				.setConnectionReuseStrategy(DefaultConnectionReuseStrategy.INSTANCE)
				.setKeepAliveStrategy(keepAliveStrategy).useSystemProperties()
				.disableContentCompression().setDefaultRequestConfig(requestConfig)
				.setDefaultCredentialsProvider(credentials).setUserAgent(userAgent).build();
	}

	@OnDisabled
	public synchronized void disable() {
		if (manager != null) {
			manager.shutdown();
			manager = null;
		}
	}

	@Override
	protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		return Collections.unmodifiableList(Arrays.asList(PROP_AUTH_USERNAME, PROP_AUTH_PASSWORD,
				PROP_SSL_CONTEXT_SERVICE, PROP_TCP_NO_DELAY, PROP_CONNECT_TIMEOUT,
				PROP_READ_TIMEOUT, PROP_ALIVE_TIMEOUT, PROP_BUFFER_SIZE, PROP_MAX_CONN_ROUTE,
				PROP_MAX_CONN, PROP_MAX_REDIRECT, PROP_USER_AGENT));
	}

	private HttpClientConnectionManager createConnectionManager(SSLContextService sslService,
			boolean tcpNoDelay, int soTimeout, int bufferSize, int maxConnRoute, int maxConn) {
		PoolingHttpClientConnectionManager mgr = createPoolingHttpClientConnectionManager(sslService);
		mgr.setDefaultSocketConfig(SocketConfig.custom().setTcpNoDelay(tcpNoDelay)
				.setSoTimeout(soTimeout).build());
		mgr.setDefaultConnectionConfig(ConnectionConfig.custom().setBufferSize(bufferSize).build());
		mgr.setDefaultMaxPerRoute(maxConnRoute);
		mgr.setMaxTotal(maxConn);
		return mgr;
	}

	private PoolingHttpClientConnectionManager createPoolingHttpClientConnectionManager(
			SSLContextService sslService) {
		LayeredConnectionSocketFactory sslSocketFactory;
		sslSocketFactory = sslService == null ? SSLConnectionSocketFactory.getSystemSocketFactory()
				: new SSLConnectionSocketFactory(sslService.createSSLContext(ClientAuth.NONE));
		Registry<ConnectionSocketFactory> registry = RegistryBuilder
				.<ConnectionSocketFactory> create()
				.register("http", PlainConnectionSocketFactory.getSocketFactory())
				.register("https", sslSocketFactory).build();
		PoolingHttpClientConnectionManager mgr;
		mgr = new PoolingHttpClientConnectionManager(registry);
		return mgr;
	}

	private CredentialsProvider createCredentialsProvider(String username, String password) {
		if (username == null)
			return new SystemDefaultCredentialsProvider();
		BasicCredentialsProvider credentials = new BasicCredentialsProvider();
		credentials.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username,
				password));
		return credentials;
	}
}
