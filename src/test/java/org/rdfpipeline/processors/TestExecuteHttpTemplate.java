package org.rdfpipeline.processors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.config.SocketConfig;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.bootstrap.HttpServer;
import org.apache.http.impl.bootstrap.ServerBootstrap;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpRequestHandler;
import org.apache.http.util.EntityUtils;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockControllerServiceInitializationContext;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestExecuteHttpTemplate {
	public static int port = 2710;
	public static String requestUrl = "http://localhost:" + port + "/";
	public static String requestMethod = "POST";
	public static String requestType = "application/sparql-query";
	public static String requestContent = "SELECT * WHERE {}";
	public static int responseStatus = HttpStatus.SC_OK;
	public static String responseType = "text/plain";
	public static String responseContent = "Hello World!";
	private HttpServer server;
	private TestRunner runner;

	@Before
	public void setUp() throws Exception {
		server = createServer();
		server.start();
		runner = TestRunners.newTestRunner(new ExecuteHttpTemplate());
	}

	@After
	public void tearDown() throws Exception {
		if (runner != null) {
			runner.shutdown();
		}
		server.shutdown(5, TimeUnit.SECONDS);
	}

	@Test
	public void testStaticQuery() throws Exception {
		requestContent = "SELECT * WHERE {}";
		// Generate a test runner to mock a processor in a flow
		TestRunner runner = runPostSparqlQuery(requestContent, new HashMap<String, String>());

		// If you need to read or do aditional tests on results you can access
		// the content
		assertEquals(0, runner.getFlowFilesForRelationship(ExecuteHttpTemplate.REL_FAILURE).size());
		List<MockFlowFile> results = runner
				.getFlowFilesForRelationship(ExecuteHttpTemplate.REL_SUCCESS_RESP);
		assertEquals(1, results.size());

		// Test attributes and content
		MockFlowFile result = results.get(0);
		result.assertAttributeEquals(ExecuteHttpTemplate.ATTR_STATUS, String.valueOf(responseStatus));
		result.assertContentEquals(responseContent);
	}

	@Test
	public void testExpressionQuery() throws Exception {
		String query = "ASK { <${subject}> ?pred ?obj }";
		// Generate a test runner to mock a processor in a flow
		HashMap<String, String> attributes = new HashMap<String, String>();
		attributes.put("subject", "urn:test:subject");
		requestContent = query.replace("${subject}", attributes.get("subject"));
		TestRunner runner = runPostSparqlQuery(requestContent, attributes);

		assertEquals(0, runner.getFlowFilesForRelationship(ExecuteHttpTemplate.REL_FAILURE).size());
		List<MockFlowFile> results = runner
				.getFlowFilesForRelationship(ExecuteHttpTemplate.REL_SUCCESS_RESP);
		assertEquals(1, results.size());

		// Test attributes and content
		MockFlowFile result = results.get(0);
		result.assertAttributeEquals(ExecuteHttpTemplate.ATTR_STATUS, String.valueOf(responseStatus));
		result.assertContentEquals(responseContent);
	}

	private TestRunner runPostSparqlQuery(String query, Map<String, String> attributes)
			throws InitializationException {
		String identifier = "httpclient";
		HttpClientServiceImpl service = new HttpClientServiceImpl();
		service.initialize(new MockControllerServiceInitializationContext(service, identifier));
		runner.addControllerService(identifier, service);
		runner.enableControllerService(service);
		runner.setProperty(ExecuteHttpTemplate.PROP_HTTP_CLIENT_SERVICE, identifier);
		runner.setProperty(ExecuteHttpTemplate.PROP_METHOD, requestMethod);
		runner.setProperty(ExecuteHttpTemplate.PROP_URL, requestUrl);
		runner.setProperty(ExecuteHttpTemplate.PROP_TYPE, requestType);
		runner.setProperty(ExecuteHttpTemplate.PROP_ACCEPT, responseType);
		runner.setProperty(ExecuteHttpTemplate.PROP_PAYLOAD, query);
		runner.enqueue(new byte[0], attributes);
		runner.run(1);
		runner.assertQueueEmpty();
		return runner;
	}

	private HttpServer createServer() {
		return ServerBootstrap
				.bootstrap()
				.setListenerPort(port)
				.setServerInfo(this.getClass().getSimpleName() + "/0.1")
				.setSocketConfig(
						SocketConfig.custom().setSoTimeout(15000).setTcpNoDelay(true).build())
				.registerHandler("*", new HttpRequestHandler() {
					public void handle(HttpRequest request, HttpResponse response,
							HttpContext context) throws HttpException, IOException {
						try {
							assertResponse(request, response);
						} catch (Exception e) {
							e.printStackTrace();
							response.setStatusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
							StringEntity entity = new StringEntity(e.toString(), ContentType
									.create("text/plain", "UTF-8"));
							response.setEntity(entity);
						}
					}
				}).create();
	}

	void assertResponse(HttpRequest request, HttpResponse response) throws IOException {
		if (!requestUrl.endsWith(request.getRequestLine().getUri())) {
			assertEquals(requestUrl, request.getRequestLine().getUri());
		}
		assertEquals(requestMethod, request.getRequestLine().getMethod());
		Header contentType = request.getFirstHeader("Content-Type");
		if (null == contentType || !contentType.getValue().startsWith(requestType)) {
			assertEquals(requestType, null == contentType ? null : contentType.getValue());
		}
		if (requestContent != null || request instanceof HttpEntityEnclosingRequest) {
			assertTrue(request instanceof HttpEntityEnclosingRequest);
			HttpEntity entity = ((HttpEntityEnclosingRequest) request).getEntity();
			assertEquals(requestContent, EntityUtils.toString(entity));
		}
		response.setStatusCode(responseStatus);
		ContentType type = ContentType.create(responseType, "UTF-8");
		StringEntity entity = new StringEntity(responseContent, type);
		response.setEntity(entity);
	}

}
