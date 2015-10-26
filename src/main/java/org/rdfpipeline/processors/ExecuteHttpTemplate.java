package org.rdfpipeline.processors;

import static org.apache.commons.lang3.StringUtils.trimToEmpty;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.Consts;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.client.utils.URIUtils;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

/**
 * 
 * @author James Leigh
 */
@SideEffectFree
@Tags({ "http", "https", "rest", "client" })
@CapabilityDescription("An HTTP client processor which converts FlowFile attributes to a UTF-8 HTTP request using a template")
public class ExecuteHttpTemplate extends AbstractProcessor {

	// attributes
	protected static final String ATTR_STATUS = "http.status";

	protected static final String ATTR_MESSAGE = "http.message";

	protected static final String ATTR_URL = "http.url";

	protected static final String ATTR_ERROR_RESPONSE = "http.response";

	// Set of flowfile attributes which we generally always ignore during
	// processing, including when converting http headers, copying attributes,
	// etc.
	// This set includes our strings defined above as well as some standard
	// flowfile
	// attributes.
	public static final Set<String> IGNORED_ATTRIBUTES = Collections.unmodifiableSet(new HashSet<>(
			Arrays.asList(ATTR_STATUS, ATTR_MESSAGE, ATTR_ERROR_RESPONSE, ATTR_URL, "uuid",
					"filename", "path")));

	// properties
	public static final PropertyDescriptor PROP_METHOD = new PropertyDescriptor.Builder()
			.name("HTTP Method")
			.description("HTTP request method (GET, POST, PUT, DELETE, HEAD, OPTIONS).")
			.required(true)
			.defaultValue("GET")
			.expressionLanguageSupported(true)
			.addValidator(
					StandardValidators.createRegexMatchingValidator(Pattern.compile("^\\S+$")))
			.build();

	protected static PropertyDescriptor PROP_URL = new PropertyDescriptor.Builder()
			.name("Remote URL")
			.description(
					"Remote URL which will be connected to, including scheme, host, port, path.")
			.required(true).expressionLanguageSupported(true)
			.addValidator(StandardValidators.URL_VALIDATOR).build();

	protected static PropertyDescriptor PROP_TYPE = new PropertyDescriptor.Builder()
			.name("Content-Type").description("HTTP request Content-Type header value")
			.required(false).expressionLanguageSupported(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	protected static PropertyDescriptor PROP_ACCEPT = new PropertyDescriptor.Builder()
			.name("Accept").description("HTTP request Accept header value").required(false)
			.expressionLanguageSupported(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.build();

	public static final PropertyDescriptor PROP_ATTRIBUTES_TO_SEND = new PropertyDescriptor.Builder()
			.name("Attributes to Send")
			.description(
					"Regular expression that defines which additional attributes to send as HTTP headers in the request. "
							+ "If not defined, no additional attributes are sent as headers.")
			.required(false).addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR).build();

	protected static PropertyDescriptor PROP_PAYLOAD = new PropertyDescriptor.Builder()
			.name("Request Body").description("HTTP request body template").required(false)
			.expressionLanguageSupported(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.build();

	protected static PropertyDescriptor PROP_HTTP_CLIENT_SERVICE = new PropertyDescriptor.Builder()
			.name("HTTP Client Service")
			.description("The HTTP Client Service used to provide client connection pooling.")
			.required(true).identifiesControllerService(HttpClientService.class).build();

	// relationships
	public static final Relationship REL_SUCCESS_REQ = new Relationship.Builder().name("Original")
			.description("Original FlowFile will be routed upon success (2xx status codes).")
			.build();

	protected static Relationship REL_SUCCESS_RESP = new Relationship.Builder().name("Response")
			.description("Response FlowFile will be routed upon success (2xx status codes).")
			.build();

	protected static Relationship REL_FAILURE = new Relationship.Builder()
			.name("Failure")
			.description(
					"FlowFile will be routed on any type of failure, timeout or general exception.")
			.build();

	@Override
	protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		return Collections.unmodifiableList(Arrays.asList(PROP_METHOD, PROP_URL, PROP_TYPE,
				PROP_ACCEPT, PROP_ATTRIBUTES_TO_SEND, PROP_PAYLOAD, PROP_HTTP_CLIENT_SERVICE));
	}

	@Override
	public Set<Relationship> getRelationships() {
		return Collections.unmodifiableSet(new HashSet<>(Arrays.asList(REL_SUCCESS_RESP,
				REL_FAILURE)));
	}

	private volatile Pattern attributesToSend = null;

	@Override
	public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue,
			final String newValue) {
		final String trimmedValue = StringUtils.trimToEmpty(newValue);

		// compile the attributes-to-send filter pattern
		if (PROP_ATTRIBUTES_TO_SEND.getName().equalsIgnoreCase(descriptor.getName())) {
			if (newValue.isEmpty()) {
				attributesToSend = null;
			} else {
				attributesToSend = Pattern.compile(trimmedValue);
			}
		}
	}

	@Override
	public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
		FlowFile flowFile = session.get();
		if (flowFile == null) {
			return;
		}

		try {
			String method = trimToEmpty(context.getProperty(PROP_METHOD)
					.evaluateAttributeExpressions(flowFile).getValue());
			String url = trimToEmpty(context.getProperty(PROP_URL)
					.evaluateAttributeExpressions(flowFile).getValue());
			String type = trimToEmpty(context.getProperty(PROP_TYPE)
					.evaluateAttributeExpressions(flowFile).getValue());
			String accept = trimToEmpty(context.getProperty(PROP_ACCEPT)
					.evaluateAttributeExpressions(flowFile).getValue());
			String payload = trimToEmpty(context.getProperty(PROP_PAYLOAD)
					.evaluateAttributeExpressions(flowFile).getValue());
			HttpClientService clientService = context.getProperty(PROP_HTTP_CLIENT_SERVICE)
					.asControllerService(HttpClientService.class);

			processRequest(clientService, session, flowFile, method, url, type, accept, payload);
		} catch (Exception e) {
			// log exception
			getLogger().error("Routing to {} due to exception: {}",
					new Object[] { REL_FAILURE.getName(), e }, e);

			// transfer to failure
			session.transfer(session.penalize(flowFile), REL_FAILURE);
		}
	}

	private void processRequest(HttpClientService clientService, final ProcessSession session,
			final FlowFile flowFile, String method, final String url, String type, String accept,
			String payload) throws IOException, ClientProtocolException {
		final long startNanos = System.nanoTime();
		HttpUriRequest request = createRequest(method, url, accept, type, payload, flowFile);
		final HttpClientContext context = HttpClientContext.create();
		CloseableHttpClient client = clientService.getHttpClient();
		try {
			client.execute(request, new ResponseHandler<Void>() {
	
				@Override
				public Void handleResponse(HttpResponse response) throws ClientProtocolException,
						IOException {
					processResponse(session, flowFile, startNanos, response, context);
					return null;
				}
			}, context);
		} finally {
			client.close();
		}
	}

	private HttpUriRequest createRequest(String method, final String url, String accept,
			String type, String payload, final FlowFile flowFile) {
		HttpUriRequest request = createHttpUriRequest(method, url, type, payload);
		if (accept != null) {
			request.setHeader("Accept", accept);
		}
		// iterate through the flowfile attributes, adding any attribute that
		// matches the attributes-to-send pattern. if the pattern is not set
		// (it's an optional property), ignore that attribute entirely
		if (attributesToSend != null) {
			Map<String, String> attributes = flowFile.getAttributes();
			Matcher m = attributesToSend.matcher("");
			for (Map.Entry<String, String> entry : attributes.entrySet()) {
				String key = trimToEmpty(entry.getKey());
				String val = trimToEmpty(entry.getValue());

				// don't include any of the ignored attributes
				if (IGNORED_ATTRIBUTES.contains(key)) {
					continue;
				}

				// check if our attribute key matches the pattern
				// if so, include in the request as a header
				m.reset(key);
				if (m.matches()) {
					request.setHeader(key, val);
				}
			}
		}
		return request;
	}

	private HttpUriRequest createHttpUriRequest(final String method, String url, String type,
			String payload) {
		if (payload == null) {
			HttpRequestBase request = new HttpRequestBase() {
				public String getMethod() {
					return method;
				}
			};
			request.setURI(URI.create(url));
			return request;
		} else {
			HttpEntityEnclosingRequestBase request = new HttpEntityEnclosingRequestBase() {
				public String getMethod() {
					return method;
				}
			};
			request.setURI(URI.create(url));
			ContentType contentType = parseContentType(type);
			StringEntity entity = new StringEntity(payload, contentType);
			if (!contentType.getMimeType().startsWith("text/")) {
				entity.setContentType(type); // Don't add charset
			}
			request.setEntity(entity);
			return request;
		}
	}

	private ContentType parseContentType(String type) {
		if (type == null || type.length() == 0)
			return ContentType.create("text/plain", Consts.UTF_8);
		else if (!type.contains(";"))
			return ContentType.create(type, Consts.UTF_8);
		ContentType parsed = ContentType.parse(type);
		if (parsed.getParameter("charset") == null)
			return ContentType.create(type, Consts.UTF_8);
		else
			return parsed;
	}

	void processResponse(ProcessSession session, FlowFile flowFile, long startNanos,
			HttpResponse response, HttpClientContext context) throws IOException {
		String url = getTargetUrl(context);
		int status = response.getStatusLine().getStatusCode();
		flowFile = session.putAttribute(flowFile, ATTR_STATUS, String.valueOf(status));
		flowFile = session.putAttribute(flowFile, ATTR_MESSAGE, response.getStatusLine()
				.getReasonPhrase());
		flowFile = session.putAttribute(flowFile, ATTR_URL, url);
		if (status / 100 == 2) {
			boolean transfered = false;
			// clone the flowfile to capture the response
			FlowFile flow = session.create(flowFile);
			try {
				// write the response headers as attributes
				// this will overwrite any existing flowfile attributes
				flow = session.putAllAttributes(flow, asHeaderMap(response));

				// transfer the message body to the payload
				if (response.getEntity() != null) {
					InputStream in = response.getEntity().getContent();
					try {
						flow = session.importFrom(in, flow);
					} finally {
						in.close();
					}
				}

				// emit provenance event
				long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
				session.getProvenanceReporter().modifyContent(flow, "Executed HTTP at " + url,
						millis);

				// log the status codes from the response
				getLogger().info("Request to {} returned status code {} for {}",
						new Object[] { url, status, flowFile });

				// transfer to the correct relationship
				session.transfer(flowFile, REL_SUCCESS_REQ);
				session.transfer(flow, REL_SUCCESS_RESP);
				transfered = true;
			} finally {
				try {
					if (!transfered) {
						session.remove(flow);
					}
				} catch (Exception e1) {
					getLogger().error("Could not cleanup response flowfile due to exception: {}",
							new Object[] { e1 }, e1);
				}
			}
		} else {
			// if not successful, store the response body into an attribute
			String body = trimToEmpty(EntityUtils.toString(response.getEntity()));
			flowFile = session.putAttribute(flowFile, ATTR_ERROR_RESPONSE, body);
			throw new ClientProtocolException("Unexpected response status: " + status);
		}
	}

	private String getTargetUrl(HttpClientContext ctx) {
		HttpUriRequest request = (HttpUriRequest) ctx.getRequest();
		try {
			URI original = request.getURI();
			HttpHost target = ctx.getTargetHost();
			List<URI> list = ctx.getRedirectLocations();
			URI absolute = URIUtils.resolve(original, target, list);
			return absolute.toASCIIString();
		} catch (URISyntaxException e) {
			return request.getURI().toASCIIString();
		}
	}

	private Map<String, String> asHeaderMap(HttpResponse response) {
		Header[] all = response.getAllHeaders();
		Map<String, String> map = new LinkedHashMap<>(all.length);
		for (Header hd : all) {
			Header[] headers = response.getHeaders(hd.getName());
			if (headers.length == 1) {
				map.put(hd.getName(), hd.getValue());
			} else if (!map.containsKey(hd.getName())) {
				StringBuilder sb = new StringBuilder();
				for (Header header : headers) {
					if (sb.length() > 0) {
						sb.append(", ");
					}
					sb.append(header.getValue());
				}
				map.put(hd.getName(), sb.toString());
			}
		}
		return map;
	}
}
