package org.rdfpipeline.processors;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.controller.ControllerService;

@Tags({ "http", "https", "connection", "pooling" })
@CapabilityDescription("HTTP Connection pooling")
public interface HttpClientService extends ControllerService {

	CloseableHttpClient getHttpClient();

}