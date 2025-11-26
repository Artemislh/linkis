/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.linkis.metadata.query.service;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;

import javax.net.ssl.SSLContext;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.cat.IndicesResponse;
import co.elastic.clients.elasticsearch.core.InfoResponse;
import co.elastic.clients.elasticsearch.indices.GetMappingRequest;
import co.elastic.clients.elasticsearch.indices.GetMappingResponse;
import co.elastic.clients.elasticsearch.indices.get_mapping.IndexMappingRecord;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

public class ElasticConnection implements Closeable {

  public static final String DEFAULT_TYPE_NAME = "type";

  private static final String DEFAULT_MAPPING_NAME = "mappings";
  private static final String DEFAULT_INDEX_NAME = "index";
  private static final String FIELD_PROPS = "properties";

  private RestClient restClient;
  private ElasticsearchClient client;

  public ElasticConnection(String[] endPoints, String username, String password, String fingerprint)
      throws IOException {
    HttpHost[] httpHosts = new HttpHost[endPoints.length];
    for (int i = 0; i < endPoints.length; i++) {
      httpHosts[i] = HttpHost.create(endPoints[i]);
    }
    RestClientBuilder restClientBuilder = RestClient.builder(httpHosts);

    restClientBuilder.setHttpClientConfigCallback(
        httpClientBuilder -> {
          configureAuthentication(httpClientBuilder, username, password);
          configureSsl(httpClientBuilder, fingerprint);
          return httpClientBuilder;
        });

    this.restClient = restClientBuilder.build();

    ElasticsearchTransport transport =
        new RestClientTransport(restClient, new JacksonJsonpMapper());
    this.client = new ElasticsearchClient(transport);

    // Try to test connection
    ping();
  }

  private void configureAuthentication(
      HttpAsyncClientBuilder httpClientBuilder, String username, String password) {
    if (StringUtils.isNotBlank(username) && StringUtils.isNotBlank(password)) {
      CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
      credentialsProvider.setCredentials(
          AuthScope.ANY, new UsernamePasswordCredentials(username, password));
      httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
    }
  }

  private void configureSsl(HttpAsyncClientBuilder httpClientBuilder, String fingerprint) {
    if (StringUtils.isNotBlank(fingerprint) && !"null".equals(fingerprint)) {
      try {
        SSLContext sslContext =
            co.elastic.clients.transport.TransportUtils.sslContextFromCaFingerprint(fingerprint);
        httpClientBuilder.setSSLContext(sslContext);
      } catch (Exception e) {
        throw new RuntimeException("Failed to configure SSL with fingerprint: " + fingerprint, e);
      }
    }
  }

  public List<String> getAllIndices() throws Exception {
    List<String> indices = new ArrayList<>();
    try {
      IndicesResponse response = client.cat().indices();
      response
          .valueBody()
          .forEach(
              record -> {
                String index = record.index();
                if (StringUtils.isNotBlank(index) && !index.startsWith(".")) {
                  indices.add(index);
                }
              });
    } catch (Exception e) {
      throw new RuntimeException("Failed to get indices", e);
    }
    return indices;
  }

  public List<String> getTypes(String index) throws Exception {
    List<String> types = new ArrayList<>();
    try {
      GetMappingResponse response = client.indices().getMapping(builder -> builder.index(index));
      Map<String, IndexMappingRecord> mappings = response.result();
      if (mappings.containsKey(index)) {
        IndexMappingRecord record = mappings.get(index);
        if (record.mappings().properties() != null) {
          types.add("_doc");
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to get types for index: " + index, e);
    }
    return types;
  }

  @SuppressWarnings("unchecked")
  public Map<Object, Object> getProps(String index, String type) throws Exception {
    try {
      GetMappingRequest request = GetMappingRequest.of(builder -> builder.index(index));
      GetMappingResponse response = client.indices().getMapping(request);

      Map<String, IndexMappingRecord> mappings = response.result();
      if (mappings.containsKey(index)) {
        IndexMappingRecord record = mappings.get(index);
        if (record.mappings().properties() != null) {
          return record.mappings().properties().entrySet().stream()
              .collect(
                  Collectors.toMap(
                      Map.Entry::getKey,
                      entry -> {
                        Map<String, Object> propMap = new java.util.HashMap<>();
                        propMap.put(DEFAULT_TYPE_NAME, entry.getValue()._kind().jsonValue());
                        return propMap;
                      }));
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to get properties for index: " + index, e);
    }
    return new java.util.HashMap<>();
  }

  public void ping() throws IOException {
    try {
      InfoResponse response = client.info();
      if (response.version() == null) {
        throw new RuntimeException("Ping to ElasticSearch ERROR: Invalid response");
      }
    } catch (Exception e) {
      throw new IOException("Failed to ping Elasticsearch", e);
    }
  }

  @Override
  public void close() throws IOException {
    if (this.restClient != null) {
      this.restClient.close();
    }
  }
}
