package com.example.springelasticproject.Config;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
//import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.io.InputStream;
import java.security.KeyStore;
@Configuration
public class ElasticsearchConfig {

    @Bean
    public RestClient restClient() throws Exception {
        // Charger le truststore avec le certificat CA
        KeyStore trustStore = KeyStore.getInstance("PKCS12");
        try (InputStream is = getClass().getClassLoader().getResourceAsStream("elastic-stack-ca1.p12")) {
            trustStore.load(is, "alouan01".toCharArray());
        }

        // Charger le keystore pour l'authentification client
        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        try (InputStream is = getClass().getClassLoader().getResourceAsStream("elastic-certificates1.p12")) {
            keyStore.load(is, "alouan01".toCharArray());
        }

        // Configurer le SSLContext
        SSLContext sslContext = SSLContextBuilder.create()
                .loadTrustMaterial(trustStore, null)
                .loadKeyMaterial(keyStore, "alouan01".toCharArray())
                .build();

        // Credentials pour l'authentification basic
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials("elastic", "D6YHB8A8DQCEb73qeXSt")); // <= Remplace si besoin
        //D6YHB8A8DQCEb73qeXSt     pour l'utilisateur elastic localhost
        // OiTi_TD*rzie2HIomT38    pour l'utilisateur elastic sur le serveur distant

        // Désactiver seulement la vérification du nom d'hôte
        HostnameVerifier hostnameVerifier = new NoopHostnameVerifier();

        // Créer le client REST
        return RestClient.builder(new HttpHost("localhost", 9200, "https"))
                .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder
                        .setSSLContext(sslContext)
                        .setSSLHostnameVerifier(hostnameVerifier)
                        .setDefaultCredentialsProvider(credentialsProvider))
                .build();
    }

    @Bean
    public ElasticsearchClient elasticsearchClient(RestClient restClient) {
        return new ElasticsearchClient(
                new RestClientTransport(restClient, new JacksonJsonpMapper())
        );
    }
}

