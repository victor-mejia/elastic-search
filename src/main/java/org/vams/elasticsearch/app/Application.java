package org.vams.elasticsearch.app;


import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;

/**
 * Created by vamsp7 on 25/03/16.
 */
public class Application {

    private static String INDEX_ALIAS = "vams-index";

    public static void main(String[] args) throws UnknownHostException, InterruptedException {
        String indexName  = generateIndexName(INDEX_ALIAS);
        Client client = getClient();
        createIndex(indexName,client);


        confirmIndex(indexName,INDEX_ALIAS,client);

        Thread.sleep(10000);
    }

    private static void createIndex(String indexName, Client client) {
        client.admin().indices().prepareCreate(indexName).execute().actionGet();
    }

    private static void confirmIndex(String indexName, String indexAlias, Client client) {
        client
            .admin()
            .indices()
            .prepareDelete(indexAlias)
            .execute((ActionListenerElasticSearch<DeleteIndexResponse>) response -> client
                    .admin()
                    .indices()
                    .prepareAliases()
                    .addAlias(indexName,indexAlias)
                    .execute());
    }

    private static String generateIndexName(String prefix) {
        StringBuilder indexName = new StringBuilder();
        indexName.append(prefix)
                 .append("-")
                 .append(new Date().getTime());
        return indexName.toString();
    }

    private static Client getClient() throws UnknownHostException {
        Settings settings = Settings.builder().put("cluster.name","elasticsearch").build();
        Client client = TransportClient.builder()
                .settings(settings).build()
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
        return client;
    }
}