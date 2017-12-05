package utils;

import io.vertx.core.json.JsonObject;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class ElasticUtils {

    private TransportClient client;
    {
        try {
            Settings settings = Settings.builder().put("cluster.name", "niu_cluster").build();
            client = new PreBuiltTransportClient(settings)
                    .addTransportAddresses(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    public JsonObject search(String index, QueryBuilder queryBuilder, HighlightBuilder highlightBuilder) {
        try {
            SearchResponse response = this.client.prepareSearch(index).setSearchType(SearchType.QUERY_THEN_FETCH)
                    .setQuery(queryBuilder).highlighter(highlightBuilder).setSize(20).execute().actionGet();
            SearchHit[] hits = response.getHits().getHits();
            JsonObject jsonObject = new JsonObject();
            List<JsonObject> jsonObjects = new ArrayList<JsonObject>();
            for (int i = 0; i < hits.length; i++) {
                JsonObject object = new JsonObject();
                StringBuffer s = new StringBuffer();
                Map<String, HighlightField> highlightFields = hits[i].getHighlightFields();
                String high = "";
                for (String s1 : highlightFields.keySet()) {
                    Text[] fragments = highlightFields.get(s1).getFragments();
                    for (Text fragment : fragments) {
                        high += s1 + ": " + fragment + " ";
                    }
                }
                object.put("shard", i);
                object.put("source", hits[i].getSource());
                object.put("highlight", high.toString());
                jsonObjects.add(object);
            }
            jsonObject.put("result", jsonObjects);
            return jsonObject;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public JsonObject search(QueryBuilder queryBuilder, HighlightBuilder highlightBuilder) {
        return this.search("*", queryBuilder, highlightBuilder);
    }

    public JsonObject search(QueryBuilder queryBuilder) {
        return this.search(queryBuilder, NiuHighLightBuilders.DEFAULT_HIGHLIGHT);
    }

    public void createIndex(String indexName) {
        try {
            CreateIndexResponse indexResponse = this.client.admin().indices().prepareCreate(indexName).get();
            System.out.println(indexResponse.isAcknowledged());
        } catch (ElasticsearchException e) {
            e.printStackTrace();
        }
    }

}
