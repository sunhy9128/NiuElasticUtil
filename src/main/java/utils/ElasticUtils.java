package utils;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
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

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;


public class ElasticUtils {
    private TransportClient client;
    private String clusterName;
    private String host;
    private HighlightBuilder highlightBuilder;

    private void init() {
        try {
            Settings settings = Settings.builder().put("cluster.name", this.clusterName).build();
            this.client = new PreBuiltTransportClient(settings)
                    .addTransportAddresses(new InetSocketTransportAddress(InetAddress.getByName(this.host), 9300));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    private JSONObject search(String index, QueryBuilder queryBuilder, HighlightBuilder highlightBuilder, String size) {
        try {
            JSONObject resultJson = new JSONObject();
            Properties properties = new Properties();
            InputStream inputStream = new BufferedInputStream(new FileInputStream("src/main/resources/application.properties"));
            properties.load(inputStream);
            Iterator<String> iterator = properties.stringPropertyNames().iterator();
            while (iterator.hasNext()) {
                String key = iterator.next();
                if (key.equals("elastic.clusterName")) {
                    this.clusterName = properties.getProperty(key);
                } else if (key.equals("elastic.host")) {
                    this.host = properties.getProperty(key);
                }
            }
            init();
            SearchRequestBuilder searchRequestBuilder = this.client.prepareSearch(index).setSearchType(SearchType.QUERY_THEN_FETCH)
                    .setQuery(queryBuilder).highlighter(highlightBuilder);
            SearchResponse searchResponse;
            if (size == null || size.isEmpty()) {
                searchResponse = searchRequestBuilder.execute().actionGet();
            } else {
                searchResponse = searchRequestBuilder.setSize(Integer.parseInt(size)).execute().actionGet();
            }
            SearchHit[] hits = searchResponse.getHits().getHits();
            JSONArray jsonObjects = new JSONArray();
            for (int i = 0; i < hits.length; i++) {
                JSONObject object = new JSONObject();
                StringBuffer s = new StringBuffer();
                Map<String, HighlightField> highlightFields = hits[i].getHighlightFields();
                List<JSONObject> list = new ArrayList<>();
                for (String key : highlightFields.keySet()) {
                    JSONObject highObject = new JSONObject();
                    highObject.put("field", key);
                    highObject.put("value", highlightFields.get(key).getFragments()[0].toString());
                    list.add(highObject);
                }
                object.put("shard", i + 1);
                object.put("source", hits[i].getSource());
                object.put("highlight", list);
                jsonObjects.add(object);
            }
            resultJson.put("total", hits.length);
            resultJson.put("data", jsonObjects);
            client.close();
            return resultJson;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private JSONObject search(QueryBuilder queryBuilder, HighlightBuilder highlightBuilder, String size) {
        return this.search("*", queryBuilder, highlightBuilder, size);
    }

    public JSONObject search(QueryBuilder queryBuilder, String size) {
        return this.search(queryBuilder, NiuHighLightBuilders.DEFAULT_HIGHLIGHT, size);
    }

    public void createIndex(String indexName) {
        try {
            CreateIndexResponse indexResponse = this.client.admin().indices().prepareCreate(indexName).get();
            System.out.println(indexResponse.isAcknowledged());
        } catch (ElasticsearchException e) {
            e.printStackTrace();
        }
    }

    public JSONObject actionQuery(JSONObject jsonObject) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, InstantiationException {
        String query = jsonObject.getString("query");
        JSONObject queryBuilderObject = jsonObject.getJSONObject("queryBuilder");
        String mode = queryBuilderObject.getString("mode") + "Query";
        String size = queryBuilderObject.getString("size");
        String fieldName = queryBuilderObject.getString("fieldName");
        NiuHighLightBuilders niuHighLightBuilders = new NiuHighLightBuilders();
        JSONObject highLight = jsonObject.getJSONObject("highLight");
        if (highLight != null) {
            this.highlightBuilder = niuHighLightBuilders.highlightBuilder(highLight.getString("preTags"), highLight.getString("postTags"));
        } else {
            this.highlightBuilder = niuHighLightBuilders.DEFAULT_HIGHLIGHT;
        }
        QueryBuilder queryBuilder = null;
        if (query == null || query.isEmpty()) {
            Method method = NiuQueryBuilders.class.getMethod(mode);
            queryBuilder = (QueryBuilder) method.invoke(NiuQueryBuilders.class.newInstance());
        } else if ((fieldName == null || fieldName.isEmpty()) && query != null) {
            Method queryMethod = NiuQueryBuilders.class.getMethod(mode, new Class[]{String.class});
            queryBuilder = (QueryBuilder) queryMethod.invoke(NiuQueryBuilders.class.newInstance(), new String[]{query});
        } else if ((fieldName != null && !fieldName.isEmpty()) && query != null) {
            Method queryMethod = NiuQueryBuilders.class.getMethod(mode, new Class[]{String.class, String.class});
            queryBuilder = (QueryBuilder) queryMethod.invoke(NiuQueryBuilders.class.newInstance(), new String[]{fieldName, query});
        }
        return search(queryBuilder, this.highlightBuilder, size);
    }

//    public static void main(String[] args) throws NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
//        JSONObject jsonObject = JSON.parseObject("{\n" +
//                "\t\n" +
//                "\t\"query\": \"李\",\n" +
//                "\t\"queryBuilder\":{\n" +
//                "\t\t\"mode\":\"match\",\n" +
//                "\t\t\"size\":5\n" +
//                "\t\t},\n" +
//                "\t\t\"highLight\":null\n" +
//                "}");
//        ElasticUtils elasticUtils = new ElasticUtils();
//
//        elasticUtils.actionQuery(jsonObject).getJSONArray("data").forEach(System.out::println);
//    }
}
