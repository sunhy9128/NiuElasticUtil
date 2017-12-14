package utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.*;
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

    private static final Logger LOGGER = LogManager.getLogger(ElasticUtils.class.getName());

    {
        try {
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
            Settings settings = Settings.builder().put("cluster.name", this.clusterName).build();
            this.client = new PreBuiltTransportClient(settings)
                    .addTransportAddresses(new InetSocketTransportAddress(InetAddress.getByName(this.host), 9300));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            LOGGER.error("properties 文件未找到");
        } catch (IOException e) {
            e.printStackTrace();
            LOGGER.error("inputstream error");
        }
    }

    private JSONObject search(String index, QueryBuilder queryBuilder, HighlightBuilder highlightBuilder, String size) {
        try {
            JSONObject resultJson = new JSONObject();
            SearchRequestBuilder searchRequestBuilder = this.client.prepareSearch(index).setSearchType(SearchType.QUERY_THEN_FETCH)
                    .setQuery(queryBuilder).highlighter(highlightBuilder);
            SearchResponse searchResponse;
            Long start=System.currentTimeMillis();
            if (size == null || size.isEmpty()) {
                searchResponse = searchRequestBuilder.execute().actionGet();
            } else {
                searchResponse = searchRequestBuilder.setSize(Integer.parseInt(size)).execute().actionGet();
            }
            Long end=System.currentTimeMillis();
            LOGGER.error("query time"+(end-start)+"ms");
            SearchHit[] hits = searchResponse.getHits().getHits();
            JSONArray jsonObjects = new JSONArray();
            for (int i = 0; i < hits.length; i++) {
                JSONObject object = new JSONObject();
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
            return resultJson;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private JSONObject search(QueryBuilder queryBuilder, HighlightBuilder highlightBuilder, String size) {
        return this.search("*", queryBuilder, highlightBuilder, size);
    }

    public void createIndex(String indexName) {
        try {
            CreateIndexResponse indexResponse = this.client.admin().indices().prepareCreate(indexName).get();
        } catch (ElasticsearchException e) {
            e.printStackTrace();
        }
    }

    public JSONObject actionQuery(JSONObject jsonObject) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, InstantiationException, NoSuchFieldException {
        String query = jsonObject.getString("query");
        JSONObject queryBuilderObject = jsonObject.getJSONObject("queryBuilder");
        String mode = queryBuilderObject.getString("mode") + "Query";
        String size = queryBuilderObject.getString("size");
        String fieldName = queryBuilderObject.getString("fieldName");
        NiuHighLightBuilders niuHighLightBuilders = new NiuHighLightBuilders();
        Object highLight = jsonObject.get("highLight");
        if (highLight != null) {
            if (highLight.getClass().equals(JSONObject.class)) {
                this.highlightBuilder = niuHighLightBuilders.highlightBuilder(((JSONObject) highLight).getString("preTags"), ((JSONObject) highLight).getString("postTags"));
            } else if (highLight.getClass().equals(String.class)) {
                this.highlightBuilder = (HighlightBuilder) niuHighLightBuilders.getClass().getField(highLight.toString().toUpperCase() + "_HIGHLIGHT").get(niuHighLightBuilders);
            }
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

//    public static void main(String[] args) throws NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchFieldException {
//        JSONObject jsonObject = JSON.parseObject("{\n" +
//                "\t\n" +
//                "\t\"query\": \"李\",\n" +
//                "\t\"queryBuilder\":{\n" +
//                "\t\t\"mode\":\"term\",\n" +
//                "\t\t\"size\":5\n" +
//                "\t\t},\n" +
//                "\t\t\"highLight\":\"strong\"\n" +
//                "}");
//        ElasticUtils elasticUtils = new ElasticUtils();
//        elasticUtils.actionQuery(jsonObject).getJSONArray("data").forEach(System.out::println);
//    }
}
