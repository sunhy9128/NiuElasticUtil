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

    public void init(){
        try {
            Settings settings = Settings.builder().put("cluster.name", this.clusterName).build();
            this.client = new PreBuiltTransportClient(settings)
                    .addTransportAddresses(new InetSocketTransportAddress(InetAddress.getByName(this.host), 9300));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    public JSONObject search(String index, QueryBuilder queryBuilder, HighlightBuilder highlightBuilder,String size) {
        try {
            Properties properties=new Properties();
            InputStream inputStream = new BufferedInputStream(new FileInputStream("src/main/resources/application.properties"));
            properties.load(inputStream);
            Iterator<String> iterator = properties.stringPropertyNames().iterator();
            while (iterator.hasNext()){
                String key=iterator.next();
                System.out.println(key);
                if (key.equals("elastic.clusterName")){
                    this.clusterName=properties.getProperty(key);
                }
                else if (key.equals("elastic.host")){
                    this.host=properties.getProperty(key);
                }
            }
            init();
            SearchRequestBuilder searchRequestBuilder = this.client.prepareSearch(index).setSearchType(SearchType.QUERY_THEN_FETCH)
                    .setQuery(queryBuilder).highlighter(highlightBuilder);
            SearchResponse searchResponse;
            if (size==null){
                searchResponse=searchRequestBuilder.execute().actionGet();
            }
            else {
                searchResponse=searchRequestBuilder.setSize(Integer.parseInt(size)).execute().actionGet();
            }
            SearchHit[] hits = searchResponse.getHits().getHits();
            JSONObject jsonObject = new JSONObject();
            JSONArray jsonObjects = new JSONArray();
            for (int i = 0; i < hits.length; i++) {
                JSONObject object = new JSONObject();
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
            client.close();
            return jsonObject;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public JSONObject search(QueryBuilder queryBuilder, HighlightBuilder highlightBuilder,String size) {
        return this.search("*", queryBuilder, highlightBuilder,size);
    }

    public JSONObject search(QueryBuilder queryBuilder,String size) {
        return this.search(queryBuilder, NiuHighLightBuilders.DEFAULT_HIGHLIGHT,size);
    }

    public void createIndex(String indexName) {
        try {
            CreateIndexResponse indexResponse = this.client.admin().indices().prepareCreate(indexName).get();
            System.out.println(indexResponse.isAcknowledged());
        } catch (ElasticsearchException e) {
            e.printStackTrace();
        }
    }

    public String actionQuery(JSONObject jsonObject) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, InstantiationException {
        String query = jsonObject.getString("query");
        JSONObject queryBuilderObject = jsonObject.getJSONObject("queryBuilder");
        JSONObject highLight = jsonObject.getJSONObject("highLight");
        String mode = queryBuilderObject.getString("mode") + "Query";
        String size=queryBuilderObject.getString("size");
        String fieldName = queryBuilderObject.getString("fieldName");
        NiuHighLightBuilders niuHighLightBuilders=new NiuHighLightBuilders();
        HighlightBuilder highlightBuilder = niuHighLightBuilders.highlightBuilder(highLight.getString("preTags"), highLight.getString("postTags"));
        QueryBuilder queryBuilder;
        if (fieldName == null) {
            Method queryMethod = NiuQueryBuilders.class.getMethod(mode, new Class[]{String.class});
            queryBuilder = (QueryBuilder) queryMethod.invoke(NiuQueryBuilders.class.newInstance(),new String[]{query});
        }
        else {
            Method queryMethod= NiuQueryBuilders.class.getMethod(mode,new Class[]{String.class,String.class});
            queryBuilder = (QueryBuilder) queryMethod.invoke(NiuQueryBuilders.class.newInstance(),new String[]{fieldName,query});
        }
        return search(queryBuilder, highlightBuilder,size).getJSONArray("result").toString();
    }
}
