package utils;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class NiuQueryBuilders {
    /**
     * A Precise Query that matches documents containing a term.Without analyser the text must be a single Chinese character.
     *
     * @param fieldName The name of the field
     * @param text      The value of the term
     */
    public QueryBuilder termQuery(String fieldName, String text) {
        QueryBuilder queryBuilder = QueryBuilders.termQuery(fieldName, text);
        return queryBuilder;
    }

    /**
     * A filer for a field based on several terms matching on any of them.
     * Can be used as full-text Query with Chinese character.
     *
     * @param fieldName The field name
     * @param text      The terms
     */
    public QueryBuilder termsQuery(String fieldName, String text) {
        String[] split = text.split("");
        QueryBuilder queryBuilder = QueryBuilders.termsQuery(fieldName, split);
        return queryBuilder;
    }

    public QueryBuilder termsQuery(String text) {
        String[] split = text.split("");
        QueryBuilder queryBuilder = QueryBuilders.termsQuery("_all", split);
        return queryBuilder;
    }

    /**
     * A query that matches on all documents.
     */
    public QueryBuilder matchAllQuery() {
        QueryBuilder queryBuilder = QueryBuilders.matchAllQuery();
        return queryBuilder;
    }

    /**
     * Creates a match query with type "BOOLEAN" for the provided field name and text.
     * Similar to termsQuery when query Chinese words,but less exactly because it may work
     * as a full-text query without analyzer installed.
     *
     * @param fieldName The field name.
     * @param text      The query text (to be analyzed).
     */
    public QueryBuilder matchQuery(String fieldName, String text) {
        QueryBuilder queryBuilder = QueryBuilders.matchQuery(fieldName, text);
        return queryBuilder;
    }

    public QueryBuilder matchQuery(String text) {
        QueryBuilder queryBuilder = QueryBuilders.matchQuery("_all", text);
        return queryBuilder;
    }

    /**
     * Creates a match query with type "BOOLEAN" for the provided field name and text.
     *
     * @param fieldNames The field names.
     * @param text       The query text (to be analyzed).
     */
    public QueryBuilder multiMatchQuery(String text, String... fieldNames) {
        QueryBuilder queryBuilder = QueryBuilders.multiMatchQuery(text, fieldNames);
        return queryBuilder;
    }

    /**
     * A query that parses a query string and runs it. There are two modes that this operates.
     * This one is a full-text query
     *
     * @param queryString The query string to run
     */
    public QueryBuilder queryStringQuery(String queryString) {
        QueryBuilder queryBuilder = org.elasticsearch.index.query.QueryBuilders.queryStringQuery(queryString).analyzer("ik_smart").defaultField("_all");
        return queryBuilder;
    }

    public QueryBuilder queryStringQuery(String queryString, String... fieldNames) {
        Map<String, Float> map = new HashMap<String, Float>();
        for (String fieldName : fieldNames) {
            map.put(fieldName, Float.POSITIVE_INFINITY);
        }
        QueryBuilder queryBuilder = org.elasticsearch.index.query.QueryBuilders.queryStringQuery(queryString).analyzer("ik_smart").fields(map);
        return queryBuilder;
    }
}
