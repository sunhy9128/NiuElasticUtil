package utils;

import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;

import java.util.Date;

public class NiuHighLightBuilders {
    private static HighlightBuilder highlightBuilder;

    public static HighlightBuilder STRONG_HIGHLIGHT;

    public static HighlightBuilder DEFAULT_HIGHLIGHT;

    private  HighlightBuilder getSTRONG_HIGHLIGHT(){
        this.STRONG_HIGHLIGHT =highlightBuilder("*","<strong>","</strong>").requireFieldMatch(false);
        return STRONG_HIGHLIGHT;
    }

    private HighlightBuilder getDEFAULT_HIGHLIGHT(){
        this.DEFAULT_HIGHLIGHT =highlightBuilder("*","<n>","</n>").requireFieldMatch(false);
        return DEFAULT_HIGHLIGHT;
    }

    public HighlightBuilder highlightBuilder(String fieldName, String preTags,String postTags){
        this.highlightBuilder=new HighlightBuilder();
        this.highlightBuilder.preTags(preTags).postTags(postTags).field(fieldName).requireFieldMatch(false);
        return highlightBuilder;
    }

    public HighlightBuilder highlightBuilder(String preTags,String postTagss){
        this.highlightBuilder= highlightBuilder("*",preTags,postTagss);
        return highlightBuilder;
    }

    public HighlightBuilder highlightBuilder(){
        this.highlightBuilder=highlightBuilder("<strong>","</strong>");
        return highlightBuilder;
    }

}
