package utils;

import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;

import java.util.Date;

public class NiuHighLightBuilders {
    private HighlightBuilder highlightBuilder;

    public static HighlightBuilder STRONG_HIGHLIGHT;

    public static HighlightBuilder DEFAULT_HIGHLIGHT;

    public static HighlightBuilder BLACK_HIGHLIGHT;

    public static HighlightBuilder INCLINE_HIGHLIGHT;

    {
        this.STRONG_HIGHLIGHT = highlightBuilder("*", "<strong>", "</strong>");
        this.DEFAULT_HIGHLIGHT = highlightBuilder("*", "<u>", "</u>");
        this.BLACK_HIGHLIGHT = highlightBuilder("*", "<b>", "</b>");
        this.INCLINE_HIGHLIGHT=highlightBuilder("*", "<i>", "</i>");

    }

    public HighlightBuilder highlightBuilder(String fieldName, String preTags, String postTags) {
        this.highlightBuilder = new HighlightBuilder();
        this.highlightBuilder.preTags(preTags).postTags(postTags).field(fieldName).requireFieldMatch(false);
        return highlightBuilder;
    }

    public HighlightBuilder highlightBuilder(String preTags, String postTags) {
        this.highlightBuilder = highlightBuilder("*", preTags, postTags);
        return highlightBuilder;
    }

    public HighlightBuilder highlightBuilder() {
        this.highlightBuilder = highlightBuilder("<s>", "</s>");
        return highlightBuilder;
    }
}
