package utils;
public class KeyConfig {
    private String clusterName;
    private String host;

    public KeyConfig(String clusterName, String host) {
        this.clusterName = clusterName;
        this.host = host;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }
}
