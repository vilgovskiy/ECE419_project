package server;

public class ServerMetadata {

    private String hostname;
    private Integer serverPort;
    private String cacheStrategy;
    private Integer cacheSize;

    public ServerMetadata(Integer cacheSize, String cacheStrategy) {
        this.serverPort = 0;
        this.hostname = "localhost";
        this.cacheStrategy = cacheStrategy;
        this.cacheSize = cacheSize;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public void setServerPort(Integer serverPort) {
        this.serverPort = serverPort;
    }

    public void setCacheStrategy(String cacheStrategy) {
        this.cacheStrategy = cacheStrategy;
    }

    public void setCacheSize(Integer cacheSize) {
        this.cacheSize = cacheSize;
    }

    public String getHostname() { return hostname; }

    public Integer getServerPort() { return serverPort; }

    public String getCacheStrategy() { return cacheStrategy; }

    public Integer getCacheSize() { return cacheSize; }

    @Override
    public String toString() {
        return hostname + ":" + serverPort + " cacheStrategy: " + cacheStrategy
                + " cacheSize: " + cacheSize;
    }
}

