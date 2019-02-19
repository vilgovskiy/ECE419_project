package shared.messages;


import ecs.ECSConsistentHash;

import com.google.gson.Gson;

public class JsonMessage implements KVMessage, SerializeDeserializable {

    private StatusType status;
    private String key;
    private String value;
	private String metadata;

    public JsonMessage() {
        this.key = "";
        this.value = "";
    }

    public JsonMessage(StatusType status, String key, String value) {
        this.status = status;
        this.key = key;
        this.value = value;
    }


    @Override
    public String getKey() {
        return key;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public StatusType getStatus() {
        return status;
    }

	@Override
	public String getMetadata() {
		return metadata;
	}

    @Override
    public void setKey(String key) {
        this.key = key;
    }

    @Override
    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public void setStatus(StatusType status) {
        this.status = status;
    }

	@Override
	public void setMetadata(String metadata) {
		this.metadata = metadata;
	}

    @Override
    public String serialize() {
        return new Gson().toJson(this);
    }

    @Override
    public void deserialize(String jsonData) {
			JsonMessage json = new Gson().fromJson(jsonData, this.getClass());
			this.status = json.status;
        	this.key = json.key;
        	this.value = json.value;
    }

    @Override
    public String toString() {
        return "JSON MSG - STATUS: " + this.status
                + " KEY: " + this.key + " VALUE: " + this.value;
    }

}
