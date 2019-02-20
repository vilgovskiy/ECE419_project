package shared.messages;

import com.google.gson.Gson;
import java.util.ArrayList;

public class JsonAdminMessage implements KVAdminMessage, SerializeDeserializable {

    private Status status;
	private ArrayList<String> args;

    public JsonAdminMessage() {}

    public JsonAdminMessage(Status status, ArrayList<String> args) {
        this.status = status;
        this.args = args;
    }

    @Override
    public Status getStatus() {
        return status;
    }

	@Override
	public ArrayList<String> getArgs() {
		return args;
	}

    @Override
    public void setStatus(Status status) {
        this.status = status;
    }

    @Override
    public String serialize() {
        return new Gson().toJson(this);
    }

    @Override
    public void deserialize(String jsonData) {
			JsonAdminMessage json = new Gson().fromJson(jsonData, this.getClass());
			this.status = json.status;
        	this.args = json.args;
    }
}
