package shared.messages;

import java.util.ArrayList;
import com.google.gson.Gson;

public class KVAdminMessage implements SerializeDeserializable {

	public enum Status {
		START, /* start processing client requests */
		STOP, /* stop processing client requests */
		SHUT_DOWN, /* exit the KVServer application */
		LOCK_WRITE, /* lock the KVServer for write applications */
		UNLOCK_WRITE, /* unlock the KVServer for write applications */
		MOVE_DATA, /* transfer data between servers */
	};

	private Status status;
	private ArrayList<String> args;

	public KVAdminMessage() {}

	public KVAdminMessage(Status status) {
		this.status = status;
	}

	public KVAdminMessage(Status status, ArrayList<String> args) {
		this.status = status;
		this.args = args;
	}

	/* Get the status */
	public Status getStatus() {
		return status;
	}

	/* Setter for the status field */
	public void setStatus(Status status) {
		this.status = status;
	}

	/* Get the args passed into the command */
	public ArrayList<String> getArgs() {
		return args;
	}

	public String serialize() {
		return new Gson().toJson(this);
	}

	public void deserialize(String jsonData) {
		KVAdminMessage msg = new Gson().fromJson(jsonData, this.getClass());
		this.status = msg.status;
		this.args = msg.args;
	}
}
