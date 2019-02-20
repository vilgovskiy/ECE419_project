package shared.messages;

import java.util.ArrayList;
import com.google.gson.Gson;

public class KVAdminMessage implements SerializeDeserializable {

	public enum Status {
		START, /* start processing client requests */
		START_SUCCESS,
		START_ERROR,
		STOP, /* stop processing client requests */
		STOP_SUCCESS,
		STOP_ERROR,
		SHUT_DOWN, /* exit the KVServer application */
		SHUT_DOWN_SUCCESS,
		SHUT_DOWN_ERROR,
		LOCK_WRITE, /* lock the KVServer for write applications */
		LOCK_WRITE_SUCCESS,
		LOCK_WRITE_ERROR,
		UNLOCK_WRITE, /* unlock the KVServer for write applications */
		UNLOCK_WRITE_SUCCESS,
		UNLOCK_WRITE_ERROR,
		MOVE_DATA, /* transfer data between servers */
		MOVE_DATA_SUCCESS,
		MOVE_DATA_ERROR,
		UPDATE_METADATA, /* update the metadata */
		UPDATE_METADATA_SUCCESS,
		UPDATE_METADATA_ERROR
	};

	private Status status;
	private ArrayList<String> args;

	public KVAdminMessage(Status status) {
		this.status = status;
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
