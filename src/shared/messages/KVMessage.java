package shared.messages;

public interface KVMessage {

	public enum StatusType {
		GET, 			/* Get - request */
		GET_ERROR, 		/* requested tuple (i.e. value) not found */
		GET_SUCCESS, 	/* requested tuple (i.e. value) found */
		PUT, 			/* Put - request */
		PUT_SUCCESS, 	/* Put - request successful, tuple inserted */
		PUT_UPDATE, 	/* Put - request successful, i.e. value updated */
		PUT_ERROR, 		/* Put - request not successful */
		DELETE_SUCCESS, /* Delete - request successful */
		DELETE_ERROR, 	/* Delete - request successful */
		SERVER_STOPPED, /* Server is stopped, no requests are processed */
		SERVER_WRITE_LOCK, /* Server locked for out, only get possible */
 		SERVER_NOT_RESPONSIBLE, /* Request not successful, server not responsible for key */

		REPLICA_PUT, /* Put for replication purpose */
		TRANSFER /* KV pair being transferred from another server to this one */
	}

	/**
	 * @return the key that is associated with this message,
	 * 		null if not key is associated.
	 */
	public String getKey();

	/**
	 * @return the value that is associated with this message,
	 * 		null if not value is associated.
	 */
	public String getValue();

	/**
	 * @return a status string that is used to identify request types,
	 * response types and error types associated to the message.
	 */
	public StatusType getStatus();

	/**
	 * @return the metadata that represents the entire KV Storage service
	 */
	public String getMetadata();

	/* Setter for field key */
	public void setKey(String key);

	/* Setter for field value */
	public void setValue(String value);

	/* Setter for field status */
	public void setStatus(StatusType status);

	/* Setter for metadata field */
	public void setMetadata(String metadata);

}
