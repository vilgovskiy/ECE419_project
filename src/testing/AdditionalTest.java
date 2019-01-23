package testing;

import org.junit.Test;

import junit.framework.TestCase;

import client.KVStore;
import shared.messages.JsonMessage;
import shared.messages.KVMessage;

public class AdditionalTest extends TestCase {
	
	// TODO add your test cases, at least 3
	
	@Test
	public void testJsonMessage() {
		JsonMessage msg = new JsonMessage(KVMessage.StatusType.PUT, "sampleKey", "sampleValue");
		String strMsg = msg.serialize();
		System.out.println(strMsg);
		JsonMessage deserializedMsg = new JsonMessage();
		deserializedMsg.deserialize(strMsg);
		System.out.println(deserializedMsg.toString());
	}
}
