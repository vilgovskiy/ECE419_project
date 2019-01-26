package server.cache.util;

public class Node {
	public String key;
	public String value;
	public Node prev;
	public Node next;

	public Node(String key, String value) {
		this.key = key;
		this.value = value;
		this.prev = null;
		this.next = null;
	}
}
