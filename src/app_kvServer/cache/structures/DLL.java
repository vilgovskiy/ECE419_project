package cache.structures;

import java.lang.Exception;
import cache.structures.Node;

public class DLL {
	Node head;
	Node tail;
	int size;

	public DLL() {
		size = 0;
		head = null;
		tail = null;
	}

	/**
	 * Inserts into the rear of the DLL
	 * @param newNode node to insert into the DLL
	 */
	public void insertRear(Node newNode) {
		if(size == 0) {
			head = newNode;
			tail = newNode;
		} else {
			tail.next = newNode;
			newNode.prev = tail;
			newNode.next = null;
			tail = newNode;
		}

		size++;
	}

	/**
	 * Deletes from the front of the DLL
	 * @throws Exception if attempting to delete from empty linked list
	 */
	public Node deleteFront() throws Exception {
		Node deletedNode = head;

		if(size == 0) {
			throw new Exception("Unable to delete from empty linked list");
		} else if(size == 1) {
			head = null;
			tail = null;
		} else {
			head = head.next;
			head.prev = null;
		}

		size--;
		return deletedNode;
	}

	/**
	 * Deletes the node n from the linked list
	 * @param n node to delete
	 */
	public void delete(Node n) {
		if(n == head && n == tail) {
			head = null;
			tail = null;
		} else if(n == head) {
			head = head.next;
			head.prev = null;
		} else if(n == tail) {
			tail = tail.prev;
			tail.next = null;
		} else {
			n.prev.next = n.next;
			n.next.prev = n.prev;
		}

		size--;
	}

	/**
	 * Gets the size of the DLL
	 * @return size
	 */
	public int getSize() {
		return size;
	}

}
