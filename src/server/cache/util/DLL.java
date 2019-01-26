package server.cache.util;

import java.lang.Exception;

public class DLL {
	private Node head;
	private Node tail;
	private int size;

	public DLL() {
		size = 0;
		head = null;
		tail = null;
	}

	/**
	 * Inserts into the front of the DLL
	 * @param newNode node to insert into the DLL
	 */
	public void insertFront(Node newNode) {
		if(size == 0) {
			head = newNode;
			tail = newNode;
		} else {
			head.prev = newNode;
			newNode.prev = null;
			newNode.next = head;
			head = newNode;
		}

		size++;
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
	 * Inserts newNode to the right of node
	 * @param node inserts to the right of this node
	 * @param newNode node to insert into the DLL
	 */
	public void insertRight(Node node, Node newNode) {
		if(node == tail) {
			this.insertRear(newNode);
		} else {
			newNode.prev = node;
			newNode.next = node.next;

			node.next.prev = newNode;
			node.next = newNode;

			size++;
		}
	}

	/**
	 * Deletes from the front of the DLL
	 * @throws RuntimeException if attempting to delete from empty linked list
	 */
	public Node deleteFront() throws RuntimeException {
		Node deletedNode = head;

		if(size == 0) {
			throw new RuntimeException("Unable to delete from empty linked list");
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

	/**
	 * Gets the head node of the DLL
	 * @return Node
	 */
	public Node getHead() {
		return head;
	}

	/**
	 * Gets the tail node of the DLL
	 * @return Node
	 */
	public Node getTail() {
		return tail;
	}

}
