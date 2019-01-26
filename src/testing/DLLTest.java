package testing;

import org.junit.Test;

import junit.framework.TestCase;
import server.cache.util.*;
import java.util.ArrayList;

public class DLLTest extends TestCase {

	private DLL dll;

	public void setUp() {
		dll = new DLL();
	}

	@Test
	public void testInsertRearOnEmptyList() {
		Node n = new Node("", "");
		dll.insertRear(n);

		assertTrue(dll.getSize() == 1);
		assertTrue(dll.getHead() == n);
		assertTrue(dll.getTail() == n);
	}

	@Test
	public void testInsertRearMultipleNodes() {
		ArrayList<Node> nodes = new ArrayList<Node>();
		Node n, ptr;

		for(int i = 0; i < 5; i++) {
			n = new Node("", "");
			nodes.add(n);
			dll.insertRear(n);
		}

		assertTrue(dll.getSize() == 5);
		assertTrue(dll.getHead() == nodes.get(0));
		assertTrue(dll.getTail() == nodes.get(4));
		ptr = dll.getHead();

		for(int i = 0; i < 5; i++) {
			assertTrue(ptr == nodes.get(i));
			ptr = ptr.next;
		}
	}

	@Test
	public void testInsertFrontMultipleNodes() {
		ArrayList<Node> nodes = new ArrayList<Node>();
		Node n, ptr;

		for(int i = 0; i < 5; i++) {
			n = new Node("", "");
			nodes.add(n);
			dll.insertFront(n);
		}

		assertTrue(dll.getSize() == 5);
		assertTrue(dll.getHead() == nodes.get(4));
		assertTrue(dll.getTail() == nodes.get(0));
		ptr = dll.getHead();

		for(int i = 4; i >= 0; i--) {
			assertTrue(ptr == nodes.get(i));
			ptr = ptr.next;
		}
	}

	@Test
	public void testInsertFrontOnEmptyList() {
		Node n = new Node("", "");
		dll.insertFront(n);

		assertTrue(dll.getSize() == 1);
		assertTrue(dll.getHead() == n);
		assertTrue(dll.getTail() == n);
	}

	@Test
	public void testInsertRightOnRearNode() {
		Node n1 = new Node("", "");
		Node n2 = new Node("", "");

		dll.insertRear(n1);
		dll.insertRight(n1, n2);

		assertTrue(n1.next == n2);
		assertTrue(dll.getTail() == n2);
		assertTrue(dll.getSize() == 2);
	}

	@Test
	public void testInsertRightOnInnerNode() {
		Node n1 = new Node("", "");
		Node n2 = new Node("", "");
		Node n3 = new Node("", "");

		dll.insertRear(n1);
		dll.insertRear(n3);
		dll.insertRight(n1, n2);

		assertTrue(n1.next == n2);
		assertTrue(n3.prev == n2);
		assertTrue(n2.prev == n1);
		assertTrue(n2.next == n3);
		assertTrue(dll.getSize() == 3);
	}

	@Test
	public void testDeleteFrontOnSingleNode() {
		Node n1 = new Node("", "");

		dll.insertFront(n1);

		Node deleted = dll.deleteFront();
		assertTrue(deleted == n1);
		assertTrue(dll.getSize() == 0);
		assertNull(dll.getHead());
		assertNull(dll.getTail());
	}

	@Test
	public void testDeleteFrontOnMultipleNodes() {
		Node n1 = new Node("", "");
		Node n2 = new Node("", "");

		dll.insertFront(n1);
		dll.insertFront(n2);

		try {
			Node deleted = dll.deleteFront();
			assertTrue(deleted == n2);
			assertTrue(dll.getSize() == 1);
			assertTrue(dll.getHead() == n1);
			assertNull(dll.getHead().prev);
		} catch (Exception e) {
			System.out.print("Attempted to delete on empty list");
		}
	}

	@Test
	public void testDeleteOnSingleNode() {
		Node n1 = new Node("", "");

		dll.insertFront(n1);
		dll.delete(n1);

		assertTrue(dll.getSize() == 0);
		assertNull(dll.getHead());
		assertNull(dll.getTail());
	}

	@Test
	public void testDeleteOnHeadNode() {
		Node n1 = new Node("", "");
		Node n2 = new Node("", "");

		dll.insertRear(n1);
		dll.insertRear(n2);
		dll.delete(n1);

		assertTrue(dll.getSize() == 1);
		assertTrue(dll.getHead() == n2);
		assertNull(n2.prev);
	}

	@Test
	public void testDeleteOnMiddleNode() {
		Node n1 = new Node("", "");
		Node n2 = new Node("", "");
		Node n3 = new Node("", "");

		dll.insertRear(n1);
		dll.insertRear(n2);
		dll.insertRear(n3);
		dll.delete(n2);

		assertTrue(dll.getSize() == 2);
		assertTrue(n1.next == n3);
		assertTrue(n3.prev == n1);
	}

	@Test
	public void testStub() {
		assertTrue(true);
	}
}
