package server.cache.util;

public class LFUFreqNode extends Node {
	public DLL queue;
	public int freq;

	public LFUFreqNode(int freq) {
		super("", "");
		this.queue = new DLL();
		this.freq = freq;
	}
}
