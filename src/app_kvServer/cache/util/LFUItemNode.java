package app_kvServer.cache.util;

public class LFUItemNode extends Node {
	public LFUFreqNode freqNode;

	public LFUItemNode(String key, String value) {
		super(key, value);
		this.freqNode = null;
	}
}
