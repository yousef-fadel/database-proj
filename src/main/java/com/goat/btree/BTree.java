package com.goat.btree;
import java.io.Serializable;
import java.util.ArrayList;

/**
 * A B+ tree
 * Since the structures and behaviors between internal node and external node are different, 
 * so there are two different classes for each kind of node.
 * @param <TKey> the data type of the key
 * @param <TValue> the data type of the value
 */
public class BTree<TKey extends Comparable<TKey>, TValue> implements Serializable{
	private BTreeNode<TKey> root;
	
	
	public BTree() {
		this.root = new BTreeLeafNode<TKey, TValue>();
	}

	/**
	 * Insert a new key and its associated value into the B+ tree.
	 */
	public void insert(TKey key, TValue value) {
		BTreeLeafNode<TKey, TValue> leaf = this.findLeafNodeShouldContainKey(key);
		leaf.insertKey(key, value);
		
		if (leaf.isOverflow()) {
			BTreeNode<TKey> n = leaf.dealOverflow();
			if (n != null)
				this.root = n; 
		}
	}
	
	/**
	 * Search a key value on the tree and return its associated value.
	 */
	public TValue search(TKey key) {
		BTreeLeafNode<TKey, TValue> leaf = this.findLeafNodeShouldContainKey(key);
		
		int index = leaf.search(key);
		return (index == -1) ? null : leaf.getValue(index);
	}
	
	public ArrayList<TValue> searchGreaterThan(TKey start,boolean inclusive)
	{
		BTreeLeafNode<TKey, TValue> leaf = this.findLeafNodeShouldContainKey(start);
		return leaf.searchGreaterThan(0, start,inclusive);
	}
	
	public ArrayList<TValue> searchLessThan(TKey start,boolean inclusive)
	{
		BTreeLeafNode<TKey, TValue> leaf = this.findLeafNodeShouldContainKey(start);
		return leaf.searchLessThan(leaf.values.length-1,start, inclusive);	
	}
	
	/**
	 * Delete a key and its associated value from the tree.
	 */
	public void delete(TKey key) {
		BTreeLeafNode<TKey, TValue> leaf = this.findLeafNodeShouldContainKey(key);
		
		if (leaf.delete(key) && leaf.isUnderflow()) {
			BTreeNode<TKey> n = leaf.dealUnderflow();
			if (n != null)
				this.root = n; 
		}
	}
	
	/**
	 * Search the leaf node which should contain the specified key
	 */
	@SuppressWarnings("unchecked")
	private BTreeLeafNode<TKey, TValue> findLeafNodeShouldContainKey(TKey key) {
		BTreeNode<TKey> node = this.root;
		while (node.getNodeType() == TreeNodeType.InnerNode) {
			node = ((BTreeInnerNode<TKey>)node).getChild( node.search(key) );
		}
		
		return (BTreeLeafNode<TKey, TValue>)node;
	}
	
	// simple; but does not support range searches or duplicates 
	
	public static void main(String[]args) throws ClassNotFoundException
	{
		BTree<String, String> b = new BTree<String, String>();

//		for(int i = 0;i<=30;i++)
//		{
//			b.insert(i, "page" + i);
//		}
		b.insert("mohamed", "page1");
		b.insert("yousef", "page2");
		
		System.out.println(b.searchGreaterThan("", true));
//		BTreeInnerNode x = (BTreeInnerNode)b.root;
//		String s = x.printTree();
//		System.out.println(b.search(5));
		
	}
}
