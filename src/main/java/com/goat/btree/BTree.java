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
	
	//Code abdo---------------------------------------------------------------------------
	public int getPageNumberForInsert(TKey primaryKey){
		BTreeLeafNode<TKey, TValue> currentNode = getLeafNodeBeforeKey(primaryKey);
		int pageNumber = 0;
		if(currentNode != null && currentNode.getRightSibling() == null && currentNode.getKey(currentNode.getKeyCount()-1).compareTo(primaryKey)<0){
			return Integer.parseInt(((String)currentNode.getValue(currentNode.getKeyCount()-1)).split("-")[0]);
		}
		while (currentNode!=null){
			for (int i = 0; i < currentNode.getKeyCount(); i++) {
				if(currentNode.getKey(i).compareTo(primaryKey) < 0){
					pageNumber = Integer.parseInt(((String)currentNode.getValue(i)).split("-")[0]);
				} else if(currentNode.getKey(i).compareTo(primaryKey) == 0){
					return -1;
				}else {
					return pageNumber;
				}
			}
			currentNode = (BTreeLeafNode<TKey, TValue>) currentNode.getRightSibling();
		}
		return 1;
	}

	private BTreeLeafNode<TKey, TValue> getLeafNodeBeforeKey(TKey key) {
		BTreeNode<TKey> currentNode = this.root;
		BTreeLeafNode<TKey, TValue> prevLeafNode = null;

		while (currentNode instanceof BTreeInnerNode<?>) {
			BTreeInnerNode<TKey> innerNode = (BTreeInnerNode<TKey>) currentNode;
			int childIndex=0;
//			int childIndex = innerNode.getChildIndex(key);
			if (childIndex == -1) {
				// Key is smaller than all children, follow the leftmost child
				currentNode = innerNode.getChild(0);
			} else {
				// Key is greater than or equal to the child at the specified index
				currentNode = innerNode.getChild(childIndex+1);
				if (!(currentNode instanceof BTreeInnerNode)) {
					prevLeafNode = (BTreeLeafNode<TKey, TValue>) currentNode;
				}
			}
		}
		return prevLeafNode;
	}
	
	//Code abdo---------------------------------------------------------------------------

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
