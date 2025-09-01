namespace BPlusTreeExercise;

public enum ENodeType
{
    Internal,
    Leaf
}

public static class ArrayExtensions
{
    public static void ShiftArrayRight<T>(this T[] array, int startIndex, int count)
    {
        int elementsToShift = count - startIndex;

        if (elementsToShift > 0)
        {
            Array.Copy(array, startIndex, array, startIndex + 1, elementsToShift);
        }
    }
    
    public static void ShiftArrayLeft<T>(this T[] array, int startIndex, int count)
    {
        int elementsToShift = count - 1 - startIndex;

        if (elementsToShift > 0)
        {
            Array.Copy(array, startIndex + 1, array, startIndex, elementsToShift);
        }
    }
}

public abstract class Node<TKey>(int order, ENodeType nodeType)
{
    protected readonly TKey[] _keys = new TKey[order - 1];

    public ENodeType NodeType => nodeType;
    public int KeyCount { get; set; }
    public InternalNode<TKey>? Parent { get; set; }
    public ReadOnlySpan<TKey> Keys => _keys.AsSpan(0, KeyCount);

    public bool IsFull => KeyCount == _keys.Length;

    public int FindKeyIndex(TKey key, IComparer<TKey> comparer)
    {
        return Keys.BinarySearch(key, comparer);
    }

    public void SetKey(int index, TKey key)
    {
        _keys[index] = key;
    }

    public bool TryUpdateKey(TKey keyToReplace, TKey newKey, IComparer<TKey> comparer)
    {
        int index = FindKeyIndex(keyToReplace, comparer);
        if (index >= 0)
        {
            _keys[index] = newKey;
            return true;
        }

        return false;
    }
}

public class InternalNode<TKey>(int order) : Node<TKey>(order, ENodeType.Internal)
{
    private readonly Node<TKey>?[] _children = new Node<TKey>[order];

    public ReadOnlySpan<Node<TKey>> Children => _children.AsSpan(0, KeyCount + 1);

    public int FindIndexOfChild(Node<TKey> childNode)
    {
        for (int i = 0; i < Children.Length; i++)
        {
            if (Children[i] == childNode)
                return i;
        }

        throw new InvalidOperationException("Child node not found");
    }

    public void SetChild(int index, Node<TKey> childNode)
    {
        _children[index] = childNode;
        childNode.Parent = this;
    }

    public KeyValuePair<TKey, Node<TKey>> TakeOne(int index)
    {
        if (index < 0 || index >= KeyCount)
            throw new ArgumentOutOfRangeException(nameof(index), "Index is out of range");

        var result = new KeyValuePair<TKey, Node<TKey>>(_keys[index], _children[index + 1]!);
        RemoveAt(index);

        return result;
    }

    public void InsertAt(int keyIndex, TKey key, Node<TKey> rightChild)
    {
        if (IsFull)
            throw new InvalidOperationException("Node is full");

        // Shift keys to the right to make space for the new key.
        _keys.ShiftArrayRight(keyIndex, KeyCount);

        // Shift children to the right to make space for the new child
        int childIndex = keyIndex + 1;
        _children.ShiftArrayRight(childIndex, KeyCount + 1);

        _keys[keyIndex] = key;
        _children[childIndex] = rightChild;

        rightChild.Parent = this;

        KeyCount++;
    }

    public void RemoveAt(int index)
    {
        _keys.ShiftArrayLeft(index, KeyCount);
        _children.ShiftArrayLeft(index + 1, KeyCount + 1);

        KeyCount--;

        // Clear the last (now unused) element
        _keys[KeyCount] = default!;
        _children[KeyCount + 1] = null;
    }

    public void RemoveChild(Node<TKey> childNode)
    {
        int index = FindIndexOfChild(childNode);
        RemoveAt(index);
    }

    public void Clear()
    {
        KeyCount = 0;

        Array.Clear(_keys, 0, _keys.Length);
        Array.Clear(_children, 0, _children.Length);
    }
}

public class LeafNode<TKey, TValue>(int order) : Node<TKey>(order, ENodeType.Leaf)
{
    private readonly TValue[] _values = new TValue[order - 1];

    public ReadOnlySpan<TValue> Values => _values.AsSpan(0, KeyCount);

    public LeafNode<TKey, TValue>? Previous { get; set; }
    public LeafNode<TKey, TValue>? Next { get; set; }

    public KeyValuePair<TKey, TValue> TakeOne(int index)
    {
        if (index < 0 || index >= KeyCount)
            throw new ArgumentOutOfRangeException(nameof(index), "Index is out of range");

        var result = new KeyValuePair<TKey, TValue>(_keys[index], _values[index]);
        RemoveAt(index);

        return result;
    }

    public void InsertAt(int index, TKey key, TValue value)
    {
        if (IsFull)
            throw new InvalidOperationException("Node is full");

        // Shift keys and values to the right to make space
        _keys.ShiftArrayRight(index, KeyCount);
        _values.ShiftArrayRight(index, KeyCount);

        _keys[index] = key;
        _values[index] = value;

        KeyCount++;
    }

    public void RemoveAt(int index)
    {
        _keys.ShiftArrayLeft(index, KeyCount);
        _values.ShiftArrayLeft(index, KeyCount);

        KeyCount--;

        // Clear the last (now unused) element
        _keys[KeyCount] = default!;
        _values[KeyCount] = default!;
    }

    public void Clear()
    {
        KeyCount = 0;

        Array.Clear(_keys, 0, _keys.Length);
        Array.Clear(_values, 0, _values.Length);
    }
}

public class BPlusTree<TKey, TValue>
{
    private readonly int order;
    private readonly IComparer<TKey> comparer;
    private readonly int minKeys;

    public BPlusTree(int order, IComparer<TKey> comparer)
    {
        if (order < 3)
            throw new ArgumentException("Order must be at least 3", nameof(order));

        this.order = order;
        this.comparer = comparer;
        minKeys = (order - 1) / 2;
    }

    public Node<TKey>? Root { get; set; }
    public List<LeafNode<TKey, TValue>> LeafNodes { get; } = [];

    public int FindInsertIndex(TKey key, ReadOnlySpan<TKey> keys)
    {
        int low = 0;
        int high = keys.Length - 1;
        int index = keys.Length; // Default to the end if key is largest

        while (low <= high)
        {
            int mid = low + (high - low) / 2;
            int compareResult = comparer.Compare(key, keys[mid]);

            if (compareResult < 0)
            {
                // key is smaller than keys[mid].
                // This is a potential insertion point. Store it and
                // check if there's an even earlier one to the left.
                index = mid;
                high = mid - 1;
            }
            else // compareResult >= 0
            {
                // key is greater than or equal to keys[mid].
                // The insertion point must be to the right.
                low = mid + 1;
            }
        }

        return index;
    }

    public void InsertIntoParent(Node<TKey> leftChild, Node<TKey> rightChild, TKey promotedKey)
    {
        var parent = leftChild.Parent;

        if (parent == null)
        {
            var newRoot = new InternalNode<TKey>(order);
            newRoot.SetChild(0, leftChild);
            newRoot.InsertAt(0, promotedKey, rightChild);
            Root = newRoot;
            return;
        }

        if (parent.IsFull)
        {
            SplitInternalNode(parent, promotedKey, rightChild);
            return;
        }

        int insertIndex = parent.FindIndexOfChild(leftChild);

        parent.InsertAt(insertIndex, promotedKey, rightChild);

        return;
    }

    public void SplitInternalNode(InternalNode<TKey> nodeToSplit, TKey keyToInsert, Node<TKey> childToInsert)
    {
        var tempKeys = new List<TKey>(nodeToSplit.Keys.ToArray());
        var tempChildren = new List<Node<TKey>>(nodeToSplit.Children.ToArray());

        int insertIndex = FindInsertIndex(keyToInsert, nodeToSplit.Keys);
        tempKeys.Insert(insertIndex, keyToInsert);
        tempChildren.Insert(insertIndex + 1, childToInsert);

        int splitPointIndex = tempKeys.Count / 2;
        TKey keyToPromote = tempKeys[splitPointIndex];


        nodeToSplit.Clear();

        var newRightNode = new InternalNode<TKey>(order);

        nodeToSplit.SetChild(0, tempChildren[0]);

        for (int i = 0; i < splitPointIndex; i++)
        {
            nodeToSplit.InsertAt(i, tempKeys[i], tempChildren[i + 1]);
        }

        newRightNode.SetChild(0, tempChildren[splitPointIndex + 1]);

        for (int i = splitPointIndex + 1; i < tempKeys.Count; i++)
        {
            newRightNode.InsertAt(i - (splitPointIndex + 1), tempKeys[i], tempChildren[i + 1]);
        }

        InsertIntoParent(nodeToSplit, newRightNode, keyToPromote);
    }

    public void SplitLeafNode(LeafNode<TKey, TValue> leafToSplit, TKey keyToInsert, TValue valueToInsert)
    {
        List<TKey> tempKeys = [.. leafToSplit.Keys.ToArray()];
        List<TValue> tempValues = [.. leafToSplit.Values.ToArray()];

        int insertIndex = FindInsertIndex(keyToInsert, leafToSplit.Keys);

        // n.b. List.Insert automatically shifts values right
        tempKeys.Insert(insertIndex, keyToInsert);
        tempValues.Insert(insertIndex, valueToInsert);

        int splitPointIndex = tempKeys.Count / 2;
        TKey keyToPromote = tempKeys[splitPointIndex];

        var newRightLeaf = new LeafNode<TKey, TValue>(order);
        LeafNodes.Add(newRightLeaf);

        leafToSplit.Clear();

        for (int i = 0; i < splitPointIndex; i++)
        {
            leafToSplit.InsertAt(i, tempKeys[i], tempValues[i]);
        }

        for (int i = splitPointIndex; i < tempKeys.Count; i++)
        {
            newRightLeaf.InsertAt(i - splitPointIndex, tempKeys[i], tempValues[i]);
        }

        newRightLeaf.Previous = leafToSplit;
        newRightLeaf.Next = leafToSplit.Next;
        leafToSplit.Next = newRightLeaf;

        InsertIntoParent(leafToSplit, newRightLeaf, keyToPromote);
    }

    public void Insert(TKey key, TValue value)
    {
        if (Root == null)
        {
            Root = new LeafNode<TKey, TValue>(order);
            ((LeafNode<TKey, TValue>)Root).InsertAt(0, key, value);
            LeafNodes.Add((LeafNode<TKey, TValue>)Root);
            return;
        }

        Node<TKey> currentNode = Root;

        while (currentNode is InternalNode<TKey> internalNode)
        {
            int childIndex = FindInsertIndex(key, internalNode.Keys);
            currentNode = internalNode.Children[childIndex];
        }

        // At this point currentNode is guaranteed to be a leaf node
        var leaf = (LeafNode<TKey, TValue>)currentNode;

        int insertIndex = FindInsertIndex(key, leaf.Keys);

        // If the leaf is not full, just insert and we're done.
        if (!leaf.IsFull)
        {
            leaf.InsertAt(insertIndex, key, value);
            return;
        }

        // If the leaf is full, we must split it.
        SplitLeafNode(leaf, key, value);
    }

    private bool TryBorrowFromLeftSibling(Node<TKey> node)
    {
        if (node is LeafNode<TKey, TValue> leafNode)
        {
            var leftSibling = leafNode.Previous;

            if (leftSibling?.KeyCount > minKeys)
            {
                var borrowed = leftSibling.TakeOne(leftSibling.KeyCount - 1);
                leafNode.InsertAt(0, borrowed.Key, borrowed.Value);

                // n.b. Parent is never null since we don't process the root node
                var parent = leafNode.Parent!;
                int keyIndexToUpdate = parent.FindIndexOfChild(node) - 1;

                parent.SetKey(keyIndexToUpdate, borrowed.Key);

                return true;
            }
        }
        else
        {
            var internalNode = (InternalNode<TKey>)node;
            // n.b. Parent is never null since we don't process the root node
            var parent = internalNode.Parent!;
            int nodeIndex = parent.FindIndexOfChild(internalNode);

            if (nodeIndex == 0)
                return false; // No left sibling

            InternalNode<TKey> leftSibling = (InternalNode<TKey>)parent.Children[nodeIndex - 1];

            if (leftSibling.KeyCount > minKeys)
            {
                TKey separatorKey = parent.Keys[nodeIndex - 1];
                var borrowed = leftSibling.TakeOne(leftSibling.KeyCount - 1);
                internalNode.InsertAt(0, separatorKey, borrowed.Value);

                parent.SetKey(nodeIndex - 1, borrowed.Key);
                return true;
            }
        }

        return false;
    }

    //private void TryMergeWithLeftSibling
    //{
    //}

    public void Remove(TKey key)
    {
        if (Root == null)
            return;

        Node<TKey> currentNode = Root;

        while (currentNode is InternalNode<TKey> internalNode)
        {
            int childIndex = FindInsertIndex(key, internalNode.Keys);

            currentNode = internalNode.Children[childIndex];
        }

        int keyIndex = currentNode.FindKeyIndex(key, comparer);
        var leaf = (LeafNode<TKey, TValue>)currentNode;
        leaf.RemoveAt(keyIndex);

        int minKeys = (order - 1) / 2;

        TKey? newKey = currentNode.Keys.Length > 0 ? currentNode.Keys[0] : default;

        while (currentNode.Parent != null)
        {

        }

    }

    public void Update(TKey key, TValue newValue)
    {
    }
}