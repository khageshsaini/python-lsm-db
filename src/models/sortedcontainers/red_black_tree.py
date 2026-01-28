"""
Red-Black Tree implementation for sorted key-value storage.

Optimized for write-heavy workloads with O(log N) operations.
"""

from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass
from enum import IntEnum
from typing import Any

from src.interfaces.sorted_container import SortedContainer


class Color(IntEnum):
    """Node color for Red-Black Tree."""

    RED = 0
    BLACK = 1


@dataclass
class Node:
    """Node in the Red-Black Tree."""

    key: str
    value: Any
    color: Color = Color.RED
    left: "Node | None" = None
    right: "Node | None" = None
    parent: "Node | None" = None


class RedBlackTree(SortedContainer):
    """
    Red-Black Tree implementation of SortedContainer.

    Properties maintained:
    1. Every node is either red or black
    2. Root is always black
    3. Red nodes cannot have red children
    4. Every path from root to leaf has same number of black nodes
    """

    def __init__(self) -> None:
        self._root: Node | None = None
        self._size: int = 0
        self._size_bytes: int = 0
        self._iter_stack: list[Node] = []

    def put(self, key: str, value: Any) -> None:
        """Insert or update a key-value pair. O(log N)"""
        if self._root is None:
            self._root = Node(key=key, value=value, color=Color.BLACK)
            self._size = 1
            self._update_size_bytes(key, value, is_add=True)
            return

        # Find insertion point
        parent = None
        current = self._root

        while current is not None:
            parent = current
            if key < current.key:
                current = current.left
            elif key > current.key:
                current = current.right
            else:
                # Key exists, update value
                old_value = current.value
                current.value = value
                self._update_size_bytes(key, value, is_add=True)
                self._update_size_bytes(key, old_value, is_add=False)
                return

        # Insert new node
        new_node = Node(key=key, value=value, parent=parent)
        if key < parent.key:
            parent.left = new_node
        else:
            parent.right = new_node

        self._size += 1
        self._update_size_bytes(key, value, is_add=True)
        self._fix_insert(new_node)

    def get(self, key: str) -> Any | None:
        """Retrieve value by key. O(log N)"""
        node = self._find_node(key)
        return node.value if node else None

    def delete(self, key: str) -> bool:
        """Remove a key-value pair. O(log N)"""
        node = self._find_node(key)
        if node is None:
            return False

        self._update_size_bytes(node.key, node.value, is_add=False)
        self._delete_node(node)
        self._size -= 1
        return True

    def has(self, key: str) -> bool:
        return self._find_node(key) is not None

    def size(self) -> int:
        return self._size

    def size_bytes(self) -> int:
        return self._size_bytes

    def __iter__(self) -> Iterator[tuple[str, Any]]:
        return self.iterator()

    def __next__(self) -> tuple[str, Any]:
        """Return next key-value pair."""
        if not self._iter_stack:
            raise StopIteration

        node = self._iter_stack.pop()
        result = (node.key, node.value)

        # Push right subtree's leftmost path
        current = node.right
        while current:
            self._iter_stack.append(current)
            current = current.left

        return result

    def iterator(
        self, start: str | None = None, end: str | None = None
    ) -> Iterator[tuple[str, Any]]:
        return _RangeIterator(self._root, start, end)

    def __aiter__(self) -> AsyncIterator[tuple[str, Any]]:
        return self.async_iterator()

    async def __anext__(self) -> tuple[str, Any]:
        """Return next key-value pair asynchronously."""
        if not self._iter_stack:
            raise StopAsyncIteration

        node = self._iter_stack.pop()
        result = (node.key, node.value)

        # Push right subtree's leftmost path
        current = node.right
        while current:
            self._iter_stack.append(current)
            current = current.left

        return result

    def async_iterator(
        self, start: str | None = None, end: str | None = None
    ) -> AsyncIterator[tuple[str, Any]]:
        return _AsyncRangeIterator(self._root, start, end)

    def _find_node(self, key: str) -> Node | None:
        """Find node by key."""
        current = self._root
        while current is not None:
            if key < current.key:
                current = current.left
            elif key > current.key:
                current = current.right
            else:
                return current
        return None

    def _update_size_bytes(self, key: str, value: Any, is_add: bool) -> None:
        """Update size tracking."""
        # Estimate: key bytes + value bytes + node overhead (~64 bytes)
        estimated_size = len(key.encode("utf-8")) + 64
        if hasattr(value, "size_bytes"):
            # Use cached size method for Value objects
            estimated_size += value.size_bytes()
        elif hasattr(value, "__bytes__"):
            estimated_size += len(bytes(value))
        elif isinstance(value, str):
            estimated_size += len(value.encode("utf-8"))
        elif isinstance(value, bytes):
            estimated_size += len(value)

        if is_add:
            self._size_bytes += estimated_size
        else:
            self._size_bytes -= estimated_size

    def _fix_insert(self, node: Node) -> None:
        """Fix Red-Black Tree properties after insert."""
        while node != self._root and node.parent and node.parent.color == Color.RED:
            if node.parent == self._grandparent(node).left if self._grandparent(node) else None:
                uncle = self._grandparent(node).right if self._grandparent(node) else None

                if uncle and uncle.color == Color.RED:
                    # Case 1: Uncle is red
                    node.parent.color = Color.BLACK
                    uncle.color = Color.BLACK
                    self._grandparent(node).color = Color.RED
                    node = self._grandparent(node)
                else:
                    if node == node.parent.right:
                        # Case 2: Node is right child
                        node = node.parent
                        self._rotate_left(node)

                    # Case 3: Node is left child
                    node.parent.color = Color.BLACK
                    if self._grandparent(node):
                        self._grandparent(node).color = Color.RED
                        self._rotate_right(self._grandparent(node))
            else:
                uncle = self._grandparent(node).left if self._grandparent(node) else None

                if uncle and uncle.color == Color.RED:
                    node.parent.color = Color.BLACK
                    uncle.color = Color.BLACK
                    self._grandparent(node).color = Color.RED
                    node = self._grandparent(node)
                else:
                    if node == node.parent.left:
                        node = node.parent
                        self._rotate_right(node)

                    node.parent.color = Color.BLACK
                    if self._grandparent(node):
                        self._grandparent(node).color = Color.RED
                        self._rotate_left(self._grandparent(node))

        self._root.color = Color.BLACK

    def _grandparent(self, node: Node) -> Node | None:
        """Get grandparent of node."""
        if node.parent:
            return node.parent.parent
        return None

    def _rotate_left(self, node: Node) -> None:
        """Left rotation."""
        right_child = node.right
        if right_child is None:
            return

        node.right = right_child.left
        if right_child.left:
            right_child.left.parent = node

        right_child.parent = node.parent

        if node.parent is None:
            self._root = right_child
        elif node == node.parent.left:
            node.parent.left = right_child
        else:
            node.parent.right = right_child

        right_child.left = node
        node.parent = right_child

    def _rotate_right(self, node: Node) -> None:
        """Right rotation."""
        left_child = node.left
        if left_child is None:
            return

        node.left = left_child.right
        if left_child.right:
            left_child.right.parent = node

        left_child.parent = node.parent

        if node.parent is None:
            self._root = left_child
        elif node == node.parent.right:
            node.parent.right = left_child
        else:
            node.parent.left = left_child

        left_child.right = node
        node.parent = left_child

    def _delete_node(self, node: Node) -> None:
        """Delete a node from the tree."""
        # Find replacement node
        if node.left and node.right:
            # Node has two children - find successor
            successor = node.right
            while successor.left:
                successor = successor.left

            # Copy successor's data to node
            node.key = successor.key
            node.value = successor.value
            node = successor

        # Node has at most one child
        child = node.left if node.left else node.right

        if node.color == Color.BLACK:
            if child and child.color == Color.RED:
                child.color = Color.BLACK
            else:
                self._fix_delete(node)

        self._replace_node(node, child)

    def _replace_node(self, node: Node, child: Node | None) -> None:
        """Replace node with child in tree."""
        if node.parent is None:
            self._root = child
        elif node == node.parent.left:
            node.parent.left = child
        else:
            node.parent.right = child

        if child:
            child.parent = node.parent

    def _fix_delete(self, node: Node) -> None:
        """Fix Red-Black Tree properties after delete."""
        while node != self._root and (node is None or node.color == Color.BLACK):
            if node is None:
                break

            if node.parent is None:
                break

            if node == node.parent.left:
                sibling = node.parent.right

                if sibling and sibling.color == Color.RED:
                    sibling.color = Color.BLACK
                    node.parent.color = Color.RED
                    self._rotate_left(node.parent)
                    sibling = node.parent.right

                if sibling is None:
                    node = node.parent
                    continue

                left_black = sibling.left is None or sibling.left.color == Color.BLACK
                right_black = sibling.right is None or sibling.right.color == Color.BLACK

                if left_black and right_black:
                    sibling.color = Color.RED
                    node = node.parent
                else:
                    if right_black:
                        if sibling.left:
                            sibling.left.color = Color.BLACK
                        sibling.color = Color.RED
                        self._rotate_right(sibling)
                        sibling = node.parent.right

                    if sibling:
                        sibling.color = node.parent.color
                    node.parent.color = Color.BLACK
                    if sibling and sibling.right:
                        sibling.right.color = Color.BLACK
                    self._rotate_left(node.parent)
                    node = self._root
            else:
                sibling = node.parent.left

                if sibling and sibling.color == Color.RED:
                    sibling.color = Color.BLACK
                    node.parent.color = Color.RED
                    self._rotate_right(node.parent)
                    sibling = node.parent.left

                if sibling is None:
                    node = node.parent
                    continue

                left_black = sibling.left is None or sibling.left.color == Color.BLACK
                right_black = sibling.right is None or sibling.right.color == Color.BLACK

                if left_black and right_black:
                    sibling.color = Color.RED
                    node = node.parent
                else:
                    if left_black:
                        if sibling.right:
                            sibling.right.color = Color.BLACK
                        sibling.color = Color.RED
                        self._rotate_left(sibling)
                        sibling = node.parent.left

                    if sibling:
                        sibling.color = node.parent.color
                    node.parent.color = Color.BLACK
                    if sibling and sibling.left:
                        sibling.left.color = Color.BLACK
                    self._rotate_right(node.parent)
                    node = self._root

        if node:
            node.color = Color.BLACK


class _RangeIterator(Iterator[tuple[str, Any]]):
    """Iterator for range queries on Red-Black Tree."""

    def __init__(self, root: Node | None, start: str | None, end: str | None) -> None:
        self._stack: list[Node] = []
        self._end = end

        # Initialize stack with nodes >= start
        self._push_left_path(root, start)

    def __iter__(self) -> Iterator[tuple[str, Any]]:
        return self

    def __next__(self) -> tuple[str, Any]:
        if not self._stack:
            raise StopIteration

        node = self._stack.pop()

        # Check end bound
        if self._end is not None and node.key >= self._end:
            self._stack.clear()
            raise StopIteration

        result = (node.key, node.value)

        # Push right subtree's left path
        self._push_left_path(node.right, None)

        return result

    def _push_left_path(self, node: Node | None, start: str | None) -> None:
        """Push leftmost path to stack, respecting start bound."""
        while node:
            if start is not None and node.key < start:
                # Skip nodes less than start
                node = node.right
            else:
                self._stack.append(node)
                node = node.left


class _AsyncRangeIterator(AsyncIterator[tuple[str, Any]]):
    """Async iterator for range queries on Red-Black Tree (in-memory, no I/O)."""

    def __init__(self, root: Node | None, start: str | None, end: str | None) -> None:
        self._stack: list[Node] = []
        self._end = end

        # Initialize stack with nodes >= start
        self._push_left_path(root, start)

    def __aiter__(self) -> "_AsyncRangeIterator":
        return self

    async def __anext__(self) -> tuple[str, Any]:
        if not self._stack:
            raise StopAsyncIteration

        node = self._stack.pop()

        # Check end bound
        if self._end is not None and node.key >= self._end:
            self._stack.clear()
            raise StopAsyncIteration

        result = (node.key, node.value)

        # Push right subtree's left path
        self._push_left_path(node.right, None)

        return result

    def _push_left_path(self, node: Node | None, start: str | None) -> None:
        """Push leftmost path to stack, respecting start bound."""
        while node:
            if start is not None and node.key < start:
                # Skip nodes less than start
                node = node.right
            else:
                self._stack.append(node)
                node = node.left
