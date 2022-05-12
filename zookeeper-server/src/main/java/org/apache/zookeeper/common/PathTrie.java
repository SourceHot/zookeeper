/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * a class that implements prefix matching for 
 * components of a filesystem path. the trie
 * looks like a tree with edges mapping to 
 * the component of a path.
 * example /ab/bc/cf would map to a trie
 *           /
 *        ab/
 *        (ab)
 *      bc/
 *       / 
 *      (bc)
 *   cf/
 *   (cf)
 */
public class PathTrie {
    /**
     * the logger for this class
     */
    private static final Logger LOG = LoggerFactory.getLogger(PathTrie.class);

    /**
     * the root node of PathTrie
     */
    private final TrieNode rootNode;


    /**
     * construct a new PathTrie with
     * a root node of /
     */
    public PathTrie() {
        this.rootNode = new TrieNode(null);
    }

    /**
     * add a path to the path trie
     * @param path
     */
    public void addPath(String path) {
        // 路径为空不操作
        if (path == null) {
            return;
        }
        // 切分路径得到路径的各项元素
        String[] pathComponents = path.split("/");
        // 创建父节点，父节点初始化时使用根节点
        TrieNode parent = rootNode;

        String part = null;
        // 路径元素长度小于等于1抛出异常
        if (pathComponents.length <= 1) {
            throw new IllegalArgumentException("Invalid path " + path);
        }
        // 处理每个路径元素
        for (int i = 1; i < pathComponents.length; i++) {
            // 获取元素
            part = pathComponents[i];
            // 如果在父节点中不存在当前元素对应的节点则添加一个新的子节点
            if (parent.getChild(part) == null) {
                parent.addChild(part, new TrieNode(parent));
            }
            // 重新设置父节点
            parent = parent.getChild(part);
        }
        // 父节点property属性设置
        parent.setProperty(true);
    }

    /**
     * delete a path from the trie
     *
     * @param path the path to be deleted
     */
    public void deletePath(String path) {

        // 路径为空不操作
        if (path == null) {
            return;
        }
        // 切分路径得到路径的各项元素
        String[] pathComponents = path.split("/");
        // 创建父节点，父节点初始化时使用根节点
        TrieNode parent = rootNode;
        String part = null;
        // 路径元素长度小于等于1抛出异常
        if (pathComponents.length <= 1) {
            throw new IllegalArgumentException("Invalid path " + path);
        }
        // 处理每个路径元素
        for (int i = 1; i < pathComponents.length; i++) {
            part = pathComponents[i];

            // 如果在父节点中不存在当前元素对应的节点则返回结束后续处理
            if (parent.getChild(part) == null) {
                //the path does not exist
                return;
            }
            // 重新设置父节点
            parent = parent.getChild(part);
            LOG.info("{}", parent);
        }
        // 在parent节点中获取父节点，然后进行删除操作
        TrieNode realParent = parent.getParent();
        realParent.deleteChild(part);
    }

    /**
     * return the largest prefix for the input path.
     *
     * @param path the input path
     * @return the largest prefix for the input path.
     */
    public String findMaxPrefix(String path) {
        // 如果路径为空返回空
        if (path == null) {
            return null;
        }
        // 如果路径为/，将参数path返回
        if ("/".equals(path)) {
            return path;
        }
        // 切分路径得到路径的各项元素
        String[] pathComponents = path.split("/");
        // 创建父节点，父节点初始化时使用根节点
        TrieNode parent = rootNode;
        // 用于组装结果的容器
        List<String> components = new ArrayList<String>();
        // 路径元素长度小于等于1抛出异常
        if (pathComponents.length <= 1) {
            throw new IllegalArgumentException("Invalid path " + path);
        }
        int i = 1;
        String part = null;
        StringBuilder sb = new StringBuilder();
        int lastindex = -1;
        // 循环处理路径元素
        while ((i < pathComponents.length)) {
            // 父节点中获取路径元素对应的数据存在
            if (parent.getChild(pathComponents[i]) != null) {
                // 更新part数据
                part = pathComponents[i];
                // 覆盖父节点数据
                parent = parent.getChild(part);
                // 向components容器中添加数据
                components.add(part);
                // 如果property数据为真则将lastindex值进行重新计算
                if (parent.getProperty()) {
                    lastindex = i - 1;
                }
            }
            // 跳过处理
            else {
                break;
            }
            // 索引值向后操作
            i++;
        }
        // 组装最终的响应
        for (int j = 0; j < (lastindex + 1); j++) {
            sb.append("/" + components.get(j));
        }
        return sb.toString();
    }

    /**
     * clear all nodes
     */
    public void clear() {
        for (String child : rootNode.getChildren()) {
            rootNode.deleteChild(child);
        }
    }


    static class TrieNode {
        final HashMap<String, TrieNode> children;
        boolean property = false;
        TrieNode parent = null;

        /**
         * create a trienode with parent
         * as parameter
         * @param parent the parent of this trienode
         */
        private TrieNode(TrieNode parent) {
            children = new HashMap<String, TrieNode>();
            this.parent = parent;
        }

        /**
         * get the parent of this node
         * @return the parent node
         */
        TrieNode getParent() {
            return this.parent;
        }

        /**
         * set the parent of this node
         * @param parent the parent to set to
         */
        void setParent(TrieNode parent) {
            this.parent = parent;
        }

        /** the property of this
         * node
         * @return the property for this
         * node
         */
        boolean getProperty() {
            return this.property;
        }

        /**
         * a property that is set
         * for a node - making it
         * special.
         */
        void setProperty(boolean prop) {
            this.property = prop;
        }

        /**
         * add a child to the existing node
         * @param childName the string name of the child
         * @param node the node that is the child
         */
        void addChild(String childName, TrieNode node) {
            synchronized (children) {
                if (children.containsKey(childName)) {
                    return;
                }
                children.put(childName, node);
            }
        }

        /**
         * delete child from this node
         * @param childName the string name of the child to
         * be deleted
         */
        void deleteChild(String childName) {
            synchronized (children) {
                if (!children.containsKey(childName)) {
                    return;
                }
                TrieNode childNode = children.get(childName);
                // this is the only child node.
                if (childNode.getChildren().length == 1) {
                    childNode.setParent(null);
                    children.remove(childName);
                } else {
                    // their are more child nodes
                    // so just reset property.
                    childNode.setProperty(false);
                }
            }
        }

        /**
         * return the child of a node mapping
         * to the input childname
         * @param childName the name of the child
         * @return the child of a node
         */
        TrieNode getChild(String childName) {
            synchronized (children) {
                if (!children.containsKey(childName)) {
                    return null;
                } else {
                    return children.get(childName);
                }
            }
        }

        /**
         * get the list of children of this
         * trienode.
         * @param node to get its children
         * @return the string list of its children
         */
        String[] getChildren() {
            synchronized (children) {
                return children.keySet().toArray(new String[0]);
            }
        }

        /**
         * get the string representation
         * for this node
         */
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("Children of trienode: ");
            synchronized (children) {
                for (String str : children.keySet()) {
                    sb.append(" " + str);
                }
            }
            return sb.toString();
        }
    }
}
