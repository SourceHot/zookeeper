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


/**
 * Path related utilities
 */
public class PathUtils {

    /** validate the provided znode path string
     * @param path znode path string
     * @param isSequential if the path is being created
     * with a sequential flag
     * @throws IllegalArgumentException if the path is invalid
     */
    public static void validatePath(String path, boolean isSequential)
            throws IllegalArgumentException {
        validatePath(isSequential ? path + "1" : path);
    }

    /**
     * Validate the provided znode path string
     *
     * @param path znode path string
     * @throws IllegalArgumentException if the path is invalid
     */
    public static void validatePath(String path) throws IllegalArgumentException {
        // 路径为空
        if (path == null) {
            throw new IllegalArgumentException("Path cannot be null");
        }
        // 路径长度为0
        if (path.length() == 0) {
            throw new IllegalArgumentException("Path length must be > 0");
        }
        // 路径第一个字符不是斜杠
        if (path.charAt(0) != '/') {
            throw new IllegalArgumentException(
                    "Path must start with / character");
        }
        // 路径长度为1
        if (path.length() == 1) { // done checking - it's the root
            return;
        }
        // 路径最后一个字符为斜杠
        if (path.charAt(path.length() - 1) == '/') {
            throw new IllegalArgumentException(
                    "Path must not end with / character");
        }

        // 循环整个路径确认是否合法
        String reason = null;
        char lastc = '/';
        char chars[] = path.toCharArray();
        char c;
        for (int i = 1; i < chars.length; lastc = chars[i], i++) {
            c = chars[i];

            if (c == 0) {
                reason = "null character not allowed @" + i;
                break;
            }
            else if (c == '/' && lastc == '/') {
                reason = "empty node name specified @" + i;
                break;
            }
            else if (c == '.' && lastc == '.') {
                if (chars[i - 2] == '/' &&
                        ((i + 1 == chars.length)
                                || chars[i + 1] == '/')) {
                    reason = "relative paths not allowed @" + i;
                    break;
                }
            }
            else if (c == '.') {
                if (chars[i - 1] == '/' &&
                        ((i + 1 == chars.length)
                                || chars[i + 1] == '/')) {
                    reason = "relative paths not allowed @" + i;
                    break;
                }
            }
            else if (c > '\u0000' && c <= '\u001f'
                    || c >= '\u007f' && c <= '\u009F'
                    || c >= '\ud800' && c <= '\uf8ff'
                    || c >= '\ufff0' && c <= '\uffff') {
                reason = "invalid character @" + i;
                break;
            }
        }

        // 如果reason信息存在抛出异常
        if (reason != null) {
            throw new IllegalArgumentException(
                    "Invalid path string \"" + path + "\" caused by " + reason);
        }
    }

    /**
     * Convert Windows path to Unix
     *
     * @param path
     *            file path
     * @return converted file path
     */
    public static String normalizeFileSystemPath(String path) {
        if (path != null) {
            String osname = java.lang.System.getProperty("os.name");
            if (osname.toLowerCase().contains("windows")) {
                return path.replace('\\', '/');
            }
        }
        return path;
    }
}
