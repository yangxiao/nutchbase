/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nutch.parse;

// Hadoop imports
import org.apache.hadoop.conf.Configurable;

// Nutch imports
import org.apache.nutch.plugin.TablePluggable;
import org.apache.nutch.util.hbase.OldWebTableRow;

/** A parser for content generated by a {@link org.apache.nutch.protocol.Protocol}
 * implementation.  This interface is implemented by extensions.  Nutch's core
 * contains no page parsing code.
 */
public interface Parser extends TablePluggable, Configurable {
  /** The name of the extension point. */
  public final static String X_POINT_ID = Parser.class.getName();

  /** 
   * <p>
   * This method takes a Hbase table row and returns a {@link Parse} instance
   * </p>
   * 
   * @param url Page's URL
   * @param row url's row. Parse plugins can add their updates here. They
   * will be reflected in hbase. Plugins must not update for items already
   * in ParseHbase
   * @return Parse
   */
  Parse getParse(String url, OldWebTableRow row);
}
