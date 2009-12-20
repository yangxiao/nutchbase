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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.nutch.parse.HTMLMetaTags;
import org.apache.nutch.plugin.*;
import org.apache.nutch.util.ObjectCache;
import org.apache.nutch.util.hbase.HbaseColumn;
import org.apache.nutch.util.hbase.OldWebTableRow;
import org.apache.hadoop.conf.Configuration;

import org.w3c.dom.DocumentFragment;

/** Creates and caches {@link HtmlParseFilter} implementing plugins.*/
public class HtmlParseFilters {

  private HtmlParseFilter[] htmlParseFilters;

  public HtmlParseFilters(Configuration conf) {
        ObjectCache objectCache = ObjectCache.get(conf);
        this.htmlParseFilters =
          (HtmlParseFilter[]) objectCache.getObject(HtmlParseFilter.class.getName());
        if (htmlParseFilters == null) {
            HashMap<String, HtmlParseFilter> filters =
              new HashMap<String, HtmlParseFilter>();
            try {
                ExtensionPoint point =
                  PluginRepository.get(conf).getExtensionPoint(HtmlParseFilter.X_POINT_ID);
                if (point == null)
                    throw new RuntimeException(HtmlParseFilter.X_POINT_ID + " not found.");
                Extension[] extensions = point.getExtensions();
                for (int i = 0; i < extensions.length; i++) {
                    Extension extension = extensions[i];
                    HtmlParseFilter parseFilter =
                      (HtmlParseFilter) extension.getExtensionInstance();
                    if (!filters.containsKey(parseFilter.getClass().getName())) {
                        filters.put(parseFilter.getClass().getName(), parseFilter);
                    }
                }
                HtmlParseFilter[] htmlParseFilters =
                  filters.values().toArray(new HtmlParseFilter[filters.size()]);
                objectCache.setObject(HtmlParseFilter.class.getName(), htmlParseFilters);
            } catch (PluginRuntimeException e) {
                throw new RuntimeException(e);
            }
            this.htmlParseFilters =
              (HtmlParseFilter[])objectCache.getObject(HtmlParseFilter.class.getName());
        }
    }                  

  /** Run all defined filters. */
  public Parse filter(String url, OldWebTableRow row, Parse parse,
                           HTMLMetaTags metaTags, DocumentFragment doc) {

    // loop on each filter
    for (HtmlParseFilter htmlParseFilter : htmlParseFilters) {
      // call filter interface
      parse = htmlParseFilter.filter(url, row, parse, metaTags, doc);

      // any failure on parse obj, return
      if (!parse.getParseStatus().isSuccess()) {
        return parse;
      }
    }

    return parse;
  }

  public Collection<HbaseColumn> getColumns() {
    Collection<HbaseColumn> columns = new HashSet<HbaseColumn>();
    for (HtmlParseFilter htmlParseFilter : htmlParseFilters) {
      columns.addAll(htmlParseFilter.getColumns());
    }
    return columns;
  }
}
