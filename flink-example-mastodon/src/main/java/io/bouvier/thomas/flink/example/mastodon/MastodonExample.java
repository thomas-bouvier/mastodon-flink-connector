/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.bouvier.thomas.flink.example.mastodon;

import io.bouvier.thomas.flink.mastodon.MastodonSource;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.time.Duration;
import java.util.StringTokenizer;

/**
 * Implements the "MastodonStream" program that computes a most used word occurrence over JSON
 * objects in a streaming fashion.
 *
 * <p>The input is a Toot stream from a MastodonSource.
 *
 * <p>Usage: <code>Usage: MastodonExample [--output &lt;path&gt;]
 * [--mastodon-source.instanceString &lt;instance&gt; --mastodon-source.accessToken &lt;secret&gt;]
 * </code><br>
 *
 * <p>If no parameters are provided, the program is run with default data from {@link
 * MastodonExampleData}.
 *
 * <p>This example shows how to:
 *
 * <ul>
 *   <li>acquire external data,
 *   <li>use in-line defined functions,
 *   <li>handle flattened stream inputs.
 * </ul>
 */
public class MastodonExample {

    // *************************************************************************
    // PROGRAM
    // *************************************************************************

    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);
        System.out.println(
                "Usage: MastodonExample [--output <path>] "
                        + "[--mastodon-source.instanceString <instance> --mastodon-source.accessToken <token>]");

        // set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        env.setParallelism(params.getInt("parallelism", 1));

        // get input data
        DataStream<String> streamSource;
        if (params.has(MastodonSource.INSTANCE_STRING)
                && params.has(MastodonSource.ACCESS_TOKEN)) {
            streamSource = env.addSource(new MastodonSource(params.getProperties()));
        } else {
            System.out.println("Executing MastodonStream example with default props.");
            System.out.println(
                    "Use --mastodon-source.instanceString <instance> --mastodon-source.accessToken <token> to specify the authentication info.");
            // get default test text data
            streamSource = env.fromElements(MastodonExampleData.TEXTS);
        }

        // Print the elements from the source
        //streamSource.print();

        // Get the english toots only
        DataStream<String> englishToots =
                streamSource
                        .filter(new EnglishFilter())
                        .map(new PayloadMap());

        DataStream<Tuple2<String, Integer>> tokenizedToots =
                englishToots
                    .flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (s, out) -> {
                        StringTokenizer tokenizer = new StringTokenizer(s);

                        // split the message
                        while (tokenizer.hasMoreTokens()) {
                            String result = tokenizer.nextToken().toLowerCase();

                            if (!result.isEmpty()) {
                                out.collect(new Tuple2<>(result, 1));
                            }
                        }
                    })
                    .keyBy(value -> value.f0)
                    .sum(1);

        // emit result
        if (params.has("output")) {
            tokenizedToots.sinkTo(
                            FileSink.<Tuple2<String, Integer>>forRowFormat(
                                            new Path(params.get("output")),
                                            new SimpleStringEncoder<>())
                                    .withRollingPolicy(
                                            DefaultRollingPolicy.builder()
                                                    .withMaxPartSize(MemorySize.ofMebiBytes(1))
                                                    .withRolloverInterval(Duration.ofSeconds(10))
                                                    .build())
                                    .build())
                    .name("output");
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            tokenizedToots.print();
        }

        // execute program
        env.execute("Mastodon Streaming Example");
    }

    // *************************************************************************
    // USER FUNCTIONS
    // *************************************************************************

    public static class EnglishFilter implements FilterFunction<String> {
        private static final long serialVersionUID = 1L;

        private transient ObjectMapper jsonParser;

        @Override
        public boolean filter(String s) throws Exception {
            if (jsonParser == null) {
                jsonParser = new ObjectMapper();
            }
            JsonNode jsonNode = jsonParser.readValue(s, JsonNode.class);

            boolean isEnglish =
                    jsonNode.has("language")
                            && jsonNode.get("language").asText().equals("en");
            boolean hasContent = jsonNode.has("content");

            return isEnglish && hasContent;
        }
    }

    public static class PayloadMap implements MapFunction<String, String> {
        private static final long serialVersionUID = 1L;

        private transient ObjectMapper jsonParser;

        @Override
        public String map(String s) throws Exception {
            if (jsonParser == null) {
                jsonParser = new ObjectMapper();
            }
            JsonNode jsonNode = jsonParser.readValue(s, JsonNode.class);

            String content = jsonNode.get("content").asText();

            // remove html tags
            Document document = Jsoup.parse(content);
            return document.body().text();
        }
    }
}
