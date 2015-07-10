/*
 * Copyright (c) 2015 Intel Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.trustedanalytics.ingestion.gearpump.converters;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import io.gearpump.Message;
import io.gearpump.cluster.UserConfig;
import io.gearpump.streaming.task.StartTime;
import io.gearpump.streaming.task.Task;
import io.gearpump.streaming.task.TaskContext;
import org.slf4j.Logger;

import java.lang.reflect.Type;
import java.util.Map;

public class JsonString2Map extends Task {

    private TaskContext context;
    private UserConfig userConf;

    private Logger LOG = super.LOG();

    public JsonString2Map(TaskContext taskContext, UserConfig userConf) {
        super(taskContext, userConf);
        this.context = taskContext;
        this.userConf = userConf;
    }

    private Long now() {
        return System.currentTimeMillis();
    }

    @Override public void onStart(StartTime startTime) {
        LOG.info("JsonString2Map.onStart [" + startTime + "]");
    }

    @Override public void onNext(Message message) {
        LOG.info("JsonString2Map.onNext message = [" + message + "]");

        Object msg = message.msg();

        if (msg instanceof String) {
            LOG.info("converting json to map.");

            Gson gson = new Gson();
            Type stringObjectMap = new TypeToken<Map<String, Object>>(){}.getType();
            Map<String,Object> map;
            try {
                map = gson.fromJson((String)msg, stringObjectMap);
                LOG.info("resulting map: " + map);
                context.output(new Message(map, now()));
            } catch (JsonSyntaxException e) {
                LOG.error("error converting json string.");
                context.output(new Message(msg, now()));
            } finally {
            }
        } else {
            LOG.info("sending message as is.");
            context.output(new Message(msg, now()));
        }
    }
}
