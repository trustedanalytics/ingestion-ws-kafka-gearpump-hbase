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

package org.trustedanalytics.ingestion.gearpump.processors;

import io.gearpump.Message;
import io.gearpump.cluster.UserConfig;
import io.gearpump.streaming.task.StartTime;
import io.gearpump.streaming.task.Task;
import io.gearpump.streaming.task.TaskContext;
import org.slf4j.Logger;
import scala.Tuple2;

import java.io.UnsupportedEncodingException;

public class String2Tuple2Task extends Task {

    private TaskContext context;

    private Logger LOGGER = super.LOG();

    public String2Tuple2Task(TaskContext taskContext, UserConfig userConf) {
        super(taskContext, userConf);
        this.context = taskContext;
    }

    private Long now() {
        return System.currentTimeMillis();
    }

    @Override
    public void onStart(StartTime startTime) {
        LOGGER.info("String2Tuple2Task.onStart [" + startTime + "]");
    }

    @Override
    public void onNext(Message message) {
        LOGGER.info("String2Tuple2Task.onNext message = [" + message + "]");

        Object msg = message.msg();
        try {
            LOGGER.debug("converting to Tuple2");
            byte[] key = "message".getBytes("UTF-8");
            byte[] value = ((String) msg).getBytes("UTF-8");
            Tuple2<byte[], byte[]> tuple = new Tuple2<>(key,value);
            context.output(new Message(tuple, now()));
        } catch (UnsupportedEncodingException e) {
            LOGGER.warn("Problem converting message. {}", e);
            LOGGER.debug("sending message as is.");
            context.output(new Message(msg, now()));
        }
    }
}
