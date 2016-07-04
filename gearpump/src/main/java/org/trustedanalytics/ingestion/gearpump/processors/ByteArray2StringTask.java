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

public class ByteArray2StringTask extends Task {

    private TaskContext context;

    private Logger LOGGER = super.LOG();

    public ByteArray2StringTask(TaskContext taskContext, UserConfig userConf) {
        super(taskContext, userConf);
        this.context = taskContext;
    }

    private Long now() {
        return System.currentTimeMillis();
    }

    @Override 
    public void onStart(StartTime startTime) {
        LOGGER.info("ByteArray2StringTask.onStart [" + startTime + "]");
    }

    @Override 
    public void onNext(Message message) {
        LOGGER.info("ByteArray2StringTask.onNext message = [" + message + "]");
        LOGGER.debug("message.msg class" + message.msg().getClass().getCanonicalName());

        Object msg = message.msg();

        if (msg instanceof byte[]) {
            LOGGER.debug("converting to String.");
            String line = new String((byte[])msg);
            context.output(new Message(line, now()));
        } else {
            LOGGER.debug("sending message as is.");
            context.output(new Message(msg, now()));
        }
    }
}
