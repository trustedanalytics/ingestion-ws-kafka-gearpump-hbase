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

import io.gearpump.Message;
import io.gearpump.cluster.UserConfig;
import io.gearpump.streaming.task.StartTime;
import io.gearpump.streaming.task.Task;
import io.gearpump.streaming.task.TaskContext;
import org.slf4j.Logger;

import java.io.UnsupportedEncodingException;

public class String2ByteArrayTask extends Task {

    private TaskContext context;
    private UserConfig userConf;

    private Logger LOG = super.LOG();

    public String2ByteArrayTask(TaskContext taskContext, UserConfig userConf) {
        super(taskContext, userConf);
        this.context = taskContext;
        this.userConf = userConf;
    }

    private Long now() {
        return System.currentTimeMillis();
    }

    @Override public void onStart(StartTime startTime) {
        LOG.info("String2ByteArrayTask.onStart [" + startTime + "]");
    }

    @Override public void onNext(Message message) {
        LOG.info("String2ByteArrayTask.onNext message = [" + message + "]");
        LOG.debug("message.msg class " + message.msg().getClass().getCanonicalName());

        Object msg = message.msg();

        if (msg instanceof String) {
            LOG.info("converting to byte[].");
            try {
                byte[] bytes = ((String) msg).getBytes("UTF-8");
                context.output(new Message(bytes, now()));
            } catch (UnsupportedEncodingException e) {
                LOG.error("error converting String to byte", e);
                context.output(new Message(msg, now()));
            }
        } else {
            LOG.info("sending message as is.");
            context.output(new Message(msg, now()));
        }
    }
}
