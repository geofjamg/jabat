/*
 * Copyright 2012 Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package fr.jamgotchian.jabat.repository;

import fr.jamgotchian.jabat.repository.impl.MetricImpl;
import java.util.Date;
import javax.batch.api.Batchlet;
import javax.batch.runtime.StepExecution;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public interface JabatStepExecution extends StepExecution {

    long getId();

    void setStatus(Status status);

    void setEndTime(Date endTime);

    void setExitStatus(String exitStatus);

    void setUserPersistentData(Object userPersistentData);

    void setMetrics(MetricImpl[] metrics);

    Batchlet getBatchlet();

    void setBatchlet(Batchlet batchlet);

}
