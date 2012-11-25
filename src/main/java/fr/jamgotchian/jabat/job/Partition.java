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
package fr.jamgotchian.jabat.job;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class Partition {

    private PartitionPlanImpl plan;

    private Artifact mapper;

    private Artifact reducer;

    private Artifact collector;

    private Artifact analyser;

    public Partition() {
    }

    public PartitionPlanImpl getPlan() {
        return plan;
    }

    public void setPlan(PartitionPlanImpl plan) {
        this.plan = plan;
    }

    public Artifact getMapper() {
        return mapper;
    }

    public void setMapper(Artifact mapper) {
        this.mapper = mapper;
    }

    public Artifact getReducer() {
        return reducer;
    }

    public void setReducer(Artifact reducer) {
        this.reducer = reducer;
    }

    public Artifact getCollector() {
        return collector;
    }

    public void setCollector(Artifact collector) {
        this.collector = collector;
    }

    public Artifact getAnalyser() {
        return analyser;
    }

    public void setAnalyser(Artifact analyser) {
        this.analyser = analyser;
    }

}
