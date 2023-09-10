/*******************************************************************************
 *   Copyright 2017 IBM Corp. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *******************************************************************************/
package com.ibm.janusgraph.utils.generator;

import java.io.File;
import java.io.FileWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.javafaker.Faker;
import com.ibm.janusgraph.utils.generator.IdStore.IdBean;
import com.ibm.janusgraph.utils.generator.bean.CSVConfig;
import com.ibm.janusgraph.utils.generator.bean.CSVIdBean;
import com.ibm.janusgraph.utils.generator.bean.ColumnBean;
import com.ibm.janusgraph.utils.generator.bean.EdgeTypeBean;
import com.ibm.janusgraph.utils.generator.bean.RelationBean;
import com.ibm.janusgraph.utils.generator.bean.VertexTypeBean;

public class CSVGenerator {
    private CSVFormat csvFileFormat = CSVFormat.DEFAULT.withRecordSeparator("\n");
    private CSVConfig csvConf = null;
    private CSVIdBean idFactory = null;
    private Calendar cal = Calendar.getInstance();
    private long CURRENT_TIME = cal.getTimeInMillis();
    private int[] RANDOM_INT_RANGE = {100000,99999999};
    private long[] RANDOM_TIME_RANGE = {(long)0, CURRENT_TIME};
    private SimpleDateFormat TIME_FORMAT = new SimpleDateFormat("dd-MMM-yyyy");

    /**
     * Initialize csv generator
     * @param csvConfPath csv config json file
     */
    public CSVGenerator(String csvConfPath){
        this.csvConf = loadConfig(csvConfPath);
        this.idFactory = new CSVIdBean(csvConf.VertexTypes);
    }

    /**
     * Generate a record that includes node_id and property key(s)
     * @param columns a ColumnBean
     * @return an array containing files for a record
     */
    private ArrayList<Object> generateOneRecord(Map<String,ColumnBean> columns){
        ArrayList<Object> rec = new ArrayList<Object>();

        columns.forEach( (name, value) -> {
            if (value.dataType.toLowerCase().equals("integer")
                    || value.dataType.toLowerCase().equals("long")){
                int fromInt, toInt;
                if (value.intRange != null) {
                  fromInt = value.intRange.get("from");
                  toInt = value.intRange.get("to");
                } else {
                  fromInt = this.RANDOM_INT_RANGE[0];
                  toInt = this.RANDOM_INT_RANGE[1];
                }
                rec.add(RandomUtils.nextInt(fromInt, toInt));
            }else if (value.dataType.toLowerCase().equals("date")){
                if (value.dateFormat != null) {
                    this.TIME_FORMAT.applyPattern(value.dateFormat);
                }
                if (value.dateRange != null) {
                    try {
                        this.RANDOM_TIME_RANGE[0] =
                                TIME_FORMAT.parse(value.dateRange.get("from")
                                            ).getTime();
                        this.RANDOM_TIME_RANGE[1] =
                                TIME_FORMAT.parse(value.dateRange.get("to")
                                            ).getTime();
                    } catch (ParseException e) {
                        throw new RuntimeException(e.getMessage() +
                                ". the date cannot be parse using " +
                                TIME_FORMAT.toPattern());
                    }
                }
                cal.setTimeInMillis(
                        RandomUtils.nextLong(this.RANDOM_TIME_RANGE[0],
                                             this.RANDOM_TIME_RANGE[1]));
                    rec.add(TIME_FORMAT.format(cal.getTime()).toString());
            }
            else{
                if ( value.dataSubType != null && value.dataSubType.toLowerCase().equals("name")) {
                    Faker faker = new Faker();
                    rec.add(faker.name().fullName());
                }else if (value.dataSubType != null && value.dataSubType.toLowerCase().equals("shakespeare")) {
                    Faker faker = new Faker();
                    Map<Integer, Runnable> roles = new HashMap<>();
                    // Populate commands map
                    roles.put(1, () -> rec.add(faker.shakespeare().asYouLikeItQuote()));
                    roles.put(2, () -> rec.add(faker.shakespeare().hamletQuote()));
                    roles.put(3, () -> rec.add(faker.shakespeare().kingRichardIIIQuote()));
                    roles.put(4, () -> rec.add(faker.shakespeare().romeoAndJulietQuote()));
                    roles.get(RandomUtils.nextInt(1,5)).run();
                }else if (value.dataSubType != null && value.dataSubType.toLowerCase().equals("custom") && value.options != null) {
                    Faker faker = new Faker();
                    rec.add(faker.options().option(value.options));
                }else if (value.dataSubType != null && value.dataSubType.toLowerCase().equals("company")) {
                    Faker faker = new Faker();
                    rec.add(faker.company().name());
                }else {
                    rec.add(RandomStringUtils.randomAlphabetic(10));
                }
            }
        });
        return rec;
    }
    /**
     * Create csv files for an EdgeType
     * @param type an edge type
     * @param outputDirectory the output folder to write the csv file
     */
    public void writeEdgeCSVs(EdgeTypeBean type, String outputDirectory ) {
        ArrayList<String> header = new ArrayList<String>();
        header.add("Left");
        header.add("Right");
        IdBean ids;
        if (type.columns != null) {
            header.addAll( type.columns.keySet());
        }
        try {
            for (RelationBean relation: type.relations) {
                /*Ex: <left-label>_<edgeType>_<right-label>_edges.csv    */
                String csvFile = outputDirectory + "/" +
                                    String.join("_",
                                                relation.left,
                                                type.name,
                                                relation.right,
                                                "edges.csv");
                CSVPrinter csvFilePrinter = new CSVPrinter(new FileWriter(csvFile), csvFileFormat);
                csvFilePrinter.printRecord(header);
                IdStore idStore = new IdStore(idFactory, relation, type.multiplicity);
                for (int i = 0; i < relation.row; i++) {
                    ArrayList<Object> record = new ArrayList<Object>();
                    ids = idStore.getRandomPairIdForRelation();
                    record.addAll(ids.toArrayList());
                    if (type.columns != null) {
                        record.addAll(generateOneRecord(type.columns));
                    }
                    csvFilePrinter.printRecord(record);
                }
                //add supernodes
                if (relation.supernode != null){
                    int numSuperV = relation.supernode.get("vertices");
                    int numE = relation.supernode.get("edges");
                    int minId = idFactory.getMinId(relation.left);
                    if (  numSuperV > 0 && numE > 0){
                        for ( int v = minId; v < minId + numSuperV; v ++){
                            for (int e = 0; e < numE; e++){
                                ArrayList<Object> record = new ArrayList<Object>();
                                ids = idStore.getRandomIdForRelation(v);
                                record.addAll(ids.toArrayList());
                                if (type.columns != null) {
                                    record.addAll(generateOneRecord(type.columns));
                                }
                                csvFilePrinter.printRecord(record);
                            }
                        }
                    }
                }
                csvFilePrinter.close();
                System.out.println("Generated edge file: "+ csvFile);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    /**
     * Create csv files for a VertexType
     * @param type a vertex type
     * @param outputDirectory the output folder to write the csv file
     */
    void writeVertexCSV(VertexTypeBean type, String outputDirectory ){
        String csvFile = outputDirectory + "/" + type.name + ".csv";
        ArrayList<String> header = new ArrayList<String>();
        header.add("node_id");
        header.addAll(type.columns.keySet());
        int botId = idFactory.getMinId(type.name);
        int topId = idFactory.getMaxId(type.name);
        try {
            CSVPrinter csvFilePrinter = new CSVPrinter(new FileWriter(csvFile), csvFileFormat);
            csvFilePrinter.printRecord(header);
            for (int i = botId; i<=topId; i++){
                ArrayList<Object> record = new ArrayList<Object>();
                record.add(i);
                record.addAll(generateOneRecord(type.columns));
                csvFilePrinter.printRecord(record);
            }
            csvFilePrinter.close();
            System.out.println("Generated vertex file: "+ csvFile);
        } catch (Exception e) {
            throw new RuntimeException(e.toString());
        }
    }
    /**
     * Create all csv files in Parallel
     * @param outputDirectory the output folder to write the csv files
     */
    public void writeAllCSVs(String outputDirectory){
        for (VertexTypeBean vertex : csvConf.VertexTypes){
            Runnable task = () -> { writeVertexCSV(vertex, outputDirectory);};
            new Thread(task).start();
        }
        for (EdgeTypeBean edge: csvConf.EdgeTypes){
            Runnable task = () -> { writeEdgeCSVs(edge, outputDirectory);};
            new Thread(task).start();

        }
    }

    /**
     * Load a csv config json file to a CSVConfig object
     * @param jsonConfFile csv config json file name
     * @return a CSVConfig object
     */
    static CSVConfig loadConfig(String jsonConfFile){
        ObjectMapper confMapper = new ObjectMapper();
        try {
            CSVConfig conf = confMapper.readValue(new File(jsonConfFile), CSVConfig.class);
            isValidConfig(conf);
            return conf;
        } catch (Exception e) {
            throw new RuntimeException("Fail to parse, read, or evaluate the config JSON. " + e.toString());
        }
    }

    /**
     * Validates a csv config file
     * @param config CSVConfig object
     */
    public static void isValidConfig(CSVConfig config){
        //TODO 1. one2one , many2one cannot have supernode
        //     2. selfRef is only for same left and right vertex types
        //     3. one2many , one2many cannot have more edges than the right vertex
        List<String> typeArray = new ArrayList<String>();
        config.VertexTypes.forEach(vertextype -> typeArray.add(vertextype.name));
        for (EdgeTypeBean edgeType: config.EdgeTypes){
            for (RelationBean relation: edgeType.relations) {
            //validate left and right are in the vertex types
                if(!typeArray.contains(relation.left)){
                    throw new RuntimeException("relationships: "
                            + relation.left + " is not of vertex types: " + typeArray.toString());}
                if(!typeArray.contains(relation.right))
                    throw new RuntimeException("relationships: "
                            + relation.right + " is not of vertex types: " + typeArray.toString());
                //validate supernode vertices don't exceed the number of vertices
                Iterator<VertexTypeBean> vTypes = config.VertexTypes.iterator();
                while (vTypes.hasNext()){
                    VertexTypeBean type = vTypes.next();
                    if (relation.left.equals(type.name) &&
                        relation.supernode != null &&
                        relation.supernode.get("vertices") > type.row){
                        ObjectMapper mapper = new ObjectMapper();
                        try {
                            mapper.writeValueAsString(relation);
                            throw new RuntimeException(
                                mapper.writeValueAsString(relation) +
                                "supernode.vertices is greater than " +
                                type.name + "'s row"
                            );
                        } catch (JsonProcessingException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }
    }
}
