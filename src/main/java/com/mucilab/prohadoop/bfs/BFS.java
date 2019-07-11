package com.mucilab.prohadoop.bfs;

/**
 * Created by Raymond on 2017/7/3.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import javax.swing.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;


public class BFS extends Configured implements Tool {

    /**
     * Runs the mapreduce process and takes the return value, which represents
     * how many times mapreduce ran before all nodes were completely prlcessed,
     * and calls the reportDistance method with that count as the argument.
     *
     * @param args The input and output files to be initially used
     */
    public static void main(String[] args) throws Exception {

        int iterations = ToolRunner.run(new Configuration(), new BFS(), args);

        if (args.length != 2) {
            System.err.println("Usage: <in> <output name>");
        }

        reportDistance(iterations);

        System.exit(0);
    }

    /**
     * Reads the output file in the last output folder and based on
     * user input, finds the correct line and parses out the Hops and Parents
     * in order to report how many flights (hops) it takes to get to the destination and what
     * airports (parents) must be passed through to get there.
     *
     * @param iterations The number of mapreduce iterations, which corresponds to the
     *                   folder that should be looked into - such as Output3 for iterations = 3.
     */
    public static void reportDistance(int iterations) throws IOException {

        // The folder name depends on how many iterations were run, but the file is always part-r-00000
        File results = new File("output" + iterations + "/" + "part-r-00000");
        BufferedReader buffReader = new BufferedReader(new FileReader(results));

        String destination = JOptionPane.showInputDialog("Enter 3-letter code for the airport you want to reach");

        Boolean found = false;
        String line;

        while ((line = buffReader.readLine()) != null) {

            String[] sections = line.split("\t");

            // Check if any line begins with the destination airport
            if (sections[0].equalsIgnoreCase(destination)) {

                found = true;

                String[] values = sections[1].split(":");

                // If the distance is still the default, then it means that node was not reachable from the source
                if (Integer.parseInt(values[0]) == 2000000000) {

                    System.out.println("I am sorry, but you cannot get to " + destination + " from here on JetBlue.");
                } else {

                    int flights = Integer.parseInt(values[0]);

                    // If the hops is 0 then source == destination
                    if (flights == 0) {

                        System.out.println("I would advise against flying at all, because your destination is the same as your departure city. Perhaps a taxi would be better.");
                    }

                    // if the hops is 1 then it's just a direct route from the source to the destination
                    else if (flights == 1) {

                        System.out.println("It is a direct flight to " + destination);
                    }

                    // Otherwise retrieve the parents and print it out along with the number of hops
                    else {

                        String[] parents = values[2].split(",");
                        int counter = parents.length;

                        String path = "You'll need to fly from ";

                        for (String parent : parents) {

                            path = path.concat(parent);

                            counter--;

                            if (counter > 0) {

                                path = path.concat(" to ");
                            } else {

                                path = path.concat(" to " + destination + ".");
                            }
                        }

                        System.out.println("The most direct route will take you " + flights + " flights before reaching " + destination);
                        System.out.println(path);
                    }
                }
            }
        }

        // If the provided destination is not in the list at all, then report that
        if (!found) {

            System.out.println("I am sorry, there is no such airport flown to by JetBlue");
        }
    }

    @Override
    public int run(String[] args) throws Exception {

        int iterationCount = 0;
        Job job;
        long terminationValue = 1;

        while (terminationValue > 0) {

            job = new Job(getConf());
            job.setJarByClass(BFS.class);
            job.setMapperClass(BFSMapper.class);
            job.setReducerClass(BFSReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            String input, output;

            // for first iteration, input is the user-specified file
            // for other iterations, input = output of prev iteration
            if (iterationCount == 0) {
                input = args[0];
            } else {
                input = args[1] + iterationCount;
            }

            // set output file
            output = args[1] + (iterationCount + 1);

            // set input file for the job
            FileInputFormat.setInputPaths(job, new Path(input));
            // set output files for the job
            FileOutputFormat.setOutputPath(job, new Path(output));

            // wait for the job to complete
            job.waitForCompletion(true);

            // retrieve the global counter
            Counters jobCounters = job.getCounters();

            // if the counter was increased (eg there are still nodes with a status of in-progress (eg, value 1)
            // then the terminationValue will remain > 0 and the loop will continue
            terminationValue = jobCounters.findCounter(MoreIterations.numberOfIterations).getValue();

            // Increase the iteration count which shows how many times the mapreduce ran before all nodes were at completion status
            iterationCount++;
        }

        return iterationCount;
    }

    // Used for the global counter
    static enum MoreIterations {

        numberOfIterations;
    }

    public static class BFSMapper extends Mapper<LongWritable, Text, Text, Text> {

        /**
         * Each mapper processes the key/value pairs it receives according to this method.
         * In this case for a breadth first search, if this node is to be processed (eg it's at
         * the current level), then this node's neighbors are emitted after their
         * distance/hops has increased by 1 and the parent updated as this incoming node as well
         * as the node itself (after the status is set to completed).
         * If this node is not to be processed then it is just emitted as-is.
         * <p/>
         * The key argument will actually be ignored -
         * Instead the mappers will pull out the proper 'key' themselves from the value.
         * Format for the value field:
         * NodeID<tab>Hops:Neighbors:Parents:StatusFlag
         * Where NodeID becomes the key,
         * Hops is initially 0 for the source and 2000000000 for all other nodes,
         * Neighbors is a comma-separated list of adjacent nodes which can be empty,
         * Parents is a comma-separated list which is initially empty for all nodes and is filled in by the mappers,
         * StatusFlag indicates how the node should be processed by the mappers:
         * StatusFlag 0 means the node has not yet been processed
         * StatusFlag 1 means the node should be processed now by the mappers
         * StatusFlag 3 means the node has been processed completely
         */
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // Split off the key (separated by a tab)
            String[] keyAndValues = value.toString().split("\t");

            // extract the nodeID from the value which will be used as the key
            String newKey = keyAndValues[0];

            // Separate the data columns into an array divided at each ":"
            String[] values = keyAndValues[1].split(":");

            // extract number of hops
            int distance = Integer.parseInt(values[0]);

            // extract status
            int status = Integer.parseInt(values[3]);

            // check if this node should be processed
            if (status == 1) {

                // check if there are any neighbors
                if (values[1].length() > 0) {

                    //extract neighbors
                    String[] neighbors = values[1].split(",");

                    for (String neighbor : neighbors) {

                        // add the parent
                        String parents = values[2].concat(newKey + ",");

                        // increase hops by 1, change status flag to in progress, and add parent destination
                        String newValue = (distance + 1) + "::" + parents + ":" + 1;

                        // emit neighbors for reducers
                        context.write(new Text(neighbor), new Text(newValue));
                    }
                }

                // once any neighbors have been emitted, we can mark this one complete and emit it also
                String newValue = distance + ":" + values[1] + ":" + values[2] + ":" + 3;
                context.write(new Text(newKey), new Text(newValue));
            } else {
                // emit as-is because it is not one that need to be processed
                String newValue = distance + ":" + values[1] + ":" + values[2] + ":" + values[3];
                context.write(new Text(newKey), new Text(newValue));
            }
        }
    }

    public static class BFSReducer extends Reducer<Text, Text, Text, Text> {

        /**
         * Each reducer processes the key/value pairs it receives according to this method.
         * Note that the key here is the NodeID emitted from the reducer and that the value
         * is a list of all of the values that were emitted by mappers along with this particular key.
         * For example if Key1, "dog" was emitted by one mapper and Key1, "cat" was emitted by another,
         * then a reducer would receive: Key1, (dog, cat)
         * <p/>
         * For our breadth first search, the reducers compare the values they receive and emit a value where
         * the smallest Hops value is used, the non-empty neighbors list is used, the parents list that came from
         * the value that had the smallest hops is used, and the more complete status flag is used (it goes 0-->1-->3)
         */
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            int shortestDistance = 2000000000;
            int completionStatus = 0;
            String neighbors = "";
            String parents = "";

            for (Text value : values) {

                String[] items = value.toString().split(":");

                // extract number of hops
                int distance = Integer.parseInt(items[0]);

                // if the distance of this one is less, keep it and the parents
                if (distance < shortestDistance) {

                    shortestDistance = distance;
                    parents = items[2];
                }

                // extract neighbors and if non-empty, assign to the neighbors variable
                if (items[1].length() > 0) {

                    neighbors = items[1];
                }

                // extract status and keep the most complete status (0 --> 1 --> 3)
                int status = Integer.parseInt(items[3]);

                if (status > completionStatus) {

                    completionStatus = status;
                }
            }

            // If the status is 'in progress' (eg, it is value 1)
            // then increment the global counter
            if (completionStatus == 1) {

                context.getCounter(MoreIterations.numberOfIterations).increment(1);
            }

            // now combine to create output and emit
            Text outputValue = new Text(shortestDistance + ":" + neighbors + ":" + parents + ":" + completionStatus);
            context.write(key, outputValue);
        }
    }
}
