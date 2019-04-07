package edu.jhu.bdpuh;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class MovieTableBuilder {
    public static final String ITEM_CF = "item";
    public static final String USER_CF = "user";
    public static final String[] FAMILIES = {ITEM_CF, USER_CF};
    Configuration config = null;

    public static void main(String[] args) throws IOException {
        new MovieTableBuilder(args);
    }

    public MovieTableBuilder(String[] args) throws IOException {
        this.config = HBaseConfiguration.create();

        if(args.length < 2) {
            System.out.printf("%s <input-file> <table-name>\n", this.getClass().getSimpleName());
            System.exit(1);
        }
        File inputFile = new File(args[0]);
        if(!inputFile.exists() || !inputFile.isFile()) {
            System.out.printf("Unable to find input file: %s\n", inputFile.getAbsoluteFile());
        }
        TableName tableName = TableName.valueOf(args[1]);

        // create table if it doesn't exist already
        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {
            System.out.println("Checking to see if table exists.");
            if(!admin.tableExists(tableName)) {
                createTable(admin, tableName);
            } else {
                System.out.printf("Table %s already exists\n", tableName.getNameAsString());
                HTableDescriptor descriptor = admin.getTableDescriptor(tableName);
                HColumnDescriptor[] families = descriptor.getColumnFamilies();
                boolean validSchema = true;
                if (families.length != FAMILIES.length) {
                    validSchema = false;
                } else for (String cf : FAMILIES) {
                    boolean match = false;
                    for(HColumnDescriptor cd : families)
                        if(cd.getNameAsString().equals(cf))
                            match = true;
                    if(!match)
                        validSchema = false;
                }
                if(!validSchema) {
                    System.out.println("Incorrect Schema. Recreating table");
                    admin.disableTable(tableName);
                    admin.deleteTable(tableName);
                    createTable(admin, tableName);
                }
            }
            BufferedMutator mutator = connection.getBufferedMutator(tableName);
            writeData(inputFile, mutator);
            mutator.close();
        }

    }

    static HTableDescriptor createTable(Admin admin, TableName tableName) throws IOException {
        System.out.printf("Creating table %s\n", tableName.getNameAsString());
        HTableDescriptor descriptor = new HTableDescriptor(tableName);
        descriptor.addFamily(new HColumnDescriptor("item"));
        descriptor.addFamily(new HColumnDescriptor("user"));
        // split the table into 5 regions
        byte[][] regions = {"350".getBytes(), "700".getBytes(), "1050".getBytes(), "1400".getBytes()};
        admin.createTable(descriptor, regions);
        return descriptor;
    }

    void addMutations(MovieItem movieItem, BufferedMutator mutator) throws IOException {
        Put put = new Put(movieItem.movieId.getBytes());
        put.addColumn(ITEM_CF.getBytes(), "Title".getBytes(), movieItem.title.getBytes());
        put.addColumn(ITEM_CF.getBytes(), "Released".getBytes(), movieItem.released.getBytes());

        for(String genre : movieItem.genres)
            put.addColumn(ITEM_CF.getBytes(), genre.getBytes(), "".getBytes());

        mutator.mutate(put);

    }
    void addMutations(MovieRating movieRating, BufferedMutator mutator) throws IOException {
        Put put = new Put(movieRating.movieId.getBytes());
        put.addColumn(USER_CF.getBytes(), movieRating.userId.getBytes(), movieRating.timestamp.getEpochSecond()*1000, movieRating.rating.getBytes());
        mutator.mutate(put);
    }
    private void writeData(File inputFile, BufferedMutator mutator) throws IOException {
        // create an instance of each record type
        MovieItem movieItem = new MovieItem();
        MovieRating movieRating = new MovieRating();

        // use a flag to detect type of records in the file
        boolean expectRating = true;

        FileReader fileReader = new FileReader(inputFile);
        BufferedReader bufferedReader = new BufferedReader(fileReader);

        int totalLines = 0;
        String line;
        while((line = bufferedReader.readLine()) != null) {
            totalLines ++;
            if(expectRating) {
                // attempt to load as rating
                if(movieRating.loadData(line))
                    addMutations(movieRating, mutator);
                else
                    expectRating = false;
            }
            if(!expectRating) {
                // already tried rating, attempt to load as item
                if(movieItem.loadData(line))
                    addMutations(movieItem, mutator);
                else {
                    System.out.println("Unexpected data. Giving up");
                    break;
                }
            }
        }
        bufferedReader.close();
        fileReader.close();
        mutator.flush();
        System.out.printf("Processed %d lines\n", totalLines);
    }
}
